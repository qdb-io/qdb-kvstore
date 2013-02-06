package io.qdb.kvstore.cluster;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Uses the Paxos algorithm to 'catch up' to other servers in the cluster, to order transactions and to receive
 * transactions from other servers.
 */
public class ClusterImpl implements Cluster, Paxos.SequenceNoFactory<SequenceNo>, Paxos.Listener<SequenceNo, StoreTx> {

    private static final Logger log = LoggerFactory.getLogger(ClusterImpl.class);

    private final ExecutorService executorService;
    private final ServerLocator serverLocator;
    private final Transport transport;
    private final int proposalTimeoutMs;
    private final Paxos<SequenceNo, StoreTx> paxos;
    private final Object proposeLock = new Object();

    private ClusteredKeyValueStore store;
    private Status status = Status.DISCONNECTED;

    private Thread proposingThread;
    private Object proposalResult;
    private CountDownLatch proposalLatch;

    public enum Status { DISCONNECTED, JOINING, SYNCING, UP, CLOSED }

    public ClusterImpl(EventBus eventBus, ExecutorService executorService, ServerLocator serverLocator,
                Transport transport, int proposalTimeoutMs) {
        this.executorService = executorService;
        this.serverLocator = serverLocator;
        this.transport = transport;
        this.proposalTimeoutMs = proposalTimeoutMs;

        Paxos.Transport<SequenceNo, StoreTx> pt = new Paxos.Transport<SequenceNo, StoreTx>() {
            public void send(Object to, Paxos.Msg<SequenceNo, StoreTx> msg, Object from) {
                ClusterImpl.this.transport.send((String) to, (Message)msg);
            }
        };

        paxos = new Paxos<SequenceNo, StoreTx>(transport.getSelf(), pt, this, new Message.Factory(), this);

        eventBus.register(this);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {   // better not have any long running stuff in sync blocks
            status = Status.CLOSED;
        }
        serverLocator.close();
        if (proposingThread != null) proposingThread.interrupt();
    }

    @Override
    public synchronized void init(ClusteredKeyValueStore store) {
        if (this.store != null) throw new IllegalStateException("Already have a store: " + this.store);
        this.store = store;
        serverLocator.lookForServers();
        // serverLocator will publish a ServerLocator.ServersFound event when it knows all the servers in our cluster
        // and we start joining the cluster when we get that event
    }

    @Override
    public synchronized KeyValueStore.Status getStoreStatus() {
        switch (status) {
            case UP:        return KeyValueStore.Status.UP;
            case CLOSED:    return KeyValueStore.Status.DOWN;
        }
        return KeyValueStore.Status.READ_ONLY;
    }

    @Subscribe
    public synchronized void onServersFound(ServerLocator.ServersFound ev) {
        switch (status) {
            case CLOSED:
                break;
            case DISCONNECTED:
                joinClusterAsync();
                break;
            default:
                paxos.setNodes(ev.servers);
        }
    }

    private synchronized void joinClusterAsync() {
        status = Status.JOINING;
        executorService.execute(new Runnable() {
            public void run() { joinCluster(); }
        });
    }

    /**
     * Propose a dummy tx to make sure we are up-to-date with the most recent tx id in the cluster. If our tx id is
     * behind the cluster the other nodes will respond with NACKs and we can download transactions or a snapshot
     * from any of them to get in sync.
     */
    @SuppressWarnings("unchecked")
    private void joinCluster() {
        int timeoutMs = proposalTimeoutMs * 5;
        while (getStatus() == Status.JOINING) {
            try {
                log.debug("Proposing NOP tx to join cluster");
                propose(new StoreTx("", StoreTx.Operation.NOP, null), timeoutMs);
                break;
            } catch (ClusterException.Timeout e) {
                log.info(timeoutMs + " ms timeout attempting to join cluster");
            } catch (ClusterException.Interrupted e) {
                if (log.isDebugEnabled()) log.debug("Interrupted while attempting to join cluster: " + e);
                // probably we have been closed in which case out new status will be CLOSED and our loop will exit
                break;
            } catch (final ClusterException.OutOfSync e) {
                // we cannot join until we get in sync
                synchronized (this) {
                    status = Status.SYNCING;
                }
                syncWith(e.from);
            } catch (ClusterException e) {
                log.error("Error attempting to join cluster: " + e, e);
            }
        }
    }

    /**
     * Download transactions or a snapshot from server to get us in sync.
     */
    private void syncWith(String server) {
        try {
            if (store.isEmpty()) {  // load a snapshot
                store.loadSnapshot(transport.getLatestSnapshotFrom(server));
            } else {                // stream transactions
                StoreTxAndId.Iter i = transport.getTransactionsFrom(server, store.getNextTxId());
                try {
                    for (StoreTxAndId tx; (tx = i.next()) != null; ) {
                        // we might accept the next transaction via Paxos before getting it from the stream in which
                        // case it will already have been appended so check for this
                        synchronized (this) {
                            long expectedTxId = store.getNextTxId();
                            if (expectedTxId == tx.txId) {
                                store.appendToTxLogAndApply(tx.storeTx);
                            } else {
                                log.info("Synced tx from " + server + " has txId 0x" + Long.toHexString(tx.txId) + " but we " +
                                        "are expecting 0x" + Long.toHexString(expectedTxId) + ", ending sync");
                            }
                        }
                    }
                } finally {
                    i.close();
                }
            }
        } catch (IOException e) {
            log.error("Error syncing with " + server + ": " + e, e);
            // go back to JOIN mode as we will get a different server to sync if ours current server is down
        }
        synchronized (this) {
            if (status == Status.SYNCING) status = Status.JOINING;
        }
        // drop back into join loop
    }

    @Subscribe
    public synchronized void onMessageReceived(MessageReceived ev) {
        // its not great that we need to know the message types used by Paxos but we have to stop responding as an
        // 'up' node if we are not able to append transactions and I don't want to have to keep a Paxos 'status'
        // in sync with our own status
        switch (ev.message.type) {
            case PREPARE:
                if (status != Status.UP) {
                    if (log.isDebugEnabled()) {
                        log.debug("Discarding " + ev.message + " from " + ev.from + " as we are " + status);
                    }
                    break;
                }
            default:
                paxos.onMessageReceived(ev.from, ev.message);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object propose(StoreTx tx) throws ClusterException {
        return propose(tx, proposalTimeoutMs);
    }

    @SuppressWarnings("unchecked")
    private Object propose(StoreTx tx, int timeoutMs) throws ClusterException {
        synchronized (proposeLock) {    // make sure we only deal with one proposal at a time
            synchronized (this) {
                proposingThread = Thread.currentThread();
                proposalLatch = new CountDownLatch(1);
                paxos.propose(tx);
            }
            try {
                if (!proposalLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new ClusterException.Timeout("Tx not accepted by cluster in " + timeoutMs + " ms");
                }
            } catch (InterruptedException e) {
                throw new ClusterException.Interrupted(e.toString());
            } finally {
                proposingThread = null;
            }
            synchronized (this) {
                Object ans = proposalResult;
                proposalResult = null;
                if (ans instanceof ClusterException) throw (ClusterException)ans;
                else if (ans instanceof Exception) throw new ClusterException(ans.toString(), (Exception)ans);
                return ans;
            }
        }
    }

    @Override
    public synchronized void proposalAccepted(StoreTx proposal, SequenceNo n, Object from) {
        if (proposal.op == StoreTx.Operation.NOP) return;

        long expectedTxId;
        try {
            expectedTxId = store.getNextTxId();
        } catch (IOException e) {
            log.error("Error getting next txId: " + e, e);
            return;
        }
        if (expectedTxId == n.txId) {   // this is the correct transaction so we can append it
            try {
                store.appendToTxLogAndApply(proposal);
            } catch (IOException e) {
                log.error("Error appending tx to log: " + e, e);
            }
        } else if (status == Status.UP) {
            log.info("Tx " + proposal + " from " + from + " has txId 0x" + Long.toHexString(n.txId) + " but we " +
                "are expecting 0x" + Long.toHexString(expectedTxId));
            // we have missed some transactions from the cluster so drop back into join mode to re-sync and recover
            joinClusterAsync();
        }
    }

    @Override
    public synchronized void ourProposalAccepted(StoreTx ourProposal) {
        try {
            proposalResult = ourProposal.op == StoreTx.Operation.NOP ? null : store.appendToTxLogAndApply(ourProposal);
        } catch (Exception e) {
            proposalResult = e;
        }
        proposalLatch.countDown();
    }

    /**
     * This happens when another server has already promised to accept a proposed transaction with a higher
     * sequence number (n). If n has a higher transaction id (txId) then we are behind and must get up to date
     * before any of our transactions will be accepted. Otherwise we can just re-propose our tx with a new
     * sequence number.
     */
    @Override
    public synchronized void ourProposalRejected(StoreTx ourProposal, SequenceNo n, Object from) {
        long ourTxId;
        try {
            ourTxId = store.getNextTxId();
        } catch (IOException e) {
            log.error(e.toString(), e);
            // todo what now? change status to DOWN?
            return;
        }
        if (n.txId > ourTxId) {
            String msg = "Server " + from + " txId 0x" + Long.toHexString(n.txId) + " higher than our txId 0x" +
                    Long.toHexString(ourTxId);
            if (log.isDebugEnabled()) log.debug(msg);
            proposalResult = new ClusterException.OutOfSync(msg, (String)from, n.txId);
            proposalLatch.countDown();
        } else {
            paxos.propose(ourProposal);
        }
    }

    @Override
    public SequenceNo next(SequenceNo n) {
        try {
            return new SequenceNo(store.getNextTxId(), n == null ? 1 : n.seq + 1, transport.getSelf());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private synchronized Status getStatus() {
        return status;
    }

}
