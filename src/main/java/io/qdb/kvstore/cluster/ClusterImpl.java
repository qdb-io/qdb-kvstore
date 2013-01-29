package io.qdb.kvstore.cluster;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import io.qdb.kvstore.StoreTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Uses the Paxos algorithm to 'catch up' to other servers in the cluster, to order transactions and to receive
 * transactions from other servers.
 */
public class ClusterImpl implements Cluster {

    private static final Logger log = LoggerFactory.getLogger(ClusterImpl.class);

    private final ServerLocator serverRegistry;
    private final Transport transport;
    private final int proposalTimeoutMs;

    private ClusterMember store;

    private String[] servers;
    private Message[] promisesReceived;         // highest numbered PROMISE received from each node
    private SequenceNo highestSeqNoSeen;
    private boolean[] accepted;                 // accepted messages received from each node
    private StoreTx v;

    private Thread proposingThread;
    private StoreTx proposal;
    private Object proposalResult;
    private CountDownLatch proposalAccepted;

    public ClusterImpl(EventBus eventBus, ServerLocator serverRegistry, Transport transport, int proposalTimeoutMs) {
        this.serverRegistry = serverRegistry;
        this.transport = transport;
        this.proposalTimeoutMs = proposalTimeoutMs;
        eventBus.register(this);
    }

    @Override
    public void close() throws IOException {
        serverRegistry.close();
        if (proposingThread != null) proposingThread.interrupt();
    }

    @Override
    public synchronized void rejoin(ClusterMember store) {
        if (this.store != null) throw new IllegalStateException("Already have a store: " + this.store);
        this.store = store;
        serverRegistry.lookForServers();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object propose(StoreTx tx) throws ClusterException {
        while (true) {
            synchronized (this) {
                proposingThread = Thread.currentThread();
                proposal = tx;
                promisesReceived = new Message[servers.length];
                Message prepare = new Message(Message.Type.PREPARE, next(highestSeqNoSeen), tx, null);
                for (String server : servers) transport.send(prepare, server);
                proposalAccepted = new CountDownLatch(1);
            }
            try {
                if (proposalAccepted.await(proposalTimeoutMs, TimeUnit.MILLISECONDS)) break;
                // todo maybe we should delay for a little before re-proposing?
            } catch (InterruptedException e) {
                throw new ClusterException(e.toString());
            } finally {
                proposingThread = null;
            }
        }
        synchronized (this) {
            Object ans = proposalResult;
            proposalResult = null;
            if (ans instanceof Exception) {
                Exception e = (Exception) ans;
                throw new ClusterException(e.toString(), e);
            }
            return ans;
        }
    }

    @Subscribe
    public synchronized void handleServersFound(ServerLocator.ServersFound ev) {
        if (this.servers == null || !Arrays.equals(this.servers, ev.servers)) {
            this.servers = ev.servers;
            promisesReceived = null;
        }
        // todo restart current proposal if any?
    }

    @Subscribe
    public synchronized void handleMessageReceived(MessageReceived ev) {
        if (log.isDebugEnabled()) log.debug("Received " + ev);
        Message msg = ev.message;
        String from = ev.from;
        switch (msg.type) {
            case PREPARE:   onPrepareReceived(from, msg);   break;
            case PROMISE:   onPromiseReceived(from, msg);   break;
            case NACK:      onNackReceived();               break;
            case ACCEPT:    onAcceptReceived(from, msg);    break;
            case ACCEPTED:  onAcceptedReceived(from, msg);  break;
            default:
                throw new IllegalArgumentException("Unknown msg type: " + msg);
        }
    }

    private int indexOfNode(Object node) {
        if (servers != null) {
            for (int i = servers.length - 1; i >= 0; i--) if (node.equals(servers[i])) return i;
        }
        return -1;
    }

    private void onPrepareReceived(String from, Message msg) {
        SequenceNo n = msg.n;
        if (highestSeqNoSeen == null) {
            // haven't seen any proposals so accept this one
            highestSeqNoSeen = n;
            v = msg.v;
            transport.send(new Message(Message.Type.PROMISE, n, v, highestSeqNoSeen), from);

        } else if (n.compareTo(highestSeqNoSeen) < 0) {
            // proposal has lower sequence no so NACK it and include our highest seq no
            transport.send(new Message(Message.Type.NACK, highestSeqNoSeen, null, null), from);

        } else {
            // proposal has higher sequence so transport.send back previous highest sequence and it's proposal
            Message ack = new Message(Message.Type.PROMISE, n, v, highestSeqNoSeen);
            highestSeqNoSeen = n;
            transport.send(ack, from);
        }
    }

    private void onPromiseReceived(String from, Message msg) {
        if (promisesReceived == null) return;  // ACCEPT already sent or proposal abandoned

        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("PROMISE received from " + from + " not known to us, ignoring: " + msg);
            return;
        }

        Message prev = promisesReceived[i];
        if (prev == null || prev.n.compareTo(msg.n) < 0) {
            promisesReceived[i] = msg;
            // see if we have a majority of PROMISEs + find the most recent (in proposal number ordering) value
            SequenceNo highest = null;
            StoreTx value = null;
            int count = 0;
            for (Message m : promisesReceived) {
                if (m == null || m.v == null) continue;
                ++count;
                if (highest == null || highest.compareTo(m.nv) < 0) {
                    highest = m.nv;
                    value = m.v;
                }
            }
            if (count > servers.length / 2) {
                // we have received promises from a majority of servers so send everyone except ourselves accept messages
                // and wait
                promisesReceived = null;
                accepted = new boolean[servers.length];
                Message accept = new Message(Message.Type.ACCEPT, highest, value, null);
                String self = transport.getSelf();
                for (int j = 0; j < servers.length; j++) {
                    String server = servers[j];
                    if (self.equals(server)) accepted[j] = true;
                    else transport.send(accept, server);
                }
            }
        }
    }

    private void onNackReceived() {
        // abandon our proposal
        promisesReceived = null;
        // todo re-propose it
    }

    @SuppressWarnings("unchecked")
    private void onAcceptReceived(String from, Message msg) {
        // ignore if we have already PROMISEd for a higher sequence no
        if (highestSeqNoSeen != null && highestSeqNoSeen.compareTo(msg.n) > 0) return;

        highestSeqNoSeen = msg.n;

        store.appendToTxLogAndApply(msg.v);

        // notify the proposer that we have accepted the tx - if it never receives a majority of ACCEPTED
        // messages it will get behind and will have to catch up
        transport.send(new Message(Message.Type.ACCEPTED, highestSeqNoSeen, msg.v, null), from);
    }

    @SuppressWarnings("unchecked")
    private void onAcceptedReceived(String from, Message msg) {
        if (accepted == null || msg.n.compareTo(highestSeqNoSeen) != 0) return;  // old or dup ACCEPTED message

        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("ACCEPTED received from " + from + " not known to us, ignoring: " + msg);
            return;
        }

        accepted[i] = true;

        // count accepted messages and let the proposing thread continue if we have a majority
        int c = 0;
        int n = accepted.length / 2;
        for (i = 0; i < accepted.length; i++) {
            if (accepted[i] && (++c > n)) {
                accepted = null;
                try {
                    proposalResult = store.appendToTxLogAndApply(proposal);
                } catch (Exception e) {
                    proposalResult = e;
                }
                proposal = null;
                proposalAccepted.countDown();
                break;
            }
        }
    }

    private SequenceNo next(SequenceNo n) {
        try {
            return new SequenceNo(store.getNextTxId(), n == null ? 1 : n.seq + 1, transport.getSelf());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
