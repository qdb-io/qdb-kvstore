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
public class ClusterImpl implements Cluster, Paxos.SequenceNoFactory<SequenceNo>, Paxos.Listener<StoreTx> {

    private static final Logger log = LoggerFactory.getLogger(ClusterImpl.class);

    private final ServerLocator serverLocator;
    private final Transport transport;
    private final int proposalTimeoutMs;
    private final Paxos<SequenceNo, StoreTx> paxos;

    private ClusterMember store;

    private Thread proposingThread;
    private Object proposalResult;
    private CountDownLatch proposalAccepted;

    public ClusterImpl(EventBus eventBus, ServerLocator serverLocator, Transport transport, int proposalTimeoutMs) {
        this.serverLocator = serverLocator;
        this.transport = transport;
        this.proposalTimeoutMs = proposalTimeoutMs;

        paxos = new Paxos<SequenceNo, StoreTx>(transport.getSelf(), transport, this, new Message.Factory(), this);

        eventBus.register(this);
    }

    @Override
    public void close() throws IOException {
        serverLocator.close();
        if (proposingThread != null) proposingThread.interrupt();
    }

    @Override
    public synchronized void rejoin(ClusterMember store) {
        if (this.store != null) throw new IllegalStateException("Already have a store: " + this.store);
        this.store = store;
        serverLocator.lookForServers();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object propose(StoreTx tx) throws ClusterException {
        while (true) {
            synchronized (this) {
                proposingThread = Thread.currentThread();
                proposalAccepted = new CountDownLatch(1);
                paxos.propose(tx);
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

    @Override
    public void proposalAccepted(StoreTx v) {
        store.appendToTxLogAndApply(v);
    }

    @Override
    public void ourProposalAccepted(StoreTx v) {
        try {
            proposalResult = store.appendToTxLogAndApply(v);
        } catch (Exception e) {
            proposalResult = e;
        }
        proposalAccepted.countDown();
    }

    @Override
    public void ourProposalRejected(StoreTx v) {
    }

    @Override
    public SequenceNo next(SequenceNo n) {
        try {
            return new SequenceNo(store.getNextTxId(), n == null ? 1 : n.seq + 1, transport.getSelf());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
