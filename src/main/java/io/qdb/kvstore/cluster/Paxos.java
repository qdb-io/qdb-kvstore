package io.qdb.kvstore.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Paxos implementation. This is independent of the msg transport and the sequence number implementation.
 */
public class Paxos<N extends Comparable<N>, V> {

    private static final Logger log = LoggerFactory.getLogger(Paxos.class);

    private final Object self;
    private final Transport<N, V> transport;
    private final SequenceNoFactory<N> sequenceNoFactory;
    private final MsgFactory<N, V> msgFactory;
    private final Listener<V> listener;

    private Object[] nodes;

    private V ourProposal;
    private Msg<N, V>[] promised;          // highest numbered PROMISE received from each node
    private N highestSeqNoSeen;
    private V v;
    private boolean[] accepted;         // node that have sent is accepted messages

    public Paxos(Object self, Transport<N, V> transport, SequenceNoFactory<N> sequenceNoFactory,
                 MsgFactory<N, V> msgFactory, Listener<V> listener) {
        this.self = self;
        this.transport = transport;
        this.sequenceNoFactory = sequenceNoFactory;
        this.msgFactory = msgFactory;
        this.listener = listener;
    }

    /**
     * Set the list of nodes we know about. Any election already in progress is cancelled if the list of nodes has
     * actually changed.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public synchronized void setNodes(Object[] nodes) {
        if (this.nodes == null || !Arrays.equals(this.nodes, nodes)) {
            int i;
            for (i = nodes.length - 1; i >= 0 && !self.equals(nodes[i]); i--);
            if (i < 0) throw new IllegalArgumentException(self + " not in nodes " + Arrays.asList(nodes));
            this.nodes = nodes;
            // todo cancel any election already in progress
            promised = null;
            accepted = null;
        }
    }

    /**
     * Start the Paxos algorithm. Any election already in progress is restarted.
     */
    @SuppressWarnings("unchecked")
    public synchronized void propose(V proposal) {
        this.ourProposal = proposal;
        this.promised = new Msg[nodes.length];
        highestSeqNoSeen = sequenceNoFactory.next(highestSeqNoSeen);
        Msg<N, V> prepare = msgFactory.create(Msg.Type.PREPARE, highestSeqNoSeen, proposal, null);
        for (int i = 0; i < nodes.length; i++) {
            Object node = nodes[i];
            if (node.equals(self)) {
                promised[i] = msgFactory.create(Msg.Type.PROMISE, highestSeqNoSeen, proposal, highestSeqNoSeen);
            } else {
                send(prepare, node);
            }
        }
    }

    private void send(Msg<N, V> msg, Object node) {
        transport.send(node, msg, self);
    }

    /**
     * A message has been received from another Node.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    public synchronized void onMessageReceived(Object from, Msg<N, V> msg) {
        if (log.isDebugEnabled()) log.debug("Received " + msg + "  from " + from);
        switch (msg.getType()) {
            case PREPARE:   onPrepareReceived(from, msg);     break;
            case PROMISE:   onPromiseReceived(from, msg);     break;
            case NACK:      onNackReceived();                 break;
            case ACCEPT:    onAcceptReceived(from, msg);      break;
            case ACCEPTED:  onAcceptedReceived(from, msg);    break;
            default:
                throw new IllegalArgumentException("Unknown msg type: " + msg);
        }
    }

    private int indexOfNode(Object node) {
        if (nodes != null) {
            for (int i = nodes.length - 1; i >= 0; i--) if (node.equals(nodes[i])) return i;
        }
        return -1;
    }

    private void onPrepareReceived(Object from, Msg<N, V> msg) {
        N n = msg.getN();
        if (highestSeqNoSeen == null) {
            // haven't seen any proposals so accept this one
            highestSeqNoSeen = n;
            v = msg.getV();
            send(msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen), from);

        } else if (n.compareTo(highestSeqNoSeen) < 0) {
            // proposal has lower sequence no so NACK it and include our highest seq no
            send(msgFactory.create(Msg.Type.NACK, highestSeqNoSeen, null, null), from);

        } else {
            // proposal has higher sequence so send back previous highest sequence and it's proposal
            Msg<N, V> ack = msgFactory.create(Msg.Type.PROMISE, n, v, highestSeqNoSeen);
            highestSeqNoSeen = n;
            send(ack, from);
        }
    }

    private void onPromiseReceived(Object from, Msg<N, V> msg) {
        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("PROMISE received from node " + from + " not known to us, ignoring: " + msg);
            return;
        }
        if (promised == null) return;  // ACCEPT already sent or proposal abandoned

        Msg<N, V> prev = promised[i];
        if (prev == null || prev.getN().compareTo(msg.getN()) < 0) {
            promised[i] = msg;
            // see if we have a majority of PROMISEs + find the most recent (in proposal number ordering) value
            N highest = null;
            V value = null;
            int count = 0;
            for (Msg<N, V> m : promised) {
                if (m == null || m.getV() == null) continue;
                ++count;
                if (highest == null || highest.compareTo(m.getNv()) < 0) {
                    highest = m.getNv();
                    value = m.getV();
                }
            }
            if (count > nodes.length / 2) {
                // we have received promises from a majority of servers so send everyone except ourselves accept
                // messages and wait
                promised = null;
                accepted = new boolean[nodes.length];
                Msg<N, V> accept = msgFactory.create(Msg.Type.ACCEPT, highest, value, null);
                for (int j = 0; j < nodes.length; j++) {
                    Object node = nodes[j];
                    if (node.equals(self)) accepted[j] = true;
                    else send(accept, node);
                }
            }
        }
    }

    private void onNackReceived() {
        if (promised != null) {     // abandon our proposal
            promised = null;
            V copy = ourProposal;
            ourProposal = null;
            listener.ourProposalRejected(copy);
        }
    }

    private void onAcceptReceived(Object from, Msg<N, V> msg) {
        // ignore if we have already PROMISEd for a higher sequence no
        if (highestSeqNoSeen != null && highestSeqNoSeen.compareTo(msg.getN()) > 0) return;

        highestSeqNoSeen = msg.getN();
        v = msg.getV();

        listener.proposalAccepted(v);

        // let the node we received the message from know that we have accepted it
        send(msgFactory.create(Msg.Type.ACCEPTED, highestSeqNoSeen, v, null), from);
    }

    private void onAcceptedReceived(Object from, Msg<N, V> msg) {
        highestSeqNoSeen = msg.getN();
        v = msg.getV();
        listener.ourProposalAccepted(v);

        if (accepted == null || msg.getN().compareTo(highestSeqNoSeen) != 0) return;  // old or dup ACCEPTED message

        int i = indexOfNode(from);
        if (i < 0) {
            log.warn("ACCEPTED received from " + from + " not known to us, ignoring: " + msg);
            return;
        }

        accepted[i] = true;

        // count accepted messages and let our listener know if we have a majority
        int c = 0;
        int n = accepted.length / 2;
        for (i = 0; i < accepted.length; i++) {
            if (accepted[i] && (++c > n)) {
                accepted = null;
                listener.ourProposalAccepted(msg.getV());
                break;
            }
        }
    }

    /** Sends messages to nodes asynchronously. */
    public interface Transport<N extends Comparable<N>, V> {
        void send(Object to, Msg<N, V> msg, Object from);
    }

    public interface SequenceNoFactory<N extends Comparable<N>> {
        /** Generate a unique sequence number higher than n (which may be null). */
        public N next(N n);
    }

    /** Notified of the progress of the algorithm and of accepted values. */
    public interface Listener<V> {

        /** We have accepted a proposal from another node. */
        void proposalAccepted(V v);

        /** Our proposal has been accepted by a majority of nodes. */
        void ourProposalAccepted(V v);

        /** Our proposal has been rejected. */
        void ourProposalRejected(V v);
    }

    /** Creates messages. */
    public interface MsgFactory<N extends Comparable<N>, V> {
        Msg<N, V> create(Msg.Type type, N n, V v, N nv);
    }

    public interface Msg<N extends Comparable, V> {
        enum Type { PREPARE, PROMISE, NACK, ACCEPT, ACCEPTED }
        public Type getType();
        public N getN();
        public V getV();
        public N getNv();
    }
}
