package io.qdb.kvstore.cluster;

import java.io.Serializable;

/**
 * Sequence number for the Paxos algorithm. The most significant portion is the transaction id of the server
 * generating the number to ensure that only servers with the highest txId (i.e. likely the most up-to-date meta
 * data) can win.
 */
public class SequenceNo implements Comparable<SequenceNo>, Serializable {

    public long txId;
    public int seq;
    public String server;

    @SuppressWarnings("UnusedDeclaration")
    public SequenceNo() { }

    public SequenceNo(long txId, int seq, String server) {
        this.txId = txId;
        this.seq = seq;
        this.server = server;
    }

    @Override
    public int compareTo(SequenceNo o) {
        if (txId != o.txId) return txId < o.txId ? -1 : +1;
        if (seq != o.seq) return seq < o.seq ? -1 : +1;
        return server.compareTo(o.server);
    }

    @Override
    public String toString() {
        return Long.toHexString(txId) + "-" + seq + "-" + server;
    }

}
