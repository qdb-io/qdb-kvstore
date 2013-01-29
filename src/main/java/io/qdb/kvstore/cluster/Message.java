package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

import java.io.Serializable;

/**
 * A message between nodes in our cluster.
 */
public class Message implements Serializable {

    enum Type { PREPARE, PROMISE, NACK, ACCEPT, ACCEPTED }

    public Type type;
    public SequenceNo n;
    public StoreTx v;
    public SequenceNo nv;

    public Message(Type type, SequenceNo n, StoreTx v) {
        this.type = type;
        this.n = n;
        this.v = v;
    }

    public Message(Type type, SequenceNo n, StoreTx v, SequenceNo nv) {
        this(type, n, v);
        this.nv = nv;
    }

    @Override
    public String toString() {
        return type + (n != null ? " n=" + n : "" ) + (v != null ? " v=" + v : "") + (nv != null ? " nv=" + nv : "");
    }
}
