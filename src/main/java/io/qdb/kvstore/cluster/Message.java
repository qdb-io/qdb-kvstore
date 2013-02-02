package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

import java.io.Serializable;

/**
 * A message between nodes in our cluster.
 */
public class Message implements Serializable, Paxos.Msg<SequenceNo, StoreTx> {

    public Paxos.Msg.Type type;
    public SequenceNo n;
    public StoreTx v;
    public SequenceNo nv;

    public static class Factory implements Paxos.MsgFactory<SequenceNo, StoreTx> {
        @Override
        public Paxos.Msg<SequenceNo, StoreTx> create(Type type, SequenceNo sequenceNo, StoreTx v, SequenceNo nv) {
            return new Message(type, sequenceNo, v, nv);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public Message() { }

    public Message(Paxos.Msg.Type type, SequenceNo n, StoreTx v) {
        this.type = type;
        this.n = n;
        this.v = v;
    }

    public Message(Paxos.Msg.Type type, SequenceNo n, StoreTx v, SequenceNo nv) {
        this(type, n, v);
        this.nv = nv;
    }

    @Override
    public Paxos.Msg.Type getType() { return type; }

    @Override
    public SequenceNo getN() { return n; }

    @Override
    public StoreTx getV() { return v; }

    @Override
    public SequenceNo getNv() { return nv; }

    @Override
    public String toString() {
        return type + (n != null ? " n=" + n : "" ) + (v != null ? " v=" + v : "") + (nv != null ? " nv=" + nv : "");
    }
}
