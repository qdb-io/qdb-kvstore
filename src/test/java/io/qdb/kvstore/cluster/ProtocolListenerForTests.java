package io.qdb.kvstore.cluster;

public class ProtocolListenerForTests implements Protocol.Listener {

    public String from;
    public PaxosMessage msg;

    @Override
    public void onPaxosMessageReceived(String from, PaxosMessage msg) {
        this.from = from;
        this.msg = msg;
    }
}
