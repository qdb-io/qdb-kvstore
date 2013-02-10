package io.qdb.kvstore.cluster;

public class ProtocolListenerForTests implements Protocol.Listener {

    public String from;
    public PaxosMessage msg;
    public String[] servers;

    @Override
    public void onPaxosMessageReceived(String from, PaxosMessage msg) {
        this.from = from;
        this.msg = msg;
    }

    @Override
    public void onServersFound(String[] servers) {
        this.servers = servers;
    }
}
