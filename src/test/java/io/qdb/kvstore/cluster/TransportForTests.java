package io.qdb.kvstore.cluster;

import java.io.IOException;

public class TransportForTests implements Transport {

    public String self;
    public MessageListener listener;

    public String to;
    public byte[] msg;
    public String from;
    public byte[] response;

    public TransportForTests(String self) {
        this.self = self;
    }

    @Override
    public void init(MessageListener listener) {
        this.listener = listener;
    }

    @Override
    public String getSelf() {
        return self;
    }

    @Override
    public void send(String to, byte[] msg) {
        this.to = to;
        this.msg = msg;
    }

    @Override
    public byte[] get(String from, byte[] msg) throws IOException {
        this.from = from;
        this.msg = msg;
        return response;
    }
}
