package io.qdb.kvstore.cluster;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks messages sent during tests.
 */
public class TransportForTests implements Transport {

    public final String self;
    public final List<Delivery> sent = new ArrayList<Delivery>();

    public TransportForTests(String self) {
        this.self = self;
    }

    @Override
    public String getSelf() {
        return self;
    }

    @Override
    public void send(Message msg, String to) {
        sent.add(new Delivery(msg, to));
    }

    @Override
    public String toString() {
        String s = sent.toString();
        return s.substring(1, s.length() - 1);
    }
}
