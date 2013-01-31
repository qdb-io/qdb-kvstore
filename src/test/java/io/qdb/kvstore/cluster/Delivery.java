package io.qdb.kvstore.cluster;

/**
 * A message and its destination.
 */
public class Delivery {

    public final Message msg;
    public final String to;

    public Delivery(Message msg, String to) {
        this.msg = msg;
        this.to = to;
    }

    @Override
    public String toString() {
        return msg.toString() + " to " + to;
    }
}
