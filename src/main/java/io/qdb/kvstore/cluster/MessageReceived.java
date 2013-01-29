package io.qdb.kvstore.cluster;

/**
 * Posted on the event bus when a message is received from another server in the cluster.
 */
public class MessageReceived {

    public final String from;
    public final Message message;

    public MessageReceived(String from, Message message) {
        this.from = from;
        this.message = message;
    }

    @Override
    public String toString() {
        return message + " from " + from;
    }
}
