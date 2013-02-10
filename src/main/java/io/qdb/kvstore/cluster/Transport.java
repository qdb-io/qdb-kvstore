package io.qdb.kvstore.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Responsible for finding out which servers are in our cluster for for sending/receiving messages to/from them.
 */
public interface Transport extends Closeable {

    public interface Listener {
        void onMessageReceived(String from, InputStream ins, OutputStream out) throws IOException;
        void onServersFound(String[] servers);
    }

    /**
     * No calls will be made to the transport until init has been called. Messages received from other servers
     * in the cluster should be delivered to the listener's onMessageReceived method.
     */
    void init(Listener listener);

    /**
     * Get our server.
     */
    String getSelf();

    /**
     * Start figuring out which servers are in our cluster. Call the listener when this is done or when the
     * list changes (e.g. new server joins the cluster or a server leaves). This is a NOP if we are already looking
     * for servers. If we already know our servers this will still post an event. So clients wanting the server list
     * should call this method and wait for the event.
     */
    void lookForServers();

    /**
     * Send a message to the server asynchronously.
     */
    void send(String to, byte[] msg);

    /**
     * Send a message to the server synchronously and return the response.
     */
    byte[] get(String from, byte[] msg) throws IOException;

}
