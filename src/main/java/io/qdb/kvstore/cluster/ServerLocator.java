package io.qdb.kvstore.cluster;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Responsible for finding out which servers are in our cluster. Posts an event to the shared event bus when the
 * collection of servers in the cluster changes.
 */
public interface ServerLocator extends Closeable {

    /**
     * Start figuring out which servers are in our cluster. Posts a {@link ServersFound} event when done or if the
     * list changes (e.g. new server joins the cluster or a server leaves). This is a NOP if we are already looking
     * for servers. If we already know our servers this will still post an event. So clients wanting the server list
     * should call this method and wait for the event.
     */
    void lookForServers();

    /**
     * This event is posted when the servers in the cluster have been identified.
     */
    public static class ServersFound {

        public final String[] servers;

        public ServersFound(String[] servers) {
            this.servers = servers;
        }

        @Override
        public String toString() {
            return "ServersFound " + Arrays.asList(servers);
        }
    }
}
