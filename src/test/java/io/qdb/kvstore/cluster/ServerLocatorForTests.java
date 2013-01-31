package io.qdb.kvstore.cluster;

import com.google.common.eventbus.EventBus;

import java.io.IOException;

/**
 * Provides a fixed set of servers.
 */
public class ServerLocatorForTests implements ServerLocator {

    private final EventBus eventBus;
    private final String[] servers;

    public ServerLocatorForTests(EventBus eventBus, String[] servers) {
        this.eventBus = eventBus;
        this.servers = servers;
    }

    @Override
    public void lookForServers() {
        eventBus.post(new ServersFound(servers));
    }

    @Override
    public void close() throws IOException {
    }
}
