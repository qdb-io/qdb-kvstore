package io.qdb.kvstore;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.qdb.kvstore.cluster.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Helps create a DataStore instance. This makes it possible for the data store to receive all its configuration
 * in the constructor without breaking clients when new parameters are needed.
 */
public class KeyValueStoreBuilder<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreBuilder.class);

    private File dir;
    private KeyValueStore.Serializer serializer;
    private KeyValueStore.VersionProvider<V> versionProvider;
    private KeyValueStore.Listener<K, V> listener;
    private int txLogSizeM = 10;
    private int maxObjectSize = 100000;
    private int snapshotCount = 3;
    private int snapshotIntervalSecs = 60;

    private EventBus eventBus;
    private ExecutorService executorService;
    private ServerLocator serverLocator;
    private Transport transport;
    private int proposalTimeoutMs = 4000;

    public KeyValueStoreBuilder() { }

    public KeyValueStore<K, V> create() throws IOException {
        if (dir == null) throw new IllegalStateException("dir is required");
        if (serializer == null) throw new IllegalStateException("serializer is required");
        if (versionProvider == null) versionProvider = new NopVersionProvider<V>();

        Cluster cluster;
        if (serverLocator != null || transport != null) {
            if (serverLocator == null) throw new IllegalStateException("serverLocator is required");
            if (transport == null) throw new IllegalStateException("transport is required");
            if (eventBus == null) throw new IllegalStateException("eventBus is required");
            if (executorService == null)  {
                executorService = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("qdb-kvstore-%d")
                            .setUncaughtExceptionHandler(new UncaughtHandler()).build());
            }
            cluster = new ClusterImpl(eventBus, executorService, serverLocator, transport, proposalTimeoutMs);
        } else {
            cluster = new StandaloneCluster();
        }

        // todo use executor service instead of timer for KeyValueStoreImpl as well

        return new KeyValueStoreImpl<K, V>(serializer, versionProvider, listener, cluster, dir,
                txLogSizeM, maxObjectSize, snapshotCount, snapshotIntervalSecs);
    }

    /**
     * Directory for snapshots and the transaction log. Created if it does not exist. Must be writable.
     */
    public KeyValueStoreBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    /**
     * Directory for snapshots and the transaction log. Created if it does not exist. Must be writable.
     */
    public KeyValueStoreBuilder dir(String dir) {
        this.dir = new File(dir);
        return this;
    }

    /**
     * The serializer is responsible for converting objects to/from byte streams.
     */
    public KeyValueStoreBuilder serializer(KeyValueStore.Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    /**
     * If you want to use optimistic locking then you must supply a version provider which is responsible for
     * getting and incrementing version numbers.
     */
    public KeyValueStoreBuilder versionProvider(KeyValueStore.VersionProvider<V> versionProvider) {
        this.versionProvider = versionProvider;
        return this;
    }

    /**
     * If you want to be notified when the store is changed then supply a listener.
     */
    public KeyValueStoreBuilder listener(KeyValueStore.Listener<K, V> listener) {
        this.listener = listener;
        return this;
    }

    /**
     * Set the max size in M of the transaction log. Default is 10M.
     */
    public KeyValueStoreBuilder txLogSizeM(int txLogSizeM) {
        this.txLogSizeM = txLogSizeM;
        return this;
    }

    /**
     * Set the maximum size in bytes of stored objects. Default is 100000.
     */
    public KeyValueStoreBuilder maxObjectSize(int maxObjectSize) {
        this.maxObjectSize = maxObjectSize;
        return this;
    }

    /**
     * How many snapshot files should be kept? Default is 3.
     */
    public KeyValueStoreBuilder snapshotCount(int snapshotCount) {
        this.snapshotCount = snapshotCount;
        return this;
    }

    /**
     * How often should automatic snapshots be taken? Default is every 60 seconds if changes have been made to the
     * store. Snapshots are taken as quickly as possible if the transaction log is more than half full.
     */
    public KeyValueStoreBuilder snapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
        return this;
    }

    /**
     * How long should serves in a cluster wait for proposed transactions to be accepted?
     */
    public KeyValueStoreBuilder proposalTimeoutMs(int proposalTimeoutMs) {
        this.proposalTimeoutMs = proposalTimeoutMs;
        return this;
    }

    /**
     * Set event bus use for communication between cluster components. Only required for clustered deployment.
     * The {@link #serverLocator} and {@link Transport} must be hooked up to this bus.
     */
    public KeyValueStoreBuilder eventBus(EventBus eventBus) {
        this.eventBus = eventBus;
        return this;
    }

    /**
     * Set thread pool that used for background tasks. A default pool is created if none is set.
     */
    public KeyValueStoreBuilder executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * The server locator is required for clustering and is responsible for discovering whuch servers are in the
     * cluster.
     */
    public KeyValueStoreBuilder serverLocator(ServerLocator serverLocator) {
        this.serverLocator = serverLocator;
        return this;
    }

    /**
     * The transport is required for clustering and is responsible for sending messages to and receiving messages
     * from other servers in the cluster.
     */
    public KeyValueStoreBuilder transport(Transport transport) {
        this.transport = transport;
        return this;
    }

    private static class NopVersionProvider<V> implements KeyValueStore.VersionProvider<V> {
        public Object getVersion(V value) { return null; }
        public void incVersion(V value) { }
    }

    private static class StandaloneCluster implements Cluster {

        private ClusterMember store;

        public void init(ClusterMember store) {
            this.store = store;
        }

        public KeyValueStore.Status getStoreStatus() {
            return KeyValueStore.Status.UP;
        }

        @SuppressWarnings("unchecked")
        public Object propose(StoreTx tx) throws ClusterException {
            try {
                return store.appendToTxLogAndApply(tx);
            } catch (IOException e) {
                throw new KeyValueStoreException(e.toString(), e);
            }
        }

        public void close() throws IOException { }
    }

    private static class UncaughtHandler implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error(e.toString(), e);
        }
    }
}
