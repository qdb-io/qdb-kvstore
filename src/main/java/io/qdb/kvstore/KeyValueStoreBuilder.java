package io.qdb.kvstore;

import java.io.File;
import java.io.IOException;

/**
 * Helps create a DataStore instance. This makes it possible for the data store to receive all its configuration
 * in the constructor without breaking clients when new parameters are needed.
 */
public class KeyValueStoreBuilder<K, V> {

    private File dir;
    private KeyValueStore.Serializer serializer;
    private KeyValueStore.VersionProvider<V> versionProvider;
    private int txLogSizeM = 10;
    private int maxObjectSize = 100000;
    private int snapshotCount = 3;
    private int snapshotIntervalSecs = 60;

    public KeyValueStoreBuilder() { }

    public KeyValueStore<K, V> create() throws IOException {
        if (dir == null) throw new IllegalStateException("dir is required");
        if (serializer == null) throw new IllegalStateException("serializer is required");
        if (versionProvider == null) versionProvider = new NullVersionProvider<V>();
        return new KeyValueStoreImpl<K, V>(serializer, versionProvider, dir, txLogSizeM, maxObjectSize, snapshotCount,
                snapshotIntervalSecs);
    }

    public KeyValueStoreBuilder dir(File dir) {
        this.dir = dir;
        return this;
    }

    public KeyValueStoreBuilder dir(String dir) {
        this.dir = new File(dir);
        return this;
    }

    public KeyValueStoreBuilder serializer(KeyValueStore.Serializer serializer) {
        this.serializer = serializer;
        return this;
    }

    public KeyValueStoreBuilder versionProvider(KeyValueStore.VersionProvider<V> versionProvider) {
        this.versionProvider = versionProvider;
        return this;
    }

    public KeyValueStoreBuilder txLogSizeM(int txLogSizeM) {
        this.txLogSizeM = txLogSizeM;
        return this;
    }

    public KeyValueStoreBuilder maxObjectSize(int maxObjectSize) {
        this.maxObjectSize = maxObjectSize;
        return this;
    }

    public KeyValueStoreBuilder snapshotCount(int snapshotCount) {
        this.snapshotCount = snapshotCount;
        return this;
    }

    public KeyValueStoreBuilder snapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
        return this;
    }

    public static class NullVersionProvider<V> implements KeyValueStore.VersionProvider<V> {
        public int getVersion(V value) { return 0; }
        public void incVersion(V value) { }
    }
}
