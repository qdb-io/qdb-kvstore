package io.qdb.kvstore;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * Clustered in memory key/value store for objects. See README.md for more information. Create instances
 * using {@link KeyValueStoreBuilder}.
 */
public interface KeyValueStore<K, V> extends Closeable {

    /**
     * Get a map for storing objects. It is only actually created when the first object is stored.
     * All methods in the map might throw {@link KeyValueStoreException}.
     */
    ConcurrentMap<K, V> getMap(String name);

    /**
     * Create a snapshot of our data. This is useful for transferring our state to another KeyValueStore (maybe
     * on a different machine).
     */
    void createSnapshot(OutputStream out) throws IOException;

    /**
     * Populate this store with data from the snapshot. Note that this is only allowed if the store is
     * {@link #isEmpty()}.
     */
    void loadSnapshot(InputStream in) throws IOException;

    /**
     * Save a snapshot. This is a NOP if we are already busy saving a snapshot or if no new transactions have been
     * applied since the most recent snapshot was saved.
     */
    void saveSnapshot() throws IOException;

    /**
     * Does this store contain no objects?
     */
    boolean isEmpty();

    /**
     * Get the names of all of the maps in this store.
     */
    List<String> getMapNames();

    /**
     * A copy of the data in a data store.
     */
    public static class Snapshot<K, V> implements Serializable {
        public Long txId;
        public String storeId;
        public Map<String, Map<K, V>> maps;
    }

    /**
     * Responsible for converting objects to/from streams. Note that this must be able to serialize
     * {@link Snapshot} and {@link StoreTx} instances which will reference K and V instances.
     */
    interface Serializer {
        public void serialize(Object value, OutputStream out) throws IOException;
        public <T> T deserialize(InputStream in, Class<T> cls) throws IOException;
    }

    /** Extracts version numbers from objects for optimistic locking. */
    interface VersionProvider<V> {
        /** Get the version of value or null if it does not have a version. */
        public Object getVersion(V value);
        /** Bump up the version number of value. NOP if not using versioning. */
        public void incVersion(V value);
    }
}
