package io.qdb.kvstore;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Clustered in memory key/value store for objects. See README.md for more information. Create instances
 * using {@link KeyValueStoreBuilder}.
 */
public interface KeyValueStore<K, V> extends Closeable {

    enum Status { DOWN, READ_ONLY, UP }

    /**
     * What is the status of this store? Standalone stores are always up.
     */
    Status getStatus();

    /**
     * Get the unique id for this store.
     */
    String getStoreId();

    /**
     * Get a map for storing objects. It is only actually created when the first object is stored.
     * All methods in the map might throw {@link KeyValueStoreException}.
     */
    ConcurrentMap<K, V> getMap(String name);

    /**
     * Get a map for storing objects of a particular type. Note that the type restriction isn't enforced.
     */
    <T extends V> ConcurrentMap<K, T> getMap(String name, Class<T> cls);

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

    /**
     * Receives notification of changes to the store. Extend {@link ListenerAdapter} instead of implementing this
     * interface directly so your code won't break if new methods are addeded.
     */
    interface Listener<K, V> {

        /** An object has been created, updated or deleted. */
        void onObjectEvent(ObjectEvent<K, V> ev);

        /** The status of the store has changed. */
        void onStatusChange(Status status);
    }

    public static class ListenerAdapter<K, V> implements Listener<K, V> {
        public void onObjectEvent(ObjectEvent<K, V> ev) { }
        public void onStatusChange(Status status) { }
    }

    /**
     * A data change to a store.
     */
    public static class ObjectEvent<K, V> {

        public enum Type { CREATED, UPDATED, DELETED }

        public final KeyValueStore<K, V> store;
        public final String map;
        public final Type type;
        public final K key;
        public final V value;

        public ObjectEvent(KeyValueStore<K, V> store, String map, Type type, K key, V value) {
            this.store = store;
            this.type = type;
            this.map = map;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return type + " " + map + "." + key + "=" + value;
        }
    }
}
