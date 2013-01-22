package io.qdb.store;

import com.google.common.io.PatternFilenameFilter;
import com.sun.tools.internal.ws.processor.model.ModelException;
import io.qdb.buffer.MessageBuffer;
import io.qdb.buffer.MessageCursor;
import io.qdb.buffer.PersistentMessageBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * KV store implementation. Create these using {@link KeyValueStoreBuilder}.
 */
public class KeyValueStoreImpl<K, V> implements KeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreImpl.class);

    private final Serializer serializer;
    private final VersionProvider<V> versionProvider;
    private final File dir;
    private final int txLogSizeM;
    private final int snapshotCount;
    private final int snapshotIntervalSecs;
    private final Timer snapshotTimer;

    private String storeId;
    private MessageBuffer txLog;
    private long mostRecentSnapshotId;
    private boolean busySavingSnapshot;
    private boolean snapshotScheduled;
    
    private final ConcurrentMap<String, ConcurrentMap<K, V>> maps = new ConcurrentHashMap<String, ConcurrentMap<K, V>>();

    @SuppressWarnings("unchecked")
    KeyValueStoreImpl(Serializer serializer, VersionProvider<V> versionProvider, File dir, int txLogSizeM,
                      int maxObjectSize, int snapshotCount, int snapshotIntervalSecs) throws IOException {
        this.serializer = serializer;
        this.versionProvider = versionProvider;
        this.dir = dir;
        this.txLogSizeM = txLogSizeM;
        this.snapshotCount = snapshotCount;
        this.snapshotIntervalSecs = snapshotIntervalSecs;

        dir = DirUtil.ensureDirectory(dir);

        txLog = new PersistentMessageBuffer(DirUtil.ensureDirectory(new File(dir, "txlog")));
        txLog.setMaxSize(txLogSizeM * 1000000);
        txLog.setMaxPayloadSize(maxObjectSize + 100);

        File[] files = getSnapshotFiles();
        Snapshot<K, V> snapshot = null;
        for (int i = files.length - 1; i >= 0; i--) {
            File f = files[i];
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
            try {
                snapshot = (Snapshot<K, V>)this.serializer.deserialize(in, Snapshot.class);
            } catch (Exception e) {
                log.error("Error loading " + f + ", ignoring: " + e);
                continue;
            } finally {
                try {
                    in.close();
                } catch (IOException ignore) {
                }
            }

            String name = f.getName();
            int j = name.indexOf('-');
            int k = name.lastIndexOf('.');
            mostRecentSnapshotId = Long.parseLong(name.substring(j + 1, k), 16);
            if (log.isDebugEnabled()) log.debug("Loaded " + f);
            break;
        }

        if (mostRecentSnapshotId < txLog.getOldestMessageId()) {
            throw new IOException("Most recent snapshot " + Long.toHexString(mostRecentSnapshotId) +
                    " is older than oldest record in txlog " + Long.toHexString(txLog.getOldestMessageId()));
        }

        if (txLog.getNextMessageId() == 0 && mostRecentSnapshotId > 0) {
            // probably this a recovery after a cluster failure by copying snapshot files around and nuking tx logs
            // to get everyone in sync
            log.info("The txlog is empty but we have snapshot " + Long.toHexString(mostRecentSnapshotId) +
                    " so using that as next id");
            txLog.setFirstMessageId(mostRecentSnapshotId);
        }

        storeId = snapshot == null ? generateRepositoryId() : snapshot.storeId;
        if (snapshot != null) {
            for (Map.Entry<String, Map<K, V>> e : snapshot.maps.entrySet()) {
                maps.put(e.getKey(), new ConcurrentHashMap<K, V>(e.getValue()));
            }
        }

        int count = 0;
        for (MessageCursor c = txLog.cursor(mostRecentSnapshotId); c.next(); count++) {
            StoreTx tx = this.serializer.deserialize(new ByteArrayInputStream(c.getPayload()), StoreTx.class);
            try {
                apply(tx);
            } catch (ModelException e) {
                if (log.isDebugEnabled()) log.debug("Got " + e + " replaying " + tx);
            }
        }
        if (log.isDebugEnabled()) log.debug("Replayed " + count + " transaction(s)");

        snapshotTimer = new Timer("datastore-snapshot-" + dir.getName(), true);
    }

    private File[] getSnapshotFiles() {
        File[] files = dir.listFiles(new PatternFilenameFilter("snapshot-[0-9a-f]+.json"));
        Arrays.sort(files);
        return files;
    }

    private String generateRepositoryId() {
        SecureRandom rnd = new SecureRandom();
        byte[] a = new byte[8];
        rnd.nextBytes(a);
        return new BigInteger(a).abs().toString(36);
    }

    @Override
    public void close() throws IOException {
        snapshotTimer.cancel();
        txLog.close();
    }

    private synchronized Snapshot<K, V> createSnapshot() throws IOException {
        Snapshot<K, V> s = new Snapshot<K, V>();
        s.storeId = storeId;
        s.txId = txLog.getNextMessageId();
        s.maps = new HashMap<String, Map<K, V>>();
        for (Map.Entry<String, ConcurrentMap<K, V>> e : maps.entrySet()) {
            s.maps.put(e.getKey(), new HashMap<K, V>(e.getValue()));
        }
        return s;
    }

    @Override
    public synchronized void createSnapshot(OutputStream out) throws IOException {
        txLog.sync();
        Snapshot snapshot = createSnapshot();
        snapshot.txId = txLog.getNextMessageId();
        serializer.serialize(snapshot, out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void loadSnapshot(InputStream in) throws IOException {
        if (!isEmpty()) throw new IllegalStateException("Repository is not empty");
        Snapshot<K, V> snapshot = (Snapshot<K, V>)serializer.deserialize(in, Snapshot.class);
        if (snapshot.txId == null) throw new IllegalArgumentException("Snapshot is missing txId");
        storeId = snapshot.storeId;
        txLog.setFirstMessageId(snapshot.txId);
        for (Map.Entry<String, Map<K, V>> e : snapshot.maps.entrySet()) {
            maps.put(e.getKey(), new ConcurrentHashMap<K, V>(e.getValue()));
        }
        saveSnapshot();
    }

    @Override
    public boolean isEmpty() {
        return maps.isEmpty();
    }

    /**
     * Save a snapshot. This is a NOP if we are already busy saving a snapshot or if no new transactions have been
     * applied since the most recent snapshot was saved.
     */
    public void saveSnapshot() throws IOException {
        Snapshot snapshot;
        long id;
        try {
            synchronized (this) {
                if (busySavingSnapshot) return;
                busySavingSnapshot = true;
                txLog.sync();
                id = txLog.getNextMessageId();
                if (id == mostRecentSnapshotId) return; // nothing to do
                snapshot = createSnapshot();
            }
            File f = new File(dir, "snapshot-" + String.format("%016x", id) + ".json");
            if (log.isDebugEnabled()) log.debug("Creating " + f);
            boolean ok = false;
            FileOutputStream out = new FileOutputStream(f);
            try {
                serializer.serialize(snapshot, out);
                out.flush();
                out.getChannel().force(true);
                out.close();
                synchronized (this) {
                    mostRecentSnapshotId = id;
                }
                ok = true;
            } finally {
                if (!ok) {
                    try {
                        out.close();
                    } catch (IOException ignore) {
                    }
                    if (!f.delete()) {
                        log.error("Unable to delete bad snapshot: " + f);
                    }
                }
            }

            deleteOldSnapshots();

        } finally {
            synchronized (this) {
                busySavingSnapshot = false;
            }
        }
    }

    private void deleteOldSnapshots() {
        File[] a = getSnapshotFiles();
        for (int i = 0; i < (a.length - snapshotCount); i++) {
            if (a[i].delete()) {
                if (log.isDebugEnabled()) log.debug("Deleted " + a[i]);
            } else {
                log.error("Unable to delete " + a[i]);
            }
        }
    }

    @Override
    public ConcurrentMap<K, V> namespace(String namespace) {
        return new Namespace(namespace);
    }

    public class Namespace implements ConcurrentMap<K, V> {

        private final String namespace;

        public Namespace(String namespace) {
            this.namespace = namespace;
        }

        @Override
        public V putIfAbsent(K key, V value) {
            return null;
        }

        @Override
        public boolean remove(Object key, Object value) {
            return false;
        }

        @Override
        public boolean replace(K key, V oldValue, V newValue) {
            return false;
        }

        @Override
        public V replace(K key, V value) {
            return null;
        }

        @Override
        public int size() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? 0 : m.size();
        }

        @Override
        public boolean isEmpty() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null || m.isEmpty();
        }

        @Override
        public boolean containsKey(Object key) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m != null && m.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m != null && m.containsValue(value);
        }

        @Override
        public V get(Object key) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? null : m.get(key);
        }

        @Override
        public V put(K key, V value) {
            return null;
        }

        @Override
        public V remove(Object key) {
            return null;
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
        }

        @Override
        public void clear() {
        }

        @Override
        public Set<K> keySet() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? null : m.keySet();
        }

        @Override
        public Collection<V> values() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? null : m.values();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? null : m.entrySet();
        }
    }

}
