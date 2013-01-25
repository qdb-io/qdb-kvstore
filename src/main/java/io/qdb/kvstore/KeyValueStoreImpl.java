package io.qdb.kvstore;

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

        storeId = snapshot == null ? generateStoreId() : snapshot.storeId;
        if (snapshot != null) populateMapsFromSnapshot(snapshot);

        int count = 0;
        for (MessageCursor c = txLog.cursor(mostRecentSnapshotId); c.next(); count++) {
            StoreTx tx = this.serializer.deserialize(new ByteArrayInputStream(c.getPayload()), StoreTx.class);
            try {
                apply(tx);
            } catch (KeyValueStoreException e) {
                if (log.isDebugEnabled()) log.debug("Got " + e + " replaying " + tx);
            }
        }
        if (log.isDebugEnabled()) log.debug("Replayed " + count + " transaction(s)");

        snapshotTimer = new Timer("kvstore-snapshot-" + dir.getName(), true);
    }

    private File[] getSnapshotFiles() {
        File[] files = dir.listFiles(new RegexFilenameFilter("[0-9a-f]+\\.snapshot"));
        Arrays.sort(files);
        return files;
    }

    private String generateStoreId() {
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

    @SuppressWarnings("unchecked")
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
        serializer.serialize(createSnapshot(), out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void loadSnapshot(InputStream in) throws IOException {
        if (!isEmpty()) throw new IllegalStateException("Store is not empty");
        Snapshot<K, V> snapshot = (Snapshot<K, V>)serializer.deserialize(in, Snapshot.class);
        if (snapshot.txId == null) throw new IllegalArgumentException("Snapshot is missing txId");
        storeId = snapshot.storeId;
        txLog.setFirstMessageId(snapshot.txId);
        populateMapsFromSnapshot(snapshot);
        saveSnapshot();
    }

    private void populateMapsFromSnapshot(Snapshot<K, V> snapshot) {
        for (Map.Entry<String, Map<K, V>> e : snapshot.maps.entrySet()) {
            maps.put(e.getKey(), new ConcurrentHashMap<K, V>(e.getValue()));
        }
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
            File f = new File(dir, String.format("%016x", id) + ".snapshot");
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

    /**
     * Attempt to apply tx. It is first proposed to the cluster (if any) and written to the transaction log and then
     * applied to our maps.
     */
    private synchronized Object exec(StoreTx<K, V> tx) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            serializer.serialize(tx, bos);
        } catch (IOException e) {
            throw new KeyValueStoreException(e);
        }
        byte[] payload = bos.toByteArray();

        propose(tx);
        appendToTxLog(payload);
        return apply(tx);
    }

    private void appendToTxLog(byte[] payload) {
        long timestamp = System.currentTimeMillis();
        boolean snapshotNow = false;
        try {
            long txId = txLog.append(timestamp, null, payload);
            // the bytes calculation isn't perfectly accurate but good enough
            long bytes = (txId + payload.length) - mostRecentSnapshotId;
            snapshotNow = bytes > txLog.getMaxSize() / 2; // half our log space is gone so do a snapshot now
        } catch (IOException e) {
            throw new KeyValueStoreException("Error appending to txLog: " + e, e);
            // todo we should go offline if we cannot write to our tx log
        } finally {
            scheduleSnapshot(snapshotNow);
        }
    }

    private synchronized void scheduleSnapshot(boolean asap) {
        if (!snapshotScheduled) {
            snapshotTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        synchronized (KeyValueStoreImpl.this) {
                            snapshotScheduled = false;
                        }
                        saveSnapshot();
                    } catch (Throwable e) {
                        log.error("Error saving snapshot: " + e, e);
                        // todo the store should go offline if it cannot save snapshots
                    }
                }
            }, asap ? 1L : snapshotIntervalSecs * 1000L);
        }
    }

    /**
     * Propose tx as the next transaction to the cluster and return only when it has been accepted or throw a
     * {@link KeyValueStoreException} otherwise.
     */
    @SuppressWarnings("UnusedParameters")
    private void propose(StoreTx<K, V> tx) {
        // nop unless clustered
    }

    /**
     * Make changes to our in memory maps based on tx.
     */
    private synchronized Object apply(StoreTx<K, V> tx) {
        ConcurrentMap<K, V> m = maps.get(tx.namespace);
        V existing;
        switch (tx.op) {
            case PUT:
            case REPLACE:
                existing = m != null ? m.get(tx.key) : null;
                if (existing != null) checkVersionNumbers(tx, existing);
                if (tx.op == StoreTx.Operation.PUT || existing != null) {
                    if (m == null) maps.put(tx.namespace, m = new ConcurrentHashMap<K, V>());
                    versionProvider.incVersion(tx.value);
                    m.put(tx.key, tx.value);
                }
                return existing;

            case REPLACE_KVV:
                if (m == null) return Boolean.FALSE;
                boolean replace = m.replace(tx.key, tx.oldValue, tx.value);
                if (replace) versionProvider.incVersion(tx.value);
                return replace;

            case PUT_IF_ABSENT:
                if (m == null) maps.put(tx.namespace, m = new ConcurrentHashMap<K, V>());
                V v = m.putIfAbsent(tx.key, tx.value);
                if (v == null) versionProvider.incVersion(tx.value);
                return v;

            case REMOVE:
                if (m == null) return null;
                V ans = m.remove(tx.key);
                if (m.isEmpty()) maps.remove(tx.namespace);
                return ans;

            case REMOVE_KV:
                if (m == null) return Boolean.FALSE;
                existing = m.get(tx.key);
                if (existing == null) return Boolean.FALSE;
                checkVersionNumbers(tx, existing);
                Boolean bool = m.remove(tx.key, tx.value);
                if (m.isEmpty()) maps.remove(tx.namespace);
                return bool;
        }
        throw new KeyValueStoreException("Unhandled operation: " + tx);
    }

    private void checkVersionNumbers(StoreTx<K, V> tx, V existing) {
        Object v1 = versionProvider.getVersion(existing);
        Object v2 = versionProvider.getVersion(tx.value);
        if (v1 != null && !v1.equals(v2)) {
            throw new OptimisticLockingException("Existing value for " + tx.namespace + "." + tx.key + " " +
                    "has version " + v1 + ", value has version " + v2 + ": " + tx.value);
        }
    }

    @Override
    public ConcurrentMap<K, V> getMap(String namespace) {
        return new Namespace(namespace);
    }

    @SuppressWarnings({"unchecked", "NullableProblems"})
    public class Namespace implements ConcurrentMap<K, V> {

        private final String namespace;

        public Namespace(String namespace) {
            this.namespace = namespace;
        }

        public V put(K key, V value) {
            return (V)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.PUT, key, value));
        }

        public V putIfAbsent(K key, V value) {
            return (V)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.PUT_IF_ABSENT, key, value));
        }

        public V remove(Object key) {
            return (V)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.REMOVE, (K) key));
        }

        public boolean remove(Object key, Object value) {
            return (Boolean)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.REMOVE_KV, (K) key, (V) value));
        }

        public V replace(K key, V value) {
            return (V)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.REPLACE, key, value));
        }

        public boolean replace(K key, V oldValue, V newValue) {
            return (Boolean)exec(new StoreTx<K, V>(namespace, StoreTx.Operation.REPLACE_KVV, key, newValue, oldValue));
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            for (Entry<? extends K, ? extends V> e : m.entrySet()) put(e.getKey(), e.getValue());
        }

        public void clear() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            if (m == null) return;
            List<K> list = new ArrayList<K>(m.keySet());
            for (K id : list) remove(id);
        }

        public int size() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? 0 : m.size();
        }

        public boolean isEmpty() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null || m.isEmpty();
        }

        public boolean containsKey(Object key) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m != null && m.containsKey(key);
        }

        public boolean containsValue(Object value) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m != null && m.containsValue(value);
        }

        public V get(Object key) {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? null : m.get(key);
        }

        public Set<K> keySet() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? Collections.EMPTY_SET : m.keySet();
        }

        public Collection<V> values() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? Collections.EMPTY_LIST : m.values();
        }

        public Set<Entry<K, V>> entrySet() {
            ConcurrentMap<K, V> m = maps.get(namespace);
            return m == null ? Collections.EMPTY_SET : m.entrySet();
        }
    }

}
