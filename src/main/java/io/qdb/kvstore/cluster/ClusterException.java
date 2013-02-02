package io.qdb.kvstore.cluster;

import io.qdb.kvstore.KeyValueStoreException;

/**
 * Something has gone wrong concerning clustering.
 */
public class ClusterException extends KeyValueStoreException {

    public ClusterException(String message) {
        super(message);
    }

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }

    public static class Timeout extends ClusterException {
        public Timeout(String message) {
            super(message);
        }
    }

    public static class Interrupted extends ClusterException {
        public Interrupted(String message) {
            super(message);
        }
    }

    public static class OutOfSync extends ClusterException {

        public final String from;
        public final long txId;

        public OutOfSync(String message, String from, long txId) {
            super(message);
            this.from = from;
            this.txId = txId;
        }
    }
}
