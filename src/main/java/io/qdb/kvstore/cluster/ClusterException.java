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

    public ClusterException(Throwable cause) {
        super(cause);
    }
}
