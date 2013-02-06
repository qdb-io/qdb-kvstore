package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * A store transaction and its id. This is used to stream transactions between servers in a cluster.
 */
public class StoreTxAndId implements Serializable {

    public long txId;
    public StoreTx storeTx;

    public StoreTxAndId() {
    }

    public StoreTxAndId(long txId, StoreTx storeTx) {
        this.txId = txId;
        this.storeTx = storeTx;
    }

    /**
     * Iterates over StoreTxAndId instances. Close these when you are done with them.
     */
    public static interface Iter extends Closeable {

        /**
         * Get the next tx or null if there are no more.
         */
        public StoreTxAndId next() throws IOException;

    }
}
