package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

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
}
