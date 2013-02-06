package io.qdb.kvstore.cluster;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;

import java.io.*;

/**
 * Extra API methods for clustering.
 */
public interface ClusteredKeyValueStore<K, V> extends KeyValueStore<K, V> {

    /**
     * What will the id of the next tx appended to the tx log be?
     */
    long getNextTxId() throws IOException;

    /**
     * Append tx to the tx log and apply it to the in memory maps. This is called when the cluster has accepted
     * a transaction (either one we have proposed or one from another server).
     */
    Object appendToTxLogAndApply(StoreTx tx) throws IOException;

}
