package io.qdb.kvstore.cluster;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * This is the link between the cluster code and the key value store.
 */
public interface ClusterMember {

    /**
     * What will the id of the next tx appended to the tx log be?
     */
    long getNextTxId() throws IOException;

    /**
     * Append tx to the tx log and apply it to the in memory maps. This is called when the cluster has accepted
     * a transaction (either one we have proposed or one from another server).
     */
    Object appendToTxLogAndApply(StoreTx tx) throws IOException;

    /**
     * Populate this store with data from the snapshot. Note that this is only allowed if the store is
     * {@link #isEmpty()}.
     */
    void loadSnapshot(InputStream in) throws IOException;

    /**
     * Does this store contain no objects?
     */
    boolean isEmpty();

}
