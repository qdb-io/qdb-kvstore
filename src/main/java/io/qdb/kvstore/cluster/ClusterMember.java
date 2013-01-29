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
     * Change the status of the store. The clustering code knows when it should be up etc and must call this whenever
     * that changes.
     */
    public void setStatus(KeyValueStore.Status status);

    /**
     * What will the id of the next tx appended to the tx log be?
     */
    public long getNextTxId() throws IOException;

    /**
     * Append tx to the tx log and apply it to the in memory maps. This is called when the cluster has accepted
     * a transaction (either one we have proposed or one from another server).
     */
    public Object appendToTxLogAndApply(StoreTx tx);

}
