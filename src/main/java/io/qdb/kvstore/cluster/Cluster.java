package io.qdb.kvstore.cluster;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;
import java.io.Closeable;

/**
 * The interface between a KV store and a cluster.
 */
public interface Cluster extends Closeable {

    /**
     * The store calls this to figure out what its status is.
     */
    public KeyValueStore.Status getStoreStatus();

    /**
     * The store wants to re-join the cluster i.e. it must already be known to be part of the cluster by the other
     * servers. This method starts the join process asynchronously. It should update the status of the store to UP
     * when the store has joined the cluster. This method is only called once during store startup.
     */
    void init(ClusteredKeyValueStore store);

    /**
     * The store has proposed a transaction to the cluster. This method must block until the transaction has been
     * accepted by the other servers in the cluster in which case it must call
     * {@link ClusteredKeyValueStore#appendToTxLogAndApply(io.qdb.kvstore.StoreTx)} and return the result. The
     * store will only propose one tx at a time i.e. there are no parallel proposals.
     */
    Object propose(StoreTx tx) throws ClusterException;
}
