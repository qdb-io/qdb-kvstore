package io.qdb.kvstore;

import io.qdb.kvstore.cluster.ClusterException;
import java.io.Closeable;

/**
 * The interface between a KV store and a cluster.
 */
public interface Cluster<S extends KeyValueStore> extends Closeable {

    /**
     * The store wants to join the cluster. This method starts the join process asynchronously. It should update the
     * status of the store to UP when the store has joined the cluster.
     */
    void join(S store);

    /**
     * The store has proposed a transaction to the cluster. It should be successfully applied or a ClusterException
     * must be thrown.
     */
    void propose(StoreTx tx) throws ClusterException;
}
