package io.qdb.kvstore.cluster;

import io.qdb.kvstore.StoreTx;

/**
 * Sends messages to servers in our cluster.
 */
public interface Transport extends Paxos.Transport<SequenceNo, StoreTx> {

    /**
     * Get our server.
     */
    String getSelf();

}
