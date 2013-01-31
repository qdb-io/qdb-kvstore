package io.qdb.kvstore.cluster;

import io.qdb.kvstore.KeyValueStore;
import io.qdb.kvstore.StoreTx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusterMemberForTests implements ClusterMember {

    public KeyValueStore.Status status;
    public long nextTxId;
    public List<StoreTx> txLog = new ArrayList<StoreTx>();

    @Override
    public void setStatus(KeyValueStore.Status status) {
        this.status = status;
    }

    @Override
    public long getNextTxId() throws IOException {
        return nextTxId;
    }

    @Override
    public Object appendToTxLogAndApply(StoreTx tx) {
        txLog.add(tx);
        return txLog.size();
    }
}
