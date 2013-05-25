package io.qdb.kvstore;

import java.io.Serializable;
import java.util.Map;

/**
 * A copy of the data in a data store. {@link KeyValueStoreSerializer}'s must be able to serialize and de-serialize
 * these.
 */
public class StoreSnapshot<K, V> implements Serializable {
    public Long txId;
    public Map<String, Map<K, V>> maps;
}
