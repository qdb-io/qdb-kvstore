package io.qdb.kvstore;

import java.io.Serializable;

/**
 * A change to a data store.
 */
public class StoreTx<K, V> implements Serializable {

    enum Operation { PUT, REMOVE, PUT_IF_ABSENT, REMOVE_KV, REPLACE, REPLACE_KVV }

    public String namespace;
    public Operation op;
    public K key;
    public V value;
    public V oldValue;

    public StoreTx() { }

    public StoreTx(String namespace, Operation op, K key) {
        this(namespace, op, key, null, null);
    }

    public StoreTx(String namespace, Operation op, K key, V value) {
        this(namespace, op, key, value, null);
    }

    public StoreTx(String namespace, Operation op, K key, V value, V oldValue) {
        this.namespace = namespace;
        this.op = op;
        this.key = key;
        this.value = value;
        this.oldValue = oldValue;
    }

    @Override
    public String toString() {
        return namespace + " " + op + " k=" + key + (value == null ? "" : " v=" + value) +
                (oldValue == null ? "" : " ov=" + oldValue);
    }

}
