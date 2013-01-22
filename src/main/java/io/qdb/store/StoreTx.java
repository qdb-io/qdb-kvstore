package io.qdb.store;

/**
 * A change to a data store.
 */
public class StoreTx<K, V> {

    enum Operation { CREATE, UPDATE, DELETE }

    public Operation op;
    public K key;
    public V value;

    public StoreTx() { }

    public StoreTx(Operation op, K key, V value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return op + " " + value;
    }

}
