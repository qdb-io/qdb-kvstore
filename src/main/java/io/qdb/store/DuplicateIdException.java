package io.qdb.store;

/**
 * Thrown on an attempt to create a new object with id the same as an existing object.
 */
public class DuplicateIdException extends KeyValueStoreException {

    public DuplicateIdException(String message) {
        super(message);
    }
}
