package io.qdb.kvstore;

/**
 * Extracts version numbers out of our model objects.
 */
public class VersionProvider implements KeyValueStore.VersionProvider<ModelObject> {

    @Override
    public Object getVersion(ModelObject value) {
        return value.version;
    }

    @Override
    public void incVersion(ModelObject value) {
        ++value.version;
    }
}
