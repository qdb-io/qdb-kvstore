package io.qdb.kvstore;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * What we store for tests.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="class")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ModelObject.class, name = "ModelObject")
})
public class ModelObject {

    public int version;
    public String name;

    public ModelObject() { }

    public ModelObject(String name) {
        this.name = name;
    }

    public ModelObject(String name, int version) {
        this(name);
        this.version = version;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ModelObject && name.equals(((ModelObject)o).name);
    }

    @Override
    public String toString() {
        return name + " v" + version;
    }
}
