package com.taboola.spark.sql.scisors;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;

public class SchemaElementImpl implements SchemaElement{
    private String name;
    private Map<String, SchemaElement> childElements;

    private AtomicBoolean requiredInSchema;
    private DataType fullSchema;
    private DataType partialSchema;

    public static SchemaElementImpl createNode(String name) {
        return new SchemaElementImpl(name, true);
    }

    public static SchemaElementImpl createLeaf(String name) {
        return new SchemaElementImpl(name, false);
    }


    private SchemaElementImpl(String name,  boolean isNode) {
        this.name = name;
        this.requiredInSchema = new AtomicBoolean(false);
        this.childElements = new HashMap<>();
    }

    @Override
    public SchemaElement cloneAlias(String aliasName) {
        SchemaElementImpl cloned = new SchemaElementImpl(aliasName, true);
        cloned.childElements = this.childElements;
        cloned.requiredInSchema = this.requiredInSchema;

        return cloned;
    }

    @Override
    public void setFullSchema(DataType dataType) {
        this.fullSchema = dataType;
        if (!SchemaOnReadUtils.isComplex(dataType)) {
            requiredInSchema.set(true);
        }
    }

    @Override
    public void setPartialSchema(DataType dataType) {
        if (getRequiredInSchema()) {
            this.partialSchema = fullSchema;
        } else if (getSelfOrDescendentRequiredInSchema()) {
            this.partialSchema = dataType;
        } else {
            this.partialSchema = skeletonType(dataType);
        }
    }

    private static DataType skeletonType(DataType dataType) {
        if (SchemaOnReadUtils.isStruct(dataType)) {
            return DataTypes.createStructType(Collections.emptyList());
        } else if (SchemaOnReadUtils.isArray(dataType)) {
            return DataTypes.createArrayType(skeletonType(((ArrayType) dataType).elementType()));
        } else if (SchemaOnReadUtils.isMap(dataType)) {
            return DataTypes.createMapType(
                    skeletonType(((MapType) dataType).keyType()),
                    skeletonType(((MapType) dataType).valueType())
            );
        }

        return dataType;
    }

    @Override
    public DataType getFullSchema() {
        return fullSchema;
    }

    @Override
    public DataType getPartialSchema() {
        return partialSchema;

    }

    @Override
    public boolean getRequiredInSchema() {
        return requiredInSchema.get();
    }

    @Override
    public boolean getSelfOrDescendentRequiredInSchema() {
        return getRequiredInSchema() ||
                getChildElements().values().stream().filter(e -> e.getSelfOrDescendentRequiredInSchema()).findAny().isPresent();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<String, SchemaElement> getChildElements() {
        return childElements;
    }

    @Override
    public void setRequiredInSchema(boolean requiredInSchema) {
        this.requiredInSchema.compareAndSet(false, requiredInSchema);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
