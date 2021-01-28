package com.taboola.spark.sql.scisors;

import org.apache.spark.sql.types.StructType;

class RelationSchema {
    private StructType schemaOnRead;
    private StructType fullSchema;

    RelationSchema(StructType schema) {
        this.schemaOnRead = new StructType();
        this.fullSchema = schema;
    }

    public StructType getSchema() {
        return schemaOnRead;
    }

    public void setSchema(StructType schema) {
        this.schemaOnRead = schema;
    }

    public StructType getFullSchema() {
        return fullSchema;
    }
}
