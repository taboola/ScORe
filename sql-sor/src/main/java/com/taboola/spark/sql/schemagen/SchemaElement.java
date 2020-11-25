package com.taboola.spark.sql.schemagen;

import java.util.Map;

import org.apache.spark.sql.types.DataType;

interface SchemaElement {
    String getName();
    Map<String, SchemaElement> getChildElements();

    boolean getRequiredInSchema();

    void setRequiredInSchema(boolean requiredInSchema);

    boolean getSelfOrDescendentRequiredInSchema();

    SchemaElement cloneAlias(String aliasName);

    void setFullSchema(DataType dataType);

    void setPartialSchema(DataType dataType);

    DataType getFullSchema();

    DataType getPartialSchema();
}
