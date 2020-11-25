package com.taboola.spark.sql.schemagen;

import java.util.List;

class SQLParsedExpression {
    String alias;
    List<String> columnPath;

    public SQLParsedExpression(String alias, List<String> columnPath) {
        this.alias = alias;
        this.columnPath = columnPath;
    }

    public String getAlias() {
        return alias;
    }

    public List<String> getColumnPath() {
        return columnPath;
    }
}
