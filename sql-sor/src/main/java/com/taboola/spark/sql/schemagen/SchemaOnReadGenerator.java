package com.taboola.spark.sql.schemagen;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.types.StructType;

import com.taboola.logging.LogWrapper;
import com.taboola.logging.ToStringSupplier;

import scala.collection.JavaConversions;

public class SchemaOnReadGenerator {
    private final static LogWrapper logger = new LogWrapper(SchemaOnReadGenerator.class);
    private final SchemaOnReadState state;
    private final long duration;



    private SchemaOnReadGenerator(SchemaOnReadState state, long duration) {
        this.state = state;
        this.duration = duration;
    }

    public static SchemaOnReadGenerator generateSchemaOnRead(Dataset<Row> dataframe) {
        return generateSchemaOnRead(dataframe.logicalPlan(), dataframe.sparkSession());
    }

    public static SchemaOnReadGenerator generateSchemaOnRead(Dataset<Row> dataframe, Supplier<Long> clockSupplier) {
        return generateSchemaOnRead(dataframe.logicalPlan(), dataframe.sparkSession(), clockSupplier);
    }

    public static SchemaOnReadGenerator generateSchemaOnRead(TreeNode logicalPlan, SparkSession session) {
        return generateSchemaOnRead(logicalPlan, session, () -> System.currentTimeMillis());
    }

    public static SchemaOnReadGenerator generateSchemaOnRead(TreeNode logicalPlan, SparkSession session, Supplier<Long> clockSupplier) {
        long startMs = clockSupplier.get();
        Set<TreeNode> upperMostSelectionNodes = new HashSet<>();
        findUpperMostSelectionNodes(logicalPlan, upperMostSelectionNodes);

        SchemaOnReadState state = new SchemaOnReadState(session, upperMostSelectionNodes);

        SchemaOnReadExtractor extractor = new SchemaOnReadExtractor(state);

        logicalPlan.foreachUp(extractor);

        extractor.restoreDirectRelationSchemaOnRead();
        long endMs = clockSupplier.get();
        state.getRelationRefSchemas().entrySet().stream()
                .forEach(e -> logger.debug("Schema for relationRef %s is:\n%s",
                        e.getKey(),
                        // Using Supplier<String> so that prettyJson would only be invoked if declared log level is enabled
                        new ToStringSupplier(() -> e.getValue().getSchema().prettyJson())));

        return new SchemaOnReadGenerator(state, endMs - startMs);
    }


    private static void findUpperMostSelectionNodes(TreeNode currNode, Set<TreeNode> upperMostSelectionNodes) {
        if (SchemaOnReadUtils.containsColumnsForQuery(currNode)) {
            upperMostSelectionNodes.add(currNode);
        } else {
            final List<TreeNode> childTreeNodes = JavaConversions.seqAsJavaList(currNode.children());
            childTreeNodes.forEach(node -> findUpperMostSelectionNodes(node, upperMostSelectionNodes));
        }
    }

    public long getDuration() {
        return duration;
    }

    public StructType getSchemaOnRead(String... inputPaths) {
        return state.getRelationSchema(inputPaths);
    }

    public StructType getSchemaOnReadByAlias(String alias) {
        return state.getAliasSchema(alias);
    }
}
