package com.taboola.spark.sql.schemagen;

import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.trees.TreeNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public interface VisitorBiConsumer<V extends SchemaOnReadExtractor, T extends TreeNode> extends BiConsumer<V, T> {
    Logger logger = LogManager.getLogger(VisitorBiConsumer.class);

    @Override
    default void accept(V visitor, T treeNode) {
        logger.debug("{} is visiting {} ({}): {}", visitor.getClass().getSimpleName(), treeNode.nodeName(), treeNode.hashCode(), treeNode);
        try {
            if (logger.isTraceEnabled()){
                logger.trace(new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(visitor.getState().getRelationColumns()));
            }
        } catch (JsonProcessingException e) {
            logger.error(e);
        }
        visitor.getState().enterNode(treeNode);
        this.visit(visitor, treeNode);
        visitor.getState().exitNode(treeNode);
    }

    void visit(V visitor, T treeNode);
}
