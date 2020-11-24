package com.taboola.spark.sql.schemagen;

import java.util.function.BiConsumer;

import org.apache.log4j.Level;
import org.apache.spark.sql.catalyst.trees.TreeNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taboola.logging.LogWrapper;
import com.taboola.logging.ThreadedLogLevelManager;

public interface VisitorBiConsumer<V extends SchemaOnReadExtractor, T extends TreeNode> extends BiConsumer<V, T> {
    LogWrapper logger = new LogWrapper(VisitorBiConsumer.class);

    @Override
    default void accept(V visitor, T treeNode) {
        logger.debug("%s is visiting %s (%s): %s", visitor.getClass().getSimpleName(), treeNode.nodeName(), treeNode.hashCode(), treeNode);
        try {
            if (ThreadedLogLevelManager.isEnabledFor(logger, Level.TRACE)){
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
