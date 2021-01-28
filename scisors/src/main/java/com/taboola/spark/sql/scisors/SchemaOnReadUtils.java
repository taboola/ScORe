package com.taboola.spark.sql.schemagen;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields;
import org.apache.spark.sql.catalyst.expressions.GetStructField;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.types.DataType;

import com.google.common.collect.Sets;

import scala.collection.JavaConversions;
import scala.collection.Seq;


public class SchemaOnReadUtils {

    private final static Set<Class<org.apache.spark.sql.catalyst.trees.TreeNode>> selectionNodeTypes = new HashSet() {{
        add(Project.class);
        add(Aggregate.class);
        add(Window.class);
        add(Join.class);
        add(Generate.class);
        add(Sort.class);
    }};

    static boolean isStruct(DataType dataType) {
        return "struct".equals(dataType.typeName());
    }

    static boolean isArray(DataType dataType) {
        return "array".equals(dataType.typeName());
    }

    static boolean isMap(DataType type) {
        return "map".equals(type.typeName());
    }

    static boolean isComplex(DataType dataType) {
        return isStruct(dataType) ||
                isArray(dataType) ||
                isMap(dataType);
    }

    static boolean containsColumnsForQuery(TreeNode treeNode) {
        return selectionNodeTypes.contains(treeNode.getClass());
    }

    public static Set<TreeNode> getMandatorySchemaColumns(TreeNode node) {
        if (node instanceof Sort) {
            return getMandatorySchemaColumns((Sort)node);
        } else if (node instanceof Aggregate) {
            return getMandatorySchemaColumns((Aggregate)node);
        } else if (node instanceof Join) {
            return getMandatorySchemaColumns((Join)node);
        } else if (node instanceof Window) {
            return getMandatorySchemaColumns((Window)node);
        }
        return Collections.emptySet();
    }

    public static Set<TreeNode> getConditionalSchemaColumns(TreeNode node) {
        if (node instanceof Project) {
            return getConditionalSchemaColumns((Project)node);
        } else if (node instanceof Window) {
            return getConditionalSchemaColumns((Window)node);
        } else if (node instanceof Aggregate) {
            return getConditionalSchemaColumns((Aggregate)node);
        } else if (node instanceof Generate) {
            return getConditionalSchemaColumns((Generate)node);
        }
        return Collections.emptySet();
    }

    public static Set<TreeNode> getConditionalSchemaColumns(Project projection) {
        return JavaConversions.seqAsJavaList(projection.projectList())
                .stream().map(ne -> (TreeNode)ne).map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getMandatorySchemaColumns(Sort sort) {
        return JavaConversions.seqAsJavaList(sort.order())
                .stream().map(ne -> (TreeNode)ne).map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    private static Set<TreeNode> findSelection(TreeNode node) {
        if (node instanceof GetStructField || node instanceof AttributeReference || node instanceof GetArrayStructFields) {
            return Sets.newHashSet(node);
        }
        else {
            List<TreeNode> children = JavaConversions.seqAsJavaList(node.children());
            final Stream<Set<TreeNode>> setStream = children.stream().map(n -> findSelection(n));
            final Set<TreeNode> treeNodes = setStream.flatMap(s -> s.stream()).collect(Collectors.toSet());
            treeNodes.add(node);
            return treeNodes;
        }
    }

    public static Set<TreeNode> getConditionalSchemaColumns(Window window) {
        return JavaConversions.seqAsJavaList(window.windowExpressions()).stream().map(e -> (TreeNode) e).map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getMandatorySchemaColumns(Window window) {
        final Stream<Set<TreeNode>> partitionSpecStream = JavaConversions.seqAsJavaList(window.partitionSpec()).stream().map(e -> (TreeNode)e).map(SchemaOnReadUtils::findSelection);
        final Stream<Set<TreeNode>> orderSpecStream = JavaConversions.seqAsJavaList(window.orderSpec()).stream().map(e -> (TreeNode)e).map(SchemaOnReadUtils::findSelection);
        return Stream.of( partitionSpecStream, orderSpecStream).flatMap(s -> s)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getConditionalSchemaColumns(Aggregate aggregate) {
        return JavaConversions.seqAsJavaList(aggregate.aggregateExpressions()).stream().map(e -> (TreeNode) e)
                .map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getMandatorySchemaColumns(Aggregate aggregate) {
        return JavaConversions.seqAsJavaList(aggregate.groupingExpressions()).stream().map(e -> (TreeNode) e)
                .map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getMandatorySchemaColumns(Join join) {
        if (join.condition().isEmpty()) {
            return Collections.emptySet();
        }

        return JavaConversions.seqAsJavaList(join.condition().get().children()).stream().map(e -> (TreeNode) e)
                .map(SchemaOnReadUtils::findSelection)
                .flatMap(s -> s.stream()).collect(Collectors.toSet());
    }

    public static Set<TreeNode> getConditionalSchemaColumns(Generate generate) {
        final Seq<Expression> children = ((TreeNode)generate.generator()).children();
        final Stream<Set<TreeNode>> generetorOutputStream = JavaConversions.seqAsJavaList(generate.generatorOutput()).stream().map(e -> (TreeNode) e)
                .map(SchemaOnReadUtils::findSelection);

        final Stream<Set<TreeNode>> generatorStream = JavaConversions.seqAsJavaList(children).stream().map(e -> (TreeNode) e)
                .map(SchemaOnReadUtils::findSelection);

        return Stream.concat(generetorOutputStream, generatorStream)
                .flatMap(s -> s.stream()).collect(Collectors.toCollection(() -> new LinkedHashSet<>()));
    }
}
