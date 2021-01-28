package com.taboola.spark.sql.scisors;

import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields;
import org.apache.spark.sql.catalyst.expressions.GetStructField;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Sort;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.Window;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;

class TreeNodeProcessingRegistry {
    static VisitorBiConsumer<SchemaOnReadExtractor, LogicalRelation> processLogicalRelation =
        (e,r) -> {
            if (r.relation() instanceof HadoopFsRelation) {
                e.getState().setHadoopFsRelation((HadoopFsRelation) r.relation());
            }
        };

    static VisitorBiConsumer<SchemaOnReadExtractor, LocalRelation> processLocalRelation =
        (e, r) -> e.getState().resetRelation();

    static VisitorBiConsumer<SchemaOnReadExtractor, SubqueryAlias> processSubqueryAlias =
        (e,alias) -> e.getState().addAliasForRelationRef(alias.alias());

    static VisitorBiConsumer<SchemaOnReadExtractor, Project> processProjection =
            (e,p)-> processSelectionNode(e);

    static VisitorBiConsumer<SchemaOnReadExtractor, Sort> processSort =
            (e,s) -> processSelectionNode(e);

    static VisitorBiConsumer<SchemaOnReadExtractor, Filter> processFilter =
        (e, filter) -> {
            JavaConversions.seqAsJavaList(filter.condition().children())
                    .stream()
                    .forEach((TreeNode exp) -> exp.foreachUp(e));
        };

    static VisitorBiConsumer<SchemaOnReadExtractor, Aggregate> processAggregate =
        (e, agg) -> processSelectionNode(e);


    static VisitorBiConsumer<SchemaOnReadExtractor, Alias> processAlias =
            (e, a) -> {
                ((TreeNode)a.child()).foreachUp(e);
                e.getState().setAlias(a.name());
            };


    // This is a complete hack!!! When encountering an alias over window expression,
    // we need to make sure that potentialAliasedNode would be the correct SchemaElement
    // so we process the window function of the expression, to make sure it points to the correct SchemaElement
    static VisitorBiConsumer<SchemaOnReadExtractor, WindowExpression> processWindowExpression =
            (e, we) -> {
                ((TreeNode)we.windowFunction()).foreachUp(e);
            };

    static VisitorBiConsumer<SchemaOnReadExtractor, Window> processWindow =
            (e, w) -> {
                e.getState().getCurrentNodeState().getColumnsWithMandatoryFullSchema().forEach(node -> e.apply(node));
                e.getState().getCurrentNodeState().getColumnsWithConditionalFullSchema().forEach(node -> e.apply(node));
                finalizeRelationsToProcess(e);
            };

    static VisitorBiConsumer<SchemaOnReadExtractor, Join> processJoin =
        (e, join) -> {
            if (join.condition().nonEmpty()) {
                processSelectionNode(e);
            }
        };

    static VisitorBiConsumer<SchemaOnReadExtractor, Generate> processGenerate =
            (e, g) -> processSelectionNode(e);

    static VisitorBiConsumer<SchemaOnReadExtractor, GetArrayStructFields> processGetArrayStructFields =
         (e, field) -> {
            e.getState().addToRelationColumns(field.sql(), field.dataType());
         };

    static VisitorBiConsumer<SchemaOnReadExtractor, AttributeReference> processAttributeReference =
        (e,att) -> {
            if (!SQLExpressionParser.windowReferenceAttribute(att.sql())) {
                e.getState().addToRelationColumns(att.sql(), att.dataType());
            }
        };

    static VisitorBiConsumer<SchemaOnReadExtractor, GetStructField> processGetStructField =
        (e, field) -> e.getState().addToRelationColumns(field.sql(), field.dataType());

    static void processSelectionNode(SchemaOnReadExtractor extractor) {
        extractor.getState().getCurrentNodeState().getColumnsWithMandatoryFullSchema().forEach(node -> extractor.apply(node));
        extractor.getState().getCurrentNodeState().getColumnsWithConditionalFullSchema().forEach(node -> extractor.apply(node));
        finalizeRelationsToProcess(extractor);
    }

    public static void finalizeRelationsToProcess(SchemaOnReadExtractor e) {
        e.getState().getRelationRefsToProcess().stream().forEach(relationRef -> {
            RelationSchema currentRelationSchema = e.getState().getRelationSchemaByRef(relationRef);
            StructType subSchema = e.subSchema(currentRelationSchema.getFullSchema(), e.getState().getRelationColumnsByRef(relationRef));
            subSchema = subSchema.merge(currentRelationSchema.getSchema());
            currentRelationSchema.setSchema(subSchema);
        });
    }
}
