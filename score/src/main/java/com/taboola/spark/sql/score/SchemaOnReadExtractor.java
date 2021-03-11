package com.taboola.spark.sql.score;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
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
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

class SchemaOnReadExtractor extends AbstractFunction1<TreeNode, TreeNode> {

    private SchemaOnReadState state;

    SchemaOnReadExtractor(SchemaOnReadState state) {
        this.state = state;
    }

    @Override
    public TreeNode apply(TreeNode treeNode) {
        switchType(treeNode, this,
                caze(LogicalRelation.class,
                        TreeNodeProcessingRegistry.processLogicalRelation),
                caze(LocalRelation.class,
                        TreeNodeProcessingRegistry.processLocalRelation),
                caze(Project.class,
                        TreeNodeProcessingRegistry.processProjection),
                caze(Aggregate.class,
                        TreeNodeProcessingRegistry.processAggregate),
                caze(AttributeReference.class,
                        TreeNodeProcessingRegistry.processAttributeReference),
                caze(GetStructField.class,
                        TreeNodeProcessingRegistry.processGetStructField),
                caze(SubqueryAlias.class,
                        TreeNodeProcessingRegistry.processSubqueryAlias),
                caze(Join.class,
                        TreeNodeProcessingRegistry.processJoin),
                caze(Filter.class,
                        TreeNodeProcessingRegistry.processFilter),
                caze(GetArrayStructFields.class,
                        TreeNodeProcessingRegistry.processGetArrayStructFields),
                caze(Generate.class,
                        TreeNodeProcessingRegistry.processGenerate),
                caze(Window.class,
                        TreeNodeProcessingRegistry.processWindow),
                caze(Alias.class,
                        TreeNodeProcessingRegistry.processAlias),
                caze(WindowExpression.class,
                        TreeNodeProcessingRegistry.processWindowExpression),
                caze(Sort.class,
                        TreeNodeProcessingRegistry.processSort)
        );
        return null;
    }

    SchemaOnReadState getState() {
        return state;
    }


    private static DataType subSchema(DataType dataType, Map<String, SchemaElement> desiredColumns) {
        if (SchemaOnReadUtils.isStruct(dataType)) {
            return subSchema((StructType)dataType, desiredColumns);
        } else if (SchemaOnReadUtils.isArray(dataType)) {
            return subSchema((ArrayType)dataType, desiredColumns);
        } else if (SchemaOnReadUtils.isMap(dataType)) {
            return subSchema((MapType)dataType, desiredColumns);
        } else {
            return dataType;
        }
    }

    private static DataType subSchema(MapType mapType, Map<String, SchemaElement> desiredColumns) {
        DataType keyType = subSchema(mapType.keyType(), desiredColumns);
        DataType valueType = subSchema(mapType.valueType(), desiredColumns);
        return DataTypes.createMapType(keyType, valueType);

    }

    private static DataType subSchema(ArrayType arrayType, Map<String, SchemaElement> desiredColumns) {
        DataType elementType = subSchema(arrayType.elementType(), desiredColumns);
        return DataTypes.createArrayType(elementType);

    }

    static StructType subSchema(StructType fullSchema, Map<String, SchemaElement> desiredColumns) {
        if (MapUtils.isEmpty(desiredColumns)) {
            return DataTypes.createStructType(Collections.emptyList());
        }

        final StructType newSchema = DataTypes.createStructType(JavaConversions.seqAsJavaList(fullSchema.toSeq()).stream()
                .filter(sf ->  desiredColumns.containsKey(sf.name().toLowerCase()))
                .map(sf -> {
                    final String name = sf.name();
                    final SchemaElement schemaElement = desiredColumns.get(name.toLowerCase());
                    schemaElement.setFullSchema(sf.dataType());
                    final DataType innerType = subSchema(sf.dataType(), schemaElement.getChildElements());
                    schemaElement.setPartialSchema(innerType);
                    final StructField structField = DataTypes.createStructField(name,
                            schemaElement.getPartialSchema(),
                            sf.nullable(),
                            sf.metadata());



                    return structField;
                })
                .collect(Collectors.toList()));


        return newSchema;
    }


    private static void switchType(TreeNode o, SchemaOnReadExtractor extractor, BiConsumer... consumers) {
        for (BiConsumer consumer : consumers) {
            consumer.accept(extractor, o);
        }
    }

    private static <T> BiConsumer<SchemaOnReadExtractor, T> caze(final Class<T> cls, BiConsumer<SchemaOnReadExtractor, T> consumer) {
        return (e, obj) -> {
            if (cls.isInstance(obj)) {
                final T casted = cls.cast(obj);
                consumer.accept(e, casted);
            }
        };
    }

    void restoreDirectRelationSchemaOnRead() {
        state.getRelationRefSchemas().values().stream()
                .filter(rs -> rs.getSchema().isEmpty())
                .forEach(rs -> rs.setSchema(rs.getFullSchema()));
    }

}
