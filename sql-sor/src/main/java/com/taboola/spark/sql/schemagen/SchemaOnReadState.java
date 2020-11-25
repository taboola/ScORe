package com.taboola.spark.sql.schemagen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.Generate;
import org.apache.spark.sql.catalyst.trees.TreeNode;
import org.apache.spark.sql.execution.datasources.HadoopFsRelation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;


import scala.collection.JavaConversions;

class SchemaOnReadState {
    private static Logger logger = LogManager.getLogger(SchemaOnReadState.class);
    private Map<Integer, Map<String,SchemaElement>> relationColumns = new ConcurrentHashMap<>();

    private Map<String, Integer> aliasSchemasRelations = new HashMap<>();
    private Map<Integer, RelationSchema> relationRefSchemas = new HashMap<>();

    private Set<Integer> relationRefsToProcess = new HashSet<>();
    private Integer currentHadoopFsRelation;
    private SparkSession session;
    private Set<TreeNode> upperMostSelectionNodes;
    private SchemaElement potentialAliasedNode;

    private boolean requireFullSchema = false;


    private final CurrentNodeState currentNodeState;
    private String generateNodeAlias;

    SchemaOnReadState(SparkSession session, Set<TreeNode> intermediateSelectionNodes) {
        this.session = session;
        this.upperMostSelectionNodes = intermediateSelectionNodes;
        this.currentNodeState = new CurrentNodeState();
    }

    public CurrentNodeState getCurrentNodeState() {
        return currentNodeState;
    }

    public Map<Integer, Map<String, SchemaElement>> getRelationColumns() {
        return relationColumns;
    }

    public void resetRelation() {
        this.currentHadoopFsRelation = null;
    }

    public Map<Integer, RelationSchema> getRelationRefSchemas() {
        return relationRefSchemas;
    }


    public StructType getRelationSchema(String[] inputFiles) {
        List<Path> paths = convertInputFilesToSortedPaths(inputFiles);
        if (paths == null) return null;
        final RelationSchema relationSchema = relationRefSchemas.get(paths.hashCode());
        logger.info("Relation schema {}, paths hash {}", relationSchema, paths.hashCode());
        return relationSchema == null ? null : relationSchema.getSchema();
    }

    private List<Path> convertInputFilesToSortedPaths(String[] inputFiles) {
        List<Path> paths = new ArrayList<>(inputFiles.length);
        try {
            for (String inputFile : inputFiles) {
                Path inputPath = new Path(inputFile);
                FileSystem fs = inputPath.getFileSystem(session.sessionState().newHadoopConf());
                Path qualifiedPath = inputPath.makeQualified(fs.getUri(), fs.getWorkingDirectory());
                paths.add(qualifiedPath);
            }
        } catch (IOException e) {
            logger.error(e);
            return null;
        }
        paths.sort(Comparator.comparing(Path::toString));
        return paths;
    }

    public StructType getAliasSchema(String alias) {
        final Integer relationRef = aliasSchemasRelations.get(alias);
        if (relationRef != null) {
            return relationRefSchemas.get(relationRef).getSchema();
        }

        return null;
    }

    public void setHadoopFsRelation(HadoopFsRelation hadoopFsRelation) {

        final int relationRef = getHadoopFsRelationRef(hadoopFsRelation);

        if (!relationRefsToProcess.contains(relationRef)) {
                // avoid setting currentHadoopFsRelation when encountering the same relation twice, before markRelationRefsProcessed was invoked on relation
                // Refer to SchemaOnReadGeneratorTest#test_filterPreAliasWithImplicitJoin
                currentHadoopFsRelation = relationRef;
                relationRefsToProcess.add(currentHadoopFsRelation);
                relationRefSchemas.computeIfAbsent(currentHadoopFsRelation,
                        ref -> new RelationSchema(hadoopFsRelation.schema()));
            }


    }

    private int getHadoopFsRelationRef(HadoopFsRelation hadoopFsRelation) {
        final String[] inputFiles = hadoopFsRelation.location().inputFiles();
		final List<Path> pathList = convertInputFilesToSortedPaths(inputFiles);
        return pathList.hashCode();
    }

    public void addAliasForRelationRef(String aliasName) {
        if (currentHadoopFsRelation != null) {
            aliasSchemasRelations.put(aliasName, currentHadoopFsRelation);
        }
    }

    public Integer resolveRelationRef(SQLParsedExpression sqlParsedExpression) {
        String alias = sqlParsedExpression.getAlias();
        if (alias == null || alias.equals(StringUtils.EMPTY) || alias.equals("as")) { // "as" is applicable when encountering alias column over exploded repeated field
            return currentHadoopFsRelation;
        } else {
            return aliasSchemasRelations.containsKey(alias) ? aliasSchemasRelations.get(alias) : currentHadoopFsRelation;
        }
    }

    public Set<Integer> getRelationRefsToProcess() {
        return relationRefsToProcess;
    }

    public RelationSchema getRelationSchemaByRef(Integer relationRef) {
        return relationRefSchemas.get(relationRef);
    }

    public Map<String, SchemaElement> getRelationColumnsByRef(Integer relationRef) {
        return relationColumns.get(relationRef);
    }

    public void addToRelationColumns(String sql, DataType dataType) {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression(sql);
        Integer relationRef = resolveRelationRef(sqlParsedExpression);
        if (relationRef != null) {
            final Map<String, SchemaElement> relationColumns = getRelationColumns().computeIfAbsent(relationRef, s -> new ConcurrentHashMap<>());
            final List<String> columnPath = sqlParsedExpression.getColumnPath();
            if (columnPath.size() > 1) {
                potentialAliasedNode = relationColumns.computeIfAbsent(columnPath.get(0).toLowerCase(), s -> SchemaElementImpl.createNode(s));

                for (int idx = 1; idx < columnPath.size() - 1; idx++) {
                    final String colName = columnPath.get(idx);
                    potentialAliasedNode = potentialAliasedNode.getChildElements().computeIfAbsent(colName.toLowerCase(),
                            s -> SchemaElementImpl.createNode(s));
                }

                if (SchemaOnReadUtils.isStruct(dataType) || SchemaOnReadUtils.isArray(dataType)) {
                    this.potentialAliasedNode = potentialAliasedNode.getChildElements().computeIfAbsent(columnPath.get(columnPath.size() - 1).toLowerCase(),
                            s -> SchemaElementImpl.createNode(s));

                } else {
                    potentialAliasedNode = potentialAliasedNode.getChildElements().computeIfAbsent(columnPath.get(columnPath.size() - 1).toLowerCase(),
                            s -> SchemaElementImpl.createLeaf(s));
                }

            } else {
                if (SchemaOnReadUtils.isComplex(dataType)) {
                    this.potentialAliasedNode = relationColumns.computeIfAbsent(columnPath.get(0).toLowerCase(), s -> SchemaElementImpl.createNode(s));
                } else {
                    potentialAliasedNode = relationColumns.computeIfAbsent(columnPath.get(0).toLowerCase(), s -> SchemaElementImpl.createLeaf(s));
                }
            }

            markFullSchemaRequired(potentialAliasedNode);
        }
    }

    private void markFullSchemaRequired(SchemaElement element) {
        element.setRequiredInSchema(currentNodeState.mandatory || (currentNodeState.conditional && requireFullSchema));
    }

    public void setAlias(final String aliasName) {
        if (currentHadoopFsRelation != null) {
            final Map<String, SchemaElement> relationColumns = getRelationColumns().computeIfAbsent(currentHadoopFsRelation, s -> new HashMap<>());
            relationColumns.computeIfAbsent(aliasName.toLowerCase(), s -> potentialAliasedNode.cloneAlias(aliasName));
        }
    }

    public void enterNode(TreeNode node) {
        if (SchemaOnReadUtils.containsColumnsForQuery(node)) {
            if (node instanceof Generate) {
                Generate generate = (Generate) node;
                final List<Attribute> attributes = JavaConversions.seqAsJavaList(generate.generatorOutput());
                this.generateNodeAlias = attributes.get(attributes.size() - 1).name();
            }
            this.requireFullSchema = upperMostSelectionNodes.contains(node);
            this.currentNodeState.columnsWithMandatoryFullSchema = SchemaOnReadUtils.getMandatorySchemaColumns(node);
            this.currentNodeState.columnsWithConditionalFullSchema = SchemaOnReadUtils.getConditionalSchemaColumns(node);
        } else {
            this.currentNodeState.mandatory = currentNodeState.columnsWithMandatoryFullSchema.contains(node);
            this.currentNodeState.conditional =
                    currentNodeState.columnsWithConditionalFullSchema.contains(node);
        }
    }

    public void exitNode(TreeNode node) {
        this.currentNodeState.conditional = false;
        this.currentNodeState.mandatory = false;
        if (SchemaOnReadUtils.containsColumnsForQuery(node)) {
            if (node instanceof Generate) {
                final Map<String, SchemaElement> elementMap = this.getRelationColumnsByRef(currentHadoopFsRelation);
                elementMap.put(generateNodeAlias.toLowerCase(), potentialAliasedNode.cloneAlias(generateNodeAlias.toLowerCase()));

                this.generateNodeAlias = null;
            }
            this.requireFullSchema = true;
            this.currentNodeState.resetColumnsInQuery();
        }
    }

    static class CurrentNodeState {
        public boolean mandatory = false;
        boolean conditional = false;
        Set<TreeNode> columnsWithMandatoryFullSchema;
        Set<TreeNode> columnsWithConditionalFullSchema;

        CurrentNodeState() {
            resetColumnsInQuery();
        }

        private void resetColumnsInQuery() {
            columnsWithMandatoryFullSchema = Collections.emptySet();
            columnsWithConditionalFullSchema = Collections.emptySet();
        }

        public Set<TreeNode> getColumnsWithMandatoryFullSchema() {
            return columnsWithMandatoryFullSchema;
        }

        public Set<TreeNode> getColumnsWithConditionalFullSchema() {
            return columnsWithConditionalFullSchema;
        }
    }
}
