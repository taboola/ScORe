package com.taboola.spark.sql.scisors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

public class SQLExpressionParserTest {

    @Test
    public void testTableAliasWithColumns() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("table.`a`.`b`");
        assertEquals("table", sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a", "b"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testNoTableAliasWithColumns() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("`a`.`b`");
        assertNull(sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a", "b"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testTableAliasWithColumnsNoBackquots() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("table.a.b");
        assertEquals("table", sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a", "b"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testTableAliasWithLeaf() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("table.`a`");
        assertEquals("table", sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testNoTableAliasWithLeaf() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("`a`");
        assertNull(sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testTableAliasWithLeafNoBackquote() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("table.a");
        assertEquals("table", sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testNoTableAliasWithLeafNoBackquote() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("a");
        assertNull(sqlParsedExpression.alias);
        assertEquals(Arrays.asList("a"), sqlParsedExpression.columnPath);
    }

    @Test
    public void testCountOne() {
        SQLParsedExpression sqlParsedExpression = SQLExpressionParser.parseSqlExpression("`count(1)`");
        assertNull(sqlParsedExpression.alias);
        assertEquals(Arrays.asList("1"), sqlParsedExpression.columnPath);
    }

}
