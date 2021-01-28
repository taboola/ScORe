package com.taboola.spark.sql.scisors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;


public class SQLExpressionParser {
    private static Pattern sqlExpressionPattern = Pattern.compile("`?(?<aggregateFunction>(\\w+\\()?)((?<tableAlias>\\w+)\\.)?(?<nestedPath>(`?\\w+`?(\\[[^\\[]+\\])*\\.)*)(?<leaf>`?\\w+`?(\\[[^\\[]+\\])*)(?<aggregateParams>[^\\)]*\\)?)`?(?<alias>( AS )?(?<aliasName>`?\\w+`?)?)");
    private static Pattern dotPattern = Pattern.compile("\\.");
    // brackets are used for collection accessors (arrays / maps), backticks are positioned arround column names
    private static Pattern bracketsOrBacktickPattern = Pattern.compile("\\[.*\\]|`");
    private static Pattern windowPattern = Pattern.compile(".*(\\) OVER \\(.*)");
    /*
    // sql is of the following pattern:
    //  tablaAlias.`parent`.`child`  -- when using alias
    // `parent`.`child`             -- when not using alias
    // `array[0]`                   -- when querying array column
    // `map[key]`                   -- when querying map column
    */
    public static SQLParsedExpression parseSqlExpression(String sql) {
        final Matcher matcher = sqlExpressionPattern.matcher(sql);
        if (!matcher.matches()) {
            throw new RuntimeException(String.format("Failed to parse sql statement [%s]", sql));
        }


        final String nestedPath = matcher.group("nestedPath");
        final String[] split = nestedPath.length() > 0 ? dotPattern.split(nestedPath) : new String[0];
        String alias = matcher.group("tableAlias");

        List<String> columns = new ArrayList<>(split.length);

        for (String expressionPart : split) {
            columns.add(bracketsOrBacktickPattern.matcher(expressionPart).replaceAll(StringUtils.EMPTY));
        }

        columns.add(bracketsOrBacktickPattern.matcher(matcher.group("leaf")).replaceAll(StringUtils.EMPTY));

        return new SQLParsedExpression(alias, columns);
    }

    public static boolean windowReferenceAttribute(String sql) {
        return windowPattern.matcher(sql).matches();
    }
}
