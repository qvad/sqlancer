package sqlancer.yugabyte.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.YugabyteCompoundDataType;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema;
import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTables;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteCastOperation;
import sqlancer.yugabyte.ast.YugabyteColumnValue;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.ast.YugabyteJoin;
import sqlancer.yugabyte.ast.YugabyteJoin.YugabyteJoinType;
import sqlancer.yugabyte.ast.YugabytePostfixText;
import sqlancer.yugabyte.ast.YugabyteSelect;
import sqlancer.yugabyte.ast.YugabyteSelect.SelectType;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteSubquery;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteExpressionGenerator;
import sqlancer.yugabyte.oracle.tlp.YugabyteTLPBase;

public class YugabyteNoRECOracle extends NoRECBase<YugabyteGlobalState> implements TestOracle {

    private final YugabyteSchema s;

    public YugabyteNoRECOracle(YugabyteGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
    }

    public static List<YugabyteJoin> getJoinStatements(YugabyteGlobalState globalState, List<YugabyteColumn> columns,
            List<YugabyteTable> tables) {
        List<YugabyteJoin> joinStatements = new ArrayList<>();
        YugabyteExpressionGenerator gen = new YugabyteExpressionGenerator(globalState).setColumns(columns);
        for (int i = 1; i < tables.size(); i++) {
            YugabyteExpression joinClause = gen.generateExpression(YugabyteDataType.BOOLEAN);
            YugabyteTable table = Randomly.fromList(tables);
            tables.remove(table);
            YugabyteJoinType options = YugabyteJoinType.getRandom();
            YugabyteJoin j = new YugabyteJoin(new YugabyteFromTable(table, Randomly.getBoolean()), joinClause, options);
            joinStatements.add(j);
        }
        // JOIN subqueries
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            YugabyteTables subqueryTables = globalState.getSchema().getRandomTableNonEmptyTables();
            YugabyteSubquery subquery = YugabyteTLPBase.createSubquery(globalState, String.format("sub%d", i),
                    subqueryTables);
            YugabyteExpression joinClause = gen.generateExpression(YugabyteDataType.BOOLEAN);
            YugabyteJoinType options = YugabyteJoinType.getRandom();
            YugabyteJoin j = new YugabyteJoin(subquery, joinClause, options);
            joinStatements.add(j);
        }
        return joinStatements;
    }

    @Override
    public void check() throws SQLException {
        YugabyteTables randomTables = s.getRandomTableNonEmptyTables();
        List<YugabyteColumn> columns = randomTables.getColumns();
        YugabyteExpression randomWhereCondition = getRandomWhereCondition(columns);
        List<YugabyteTable> tables = randomTables.getTables();

        List<YugabyteJoin> joinStatements = getJoinStatements(state, columns, tables);
        List<YugabyteExpression> fromTables = tables.stream().map(t -> new YugabyteFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        int secondCount = getUnoptimizedQueryCount(fromTables, randomWhereCondition, joinStatements);
        int firstCount = getOptimizedQueryCount(fromTables, columns, randomWhereCondition, joinStatements);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            String queryFormatString = "-- %s;\n-- count: %d";
            String firstQueryStringWithCount = String.format(queryFormatString, optimizedQueryString, firstCount);
            String secondQueryStringWithCount = String.format(queryFormatString, unoptimizedQueryString, secondCount);
            state.getState().getLocalState()
                    .log(String.format("%s\n%s", firstQueryStringWithCount, secondQueryStringWithCount));
            String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", firstCount, secondCount,
                    firstQueryStringWithCount, secondQueryStringWithCount);
            throw new AssertionError(assertionMessage);
        }
    }

    private YugabyteExpression getRandomWhereCondition(List<YugabyteColumn> columns) {
        return new YugabyteExpressionGenerator(state).setColumns(columns).generateExpression(YugabyteDataType.BOOLEAN);
    }

    private int getUnoptimizedQueryCount(List<YugabyteExpression> fromTables, YugabyteExpression randomWhereCondition,
            List<YugabyteJoin> joinStatements) throws SQLException {
        YugabyteSelect select = new YugabyteSelect();
        YugabyteCastOperation isTrue = new YugabyteCastOperation(randomWhereCondition,
                YugabyteCompoundDataType.create(YugabyteDataType.INT));
        YugabytePostfixText asText = new YugabytePostfixText(isTrue, " as count", null, YugabyteDataType.INT);
        select.setFetchColumns(Collections.singletonList(asText));
        select.setFromList(fromTables);
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + YugabyteVisitor.asString(select) + ") as res";
        if (options.logEachSelect()) {
            logger.writeCurrent(unoptimizedQueryString);
        }
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGet(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getOptimizedQueryCount(List<YugabyteExpression> randomTables, List<YugabyteColumn> columns,
            YugabyteExpression randomWhereCondition, List<YugabyteJoin> joinStatements) throws SQLException {
        YugabyteSelect select = new YugabyteSelect();
        YugabyteColumnValue allColumns = new YugabyteColumnValue(Randomly.fromList(columns), null);
        select.setFetchColumns(Arrays.asList(allColumns));
        select.setFromList(randomTables);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new YugabyteExpressionGenerator(state).setColumns(columns).generateOrderBy());
        }
        select.setSelectType(SelectType.ALL);
        select.setJoinClauses(joinStatements);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = YugabyteVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
