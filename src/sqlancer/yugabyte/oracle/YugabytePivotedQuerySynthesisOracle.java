package sqlancer.yugabyte.oracle;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteRowValue;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTables;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteColumnValue;
import sqlancer.yugabyte.ast.YugabyteConstant;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.ast.YugabytePostfixOperation;
import sqlancer.yugabyte.ast.YugabytePostfixOperation.PostfixOperator;
import sqlancer.yugabyte.ast.YugabyteSelect;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteExpressionGenerator;

public class YugabytePivotedQuerySynthesisOracle
        extends PivotedQuerySynthesisBase<YugabyteGlobalState, YugabyteRowValue, YugabyteExpression, SQLConnection> {

    private List<YugabyteColumn> fetchColumns;

    public YugabytePivotedQuerySynthesisOracle(YugabyteGlobalState globalState) throws SQLException {
        super(globalState);
        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
    }

    /*
     * Prevent name collisions by aliasing the column.
     */
    private YugabyteColumn getFetchValueAliasedColumn(YugabyteColumn c) {
        YugabyteColumn aliasedColumn = new YugabyteColumn(c.getName() + " AS " + c.getTable().getName() + c.getName(),
                c.getType());
        aliasedColumn.setTable(c.getTable());
        return aliasedColumn;
    }

    private List<YugabyteExpression> generateGroupByClause(List<YugabyteColumn> columns, YugabyteRowValue rw) {
        if (Randomly.getBoolean()) {
            return columns.stream().map(c -> YugabyteColumnValue.create(c, rw.getValues().get(c)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private YugabyteConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return YugabyteConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private YugabyteExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return YugabyteConstant.createIntConstant(0);
        } else {
            return null;
        }
    }

    private YugabyteExpression generateRectifiedExpression(List<YugabyteColumn> columns, YugabyteRowValue rw) {
        YugabyteExpression expr = new YugabyteExpressionGenerator(globalState).setColumns(columns).setRowValue(rw)
                .generateExpressionWithExpectedResult(YugabyteDataType.BOOLEAN);
        YugabyteExpression result;
        if (expr.getExpectedValue().isNull()) {
            result = YugabytePostfixOperation.create(expr, PostfixOperator.IS_NULL);
        } else {
            result = YugabytePostfixOperation.create(expr,
                    expr.getExpectedValue().cast(YugabyteDataType.BOOLEAN).asBoolean() ? PostfixOperator.IS_TRUE
                            : PostfixOperator.IS_FALSE);
        }
        rectifiedPredicates.add(result);
        return result;
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> query) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
        sb.append(query.getUnterminatedQueryString());
        sb.append(") as result WHERE ");
        int i = 0;
        for (YugabyteColumn c : fetchColumns) {
            if (i++ != 0) {
                sb.append(" AND ");
            }
            sb.append("result.");
            sb.append(c.getTable().getName());
            sb.append(c.getName());
            if (pivotRow.getValues().get(c).isNull()) {
                sb.append(" IS NULL");
            } else {
                sb.append(" = ");
                sb.append(pivotRow.getValues().get(c).getTextRepresentation());
            }
        }
        String resultingQueryString = sb.toString();
        return new SQLQueryAdapter(resultingQueryString, errors);
    }

    @Override
    public SQLQueryAdapter getRectifiedQuery() throws SQLException {
        YugabyteTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();

        YugabyteSelect selectStatement = new YugabyteSelect();
        selectStatement.setSelectType(Randomly.fromOptions(YugabyteSelect.SelectType.values()));
        List<YugabyteColumn> columns = randomFromTables.getColumns();
        pivotRow = randomFromTables.getRandomRowValue(globalState.getConnection());

        fetchColumns = columns;
        selectStatement.setFromList(randomFromTables.getTables().stream().map(t -> new YugabyteFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(fetchColumns.stream()
                .map(c -> new YugabyteColumnValue(getFetchValueAliasedColumn(c), pivotRow.getValues().get(c)))
                .collect(Collectors.toList()));
        YugabyteExpression whereClause = generateRectifiedExpression(columns, pivotRow);
        selectStatement.setWhereClause(whereClause);
        List<YugabyteExpression> groupByClause = generateGroupByClause(columns, pivotRow);
        selectStatement.setGroupByExpressions(groupByClause);
        YugabyteExpression limitClause = generateLimit();
        selectStatement.setLimitClause(limitClause);
        if (limitClause != null) {
            YugabyteExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        List<YugabyteExpression> orderBy = new YugabyteExpressionGenerator(globalState).setColumns(columns)
                .generateOrderBy();
        selectStatement.setOrderByExpressions(orderBy);
        return new SQLQueryAdapter(YugabyteVisitor.asString(selectStatement));
    }

    @Override
    protected String getExpectedValues(YugabyteExpression expr) {
        return YugabyteVisitor.asExpectedValues(expr);
    }

}
