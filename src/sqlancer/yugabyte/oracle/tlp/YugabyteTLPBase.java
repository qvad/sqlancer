package sqlancer.yugabyte.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TernaryLogicPartitioningOracleBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema;
import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTables;
import sqlancer.yugabyte.ast.YugabyteColumnValue;
import sqlancer.yugabyte.ast.YugabyteConstant;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.ast.YugabyteJoin;
import sqlancer.yugabyte.ast.YugabyteSelect;
import sqlancer.yugabyte.ast.YugabyteSelect.ForClause;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteSubquery;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteExpressionGenerator;
import sqlancer.yugabyte.oracle.YugabyteNoRECOracle;

public class YugabyteTLPBase extends TernaryLogicPartitioningOracleBase<YugabyteExpression, YugabyteGlobalState>
        implements TestOracle {

    protected YugabyteSchema s;
    protected YugabyteTables targetTables;
    protected YugabyteExpressionGenerator gen;
    protected YugabyteSelect select;

    public YugabyteTLPBase(YugabyteGlobalState state) {
        super(state);
        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
    }

    public static YugabyteSubquery createSubquery(YugabyteGlobalState globalState, String name, YugabyteTables tables) {
        List<YugabyteExpression> columns = new ArrayList<>();
        YugabyteExpressionGenerator gen = new YugabyteExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            columns.add(gen.generateExpression(0));
        }
        YugabyteSelect select = new YugabyteSelect();
        select.setFromList(tables.getTables().stream().map(t -> new YugabyteFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, YugabyteDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        if (Randomly.getBoolean()) {
            select.setLimitClause(YugabyteConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            if (Randomly.getBoolean()) {
                select.setOffsetClause(
                        YugabyteConstant.createIntConstant(Randomly.getPositiveOrZeroNonCachedInteger()));
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setForClause(ForClause.getRandom());
        }
        return new YugabyteSubquery(select, name);
    }

    @Override
    public void check() throws SQLException {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        List<YugabyteTable> tables = targetTables.getTables();
        List<YugabyteJoin> joins = getJoinStatements(state, targetTables.getColumns(), tables);
        generateSelectBase(tables, joins);
    }

    protected List<YugabyteJoin> getJoinStatements(YugabyteGlobalState globalState, List<YugabyteColumn> columns,
            List<YugabyteTable> tables) {
        return YugabyteNoRECOracle.getJoinStatements(state, columns, tables);
        // TODO joins
    }

    protected void generateSelectBase(List<YugabyteTable> tables, List<YugabyteJoin> joins) {
        List<YugabyteExpression> tableList = tables.stream().map(t -> new YugabyteFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList());
        gen = new YugabyteExpressionGenerator(state).setColumns(targetTables.getColumns());
        initializeTernaryPredicateVariants();
        select = new YugabyteSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setFromList(tableList);
        select.setWhereClause(null);
        select.setJoinClauses(joins);
        if (Randomly.getBoolean()) {
            select.setForClause(ForClause.getRandom());
        }
    }

    List<YugabyteExpression> generateFetchColumns() {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new YugabyteColumnValue(YugabyteColumn.createDummy("*"), null));
        }
        List<YugabyteExpression> fetchColumns = new ArrayList<>();
        List<YugabyteColumn> targetColumns = Randomly.nonEmptySubset(targetTables.getColumns());
        for (YugabyteColumn c : targetColumns) {
            fetchColumns.add(new YugabyteColumnValue(c, null));
        }
        return fetchColumns;
    }

    @Override
    protected ExpressionGenerator<YugabyteExpression> getGen() {
        return gen;
    }

}
