package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTables;
import sqlancer.yugabyte.ast.YugabyteSelect.ForClause;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.ast.YugabyteSelect.SelectType;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.ast.YugabyteConstant;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.ast.YugabyteSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class YugabyteRandomQueryGenerator {

    private YugabyteRandomQueryGenerator() {
    }

    public static YugabyteSelect createRandomQuery(int nrColumns, YugabyteGlobalState globalState) {
        List<YugabyteExpression> columns = new ArrayList<>();
        YugabyteTables tables = globalState.getSchema().getRandomTableNonEmptyTables();
        YugabyteExpressionGenerator gen = new YugabyteExpressionGenerator(globalState).setColumns(tables.getColumns());
        for (int i = 0; i < nrColumns; i++) {
            columns.add(gen.generateExpression(0));
        }
        YugabyteSelect select = new YugabyteSelect();
        select.setSelectType(SelectType.getRandom());
        if (select.getSelectOption() == SelectType.DISTINCT && Randomly.getBoolean()) {
            select.setDistinctOnClause(gen.generateExpression(0));
        }
        select.setFromList(tables.getTables().stream().map(t -> new YugabyteFromTable(t, Randomly.getBoolean()))
                .collect(Collectors.toList()));
        select.setFetchColumns(columns);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, YugabyteDataType.BOOLEAN));
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
            if (Randomly.getBoolean()) {
                select.setHavingClause(gen.generateHavingClause());
            }
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
        return select;
    }

}
