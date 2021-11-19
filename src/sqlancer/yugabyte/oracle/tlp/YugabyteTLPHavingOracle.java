package sqlancer.yugabyte.oracle.tlp;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.gen.YugabyteCommon;

public class YugabyteTLPHavingOracle extends YugabyteTLPBase {

    public YugabyteTLPHavingOracle(YugabyteGlobalState state) {
        super(state);
        YugabyteCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        havingCheck();
    }

    @Override
    List<YugabyteExpression> generateFetchColumns() {
        List<YugabyteExpression> expressions = gen.allowAggregates(true)
                .generateExpressions(Randomly.smallNumber() + 1);
        gen.allowAggregates(false);
        return expressions;
    }

    protected void havingCheck() throws SQLException {
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(YugabyteDataType.BOOLEAN));
        }
        select.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        select.setHavingClause(null);
        String originalQueryString = YugabyteVisitor.asString(select);
        List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);

        boolean orderBy = Randomly.getBoolean();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        select.setHavingClause(predicate);
        String firstQueryString = YugabyteVisitor.asString(select);
        select.setHavingClause(negatedPredicate);
        String secondQueryString = YugabyteVisitor.asString(select);
        select.setHavingClause(isNullPredicate);
        String thirdQueryString = YugabyteVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();
        List<String> secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                thirdQueryString, combinedString, !orderBy, state, errors);
        ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                state);
    }

    @Override
    protected YugabyteExpression generatePredicate() {
        return gen.generateHavingClause();
    }

}
