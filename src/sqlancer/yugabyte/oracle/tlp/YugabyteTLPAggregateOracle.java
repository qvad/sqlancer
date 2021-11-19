package sqlancer.yugabyte.oracle.tlp;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.postgresql.util.PSQLException;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteAggregate;
import sqlancer.yugabyte.ast.YugabyteAggregate.YugabyteAggregateFunction;
import sqlancer.yugabyte.ast.YugabyteAlias;
import sqlancer.yugabyte.ast.YugabyteExpression;
import sqlancer.yugabyte.ast.YugabyteJoin;
import sqlancer.yugabyte.ast.YugabytePostfixOperation;
import sqlancer.yugabyte.ast.YugabytePostfixOperation.PostfixOperator;
import sqlancer.yugabyte.ast.YugabytePrefixOperation;
import sqlancer.yugabyte.ast.YugabytePrefixOperation.PrefixOperator;
import sqlancer.yugabyte.ast.YugabyteSelect;
import sqlancer.yugabyte.gen.YugabyteCommon;

public class YugabyteTLPAggregateOracle extends YugabyteTLPBase implements TestOracle {

    private String firstResult;
    private String secondResult;
    private String originalQuery;
    private String metamorphicQuery;

    public YugabyteTLPAggregateOracle(YugabyteGlobalState state) {
        super(state);
        YugabyteCommon.addGroupingErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        super.check();
        aggregateCheck();
    }

    protected void aggregateCheck() throws SQLException {
        YugabyteAggregateFunction aggregateFunction = Randomly.fromOptions(YugabyteAggregateFunction.MAX,
                YugabyteAggregateFunction.MIN, YugabyteAggregateFunction.SUM, YugabyteAggregateFunction.BIT_AND,
                YugabyteAggregateFunction.BIT_OR, YugabyteAggregateFunction.BOOL_AND, YugabyteAggregateFunction.BOOL_OR,
                YugabyteAggregateFunction.COUNT);
        YugabyteAggregate aggregate = gen.generateArgsForAggregate(aggregateFunction.getRandomReturnType(),
                aggregateFunction);
        List<YugabyteExpression> fetchColumns = new ArrayList<>();
        fetchColumns.add(aggregate);
        while (Randomly.getBooleanWithRatherLowProbability()) {
            fetchColumns.add(gen.generateAggregate());
        }
        select.setFetchColumns(Arrays.asList(aggregate));
        if (Randomly.getBooleanWithRatherLowProbability()) {
            select.setOrderByExpressions(gen.generateOrderBy());
        }
        originalQuery = YugabyteVisitor.asString(select);
        firstResult = getAggregateResult(originalQuery);
        metamorphicQuery = createMetamorphicUnionQuery(select, aggregate, select.getFromList());
        secondResult = getAggregateResult(metamorphicQuery);

        String queryFormatString = "-- %s;\n-- result: %s";
        String firstQueryString = String.format(queryFormatString, originalQuery, firstResult);
        String secondQueryString = String.format(queryFormatString, metamorphicQuery, secondResult);
        state.getState().getLocalState().log(String.format("%s\n%s", firstQueryString, secondQueryString));
        if (firstResult == null && secondResult != null || firstResult != null && secondResult == null
                || firstResult != null && !firstResult.contentEquals(secondResult)
                        && !ComparatorHelper.isEqualDouble(firstResult, secondResult)) {
            if (secondResult != null && secondResult.contains("Inf")) {
                throw new IgnoreMeException(); // FIXME: average computation
            }
            String assertionMessage = String.format("the results mismatch!\n%s\n%s", firstQueryString,
                    secondQueryString);
            throw new AssertionError(assertionMessage);
        }
    }

    private String createMetamorphicUnionQuery(YugabyteSelect select, YugabyteAggregate aggregate,
            List<YugabyteExpression> from) {
        String metamorphicQuery;
        YugabyteExpression whereClause = gen.generateExpression(YugabyteDataType.BOOLEAN);
        YugabyteExpression negatedClause = new YugabytePrefixOperation(whereClause, PrefixOperator.NOT);
        YugabyteExpression notNullClause = new YugabytePostfixOperation(whereClause, PostfixOperator.IS_NULL);
        List<YugabyteExpression> mappedAggregate = mapped(aggregate);
        YugabyteSelect leftSelect = getSelect(mappedAggregate, from, whereClause, select.getJoinClauses());
        YugabyteSelect middleSelect = getSelect(mappedAggregate, from, negatedClause, select.getJoinClauses());
        YugabyteSelect rightSelect = getSelect(mappedAggregate, from, notNullClause, select.getJoinClauses());
        metamorphicQuery = "SELECT " + getOuterAggregateFunction(aggregate) + " FROM (";
        metamorphicQuery += YugabyteVisitor.asString(leftSelect) + " UNION ALL "
                + YugabyteVisitor.asString(middleSelect) + " UNION ALL " + YugabyteVisitor.asString(rightSelect);
        metamorphicQuery += ") as asdf";
        return metamorphicQuery;
    }

    private String getAggregateResult(String queryString) throws SQLException {
        // log TLP Aggregate SELECT queries on the current log file
        if (state.getOptions().logEachSelect()) {
            // TODO: refactor me
            state.getLogger().writeCurrent(queryString);
            try {
                state.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        String resultString;
        SQLQueryAdapter q = new SQLQueryAdapter(queryString, errors);
        try (SQLancerResultSet result = q.executeAndGet(state)) {
            if (result == null) {
                throw new IgnoreMeException();
            }
            if (!result.next()) {
                resultString = null;
            } else {
                resultString = result.getString(1);
            }
        } catch (PSQLException e) {
            throw new AssertionError(queryString, e);
        }
        return resultString;
    }

    private List<YugabyteExpression> mapped(YugabyteAggregate aggregate) {
        switch (aggregate.getFunction()) {
        case SUM:
        case COUNT:
        case BIT_AND:
        case BIT_OR:
        case BOOL_AND:
        case BOOL_OR:
        case MAX:
        case MIN:
            return aliasArgs(Arrays.asList(aggregate));
        // case AVG:
        //// List<YugabyteExpression> arg = Arrays.asList(new
        // YugabyteCast(aggregate.getExpr().get(0),
        // YugabyteDataType.DECIMAL.get()));
        // YugabyteAggregate sum = new YugabyteAggregate(YugabyteAggregateFunction.SUM,
        // aggregate.getExpr());
        // YugabyteCast count = new YugabyteCast(
        // new YugabyteAggregate(YugabyteAggregateFunction.COUNT, aggregate.getExpr()),
        // YugabyteDataType.DECIMAL.get());
        //// YugabyteBinaryArithmeticOperation avg = new
        // YugabyteBinaryArithmeticOperation(sum, count,
        // YugabyteBinaryArithmeticOperator.DIV);
        // return aliasArgs(Arrays.asList(sum, count));
        default:
            throw new AssertionError(aggregate.getFunction());
        }
    }

    private List<YugabyteExpression> aliasArgs(List<YugabyteExpression> originalAggregateArgs) {
        List<YugabyteExpression> args = new ArrayList<>();
        int i = 0;
        for (YugabyteExpression expr : originalAggregateArgs) {
            args.add(new YugabyteAlias(expr, "agg" + i++));
        }
        return args;
    }

    private String getOuterAggregateFunction(YugabyteAggregate aggregate) {
        switch (aggregate.getFunction()) {
        // case AVG:
        // return "SUM(agg0::DECIMAL)/SUM(agg1)::DECIMAL";
        case COUNT:
            return YugabyteAggregateFunction.SUM + "(agg0)";
        default:
            return aggregate.getFunction().toString() + "(agg0)";
        }
    }

    private YugabyteSelect getSelect(List<YugabyteExpression> aggregates, List<YugabyteExpression> from,
            YugabyteExpression whereClause, List<YugabyteJoin> joinList) {
        YugabyteSelect leftSelect = new YugabyteSelect();
        leftSelect.setFetchColumns(aggregates);
        leftSelect.setFromList(from);
        leftSelect.setWhereClause(whereClause);
        leftSelect.setJoinClauses(joinList);
        if (Randomly.getBooleanWithSmallProbability()) {
            leftSelect.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        return leftSelect;
    }

}
