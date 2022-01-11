package sqlancer.yugabyte.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteProvider;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteRandomQueryGenerator;

public class YugabyteFuzzer implements TestOracle {
    private final YugabyteGlobalState globalState;
    private final List<Query> testQueries;
    private final ExpectedErrors errors = new ExpectedErrors();

    public YugabyteFuzzer(YugabyteGlobalState globalState) {
        this.globalState = globalState;

        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
        YugabyteCommon.addGroupingErrors(errors);
        YugabyteCommon.addViewErrors(errors);

        // remove timeout error from scope
        errors.add("canceling statement due to statement timeout");

        // get config from -Dconfig.file="path/to/fuzzer.conf"
        testQueries = new ArrayList<>();
        try {
            Config config = ConfigFactory.load();
            ArrayList<Object> queriesList = (ArrayList<Object>) config.getList("queries").unwrapped();
            for (Object configValue : queriesList) {
                String type = ((String) ((Map) configValue).get("type")).toUpperCase(Locale.ROOT);
                Integer weight = (Integer) ((Map) configValue).get("weight");

                Query query = type.equalsIgnoreCase("SELECT") ? new SelectQuery()
                        : new ActionQuery(YugabyteProvider.Action.valueOf(type));

                for (int i = 0; i < weight; i++) {
                    testQueries.add(query);
                }
            }
        } catch (Exception e) {
            // do nothing
        } finally {
            if (testQueries.isEmpty()) {
                System.out.println("No configuration found. Using just random select statements");
                testQueries.add(new SelectQuery());
                testQueries.add(new ActionQuery(YugabyteProvider.Action.UPDATE));
                testQueries.add(new ActionQuery(YugabyteProvider.Action.DELETE));
                testQueries.add(new ActionQuery(YugabyteProvider.Action.INSERT));
            }
        }
    }

    @Override
    public void check() throws Exception {
        Query s = testQueries.get(globalState.getRandomly().getInteger(0, testQueries.size()));
        globalState.executeStatement(s.getQuery(globalState, errors));
        globalState.getManager().incrementSelectQueryCount();
    }

    private static class Query {
        public SQLQueryAdapter getQuery(YugabyteGlobalState state, ExpectedErrors errors) throws Exception {
            throw new IllegalAccessException("Should be implemented");
        };
    }

    private static class ActionQuery extends Query {
        private final YugabyteProvider.Action action;

        ActionQuery(YugabyteProvider.Action action) {
            this.action = action;
        }

        @Override
        public SQLQueryAdapter getQuery(YugabyteGlobalState state, ExpectedErrors errors) throws Exception {
            return action.getQuery(state);
        }
    }

    private static class SelectQuery extends Query {

        @Override
        public SQLQueryAdapter getQuery(YugabyteGlobalState state, ExpectedErrors errors) throws Exception {
            return new SQLQueryAdapter(
                    YugabyteVisitor.asString(
                            YugabyteRandomQueryGenerator.createRandomQuery(Randomly.smallNumber() + 1, state)) + ";",
                    errors);
        }
    }
}
