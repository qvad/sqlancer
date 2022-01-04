package sqlancer.yugabyte.oracle;

import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteProvider;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.gen.YugabyteRandomQueryGenerator;

public class YugabyteFuzzer implements TestOracle {
    private final YugabyteGlobalState globalState;

    private final List<YugabyteProvider.Action> dmlActions = Arrays.asList(YugabyteProvider.Action.INSERT,
            YugabyteProvider.Action.UPDATE, YugabyteProvider.Action.DELETE);

    public YugabyteFuzzer(YugabyteGlobalState globalState) {
        this.globalState = globalState;
    }

    private YugabyteProvider.Action getRandomAction(List<YugabyteProvider.Action> actions) {
        return actions.get(globalState.getRandomly().getInteger(0, actions.size()));
    }

    @Override
    public void check() throws Exception {
        SQLQueryAdapter s;
        if (globalState.getRandomly().getInteger(0, 100) > 80) {
            s = getRandomAction(dmlActions).getQuery(globalState);
        } else {
            s = new SQLQueryAdapter(YugabyteVisitor.asString(
                    YugabyteRandomQueryGenerator.createRandomQuery(Randomly.smallNumber() + 1, globalState)) + ";");
        }
        globalState.executeStatement(s);
        globalState.getManager().incrementSelectQueryCount();
    }
}
