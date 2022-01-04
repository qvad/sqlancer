package sqlancer.yugabyte.oracle;

import static sqlancer.yugabyte.YugabyteProvider.CREATION_LOCK;

import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteProvider;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteTableGenerator;

public class YugabyteCatalog implements TestOracle {
    protected final YugabyteGlobalState state;

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    private final List<YugabyteProvider.Action> dmlActions = Arrays.asList(YugabyteProvider.Action.INSERT,
            YugabyteProvider.Action.UPDATE, YugabyteProvider.Action.DELETE);
    private final List<YugabyteProvider.Action> catalogActions = Arrays.asList(YugabyteProvider.Action.CREATE_VIEW,
            YugabyteProvider.Action.CREATE_SEQUENCE, YugabyteProvider.Action.ALTER_TABLE,
            YugabyteProvider.Action.SET_CONSTRAINTS, YugabyteProvider.Action.DISCARD,
            YugabyteProvider.Action.DROP_INDEX, YugabyteProvider.Action.COMMENT_ON, YugabyteProvider.Action.RESET_ROLE,
            YugabyteProvider.Action.RESET);
    private final List<YugabyteProvider.Action> diskActions = Arrays.asList(YugabyteProvider.Action.TRUNCATE,
            YugabyteProvider.Action.VACUUM);

    public YugabyteCatalog(YugabyteGlobalState globalState) {
        this.state = globalState;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
    }

    private YugabyteProvider.Action getRandomAction(List<YugabyteProvider.Action> actions) {
        return actions.get(state.getRandomly().getInteger(0, actions.size()));
    }

    protected void createTables(YugabyteGlobalState globalState, int numTables) throws Exception {
        synchronized (CREATION_LOCK) {
            while (globalState.getSchema().getDatabaseTables().size() < numTables) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                    SQLQueryAdapter createTable = YugabyteTableGenerator.generate(tableName, true, globalState);
                    globalState.executeStatement(createTable);
                    globalState.getManager().incrementSelectQueryCount();
                    globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
                } catch (IgnoreMeException e) {
                    // do nothing
                }
            }
        }
    }

    @Override
    public void check() throws Exception {
        // create table or evaluate catalog test
        int seed = state.getRandomly().getInteger(1, 100);
        if (seed > 95) {
            createTables(state, 1);
        } else {
            YugabyteProvider.Action randomAction;

            if (seed > 40) {
                randomAction = getRandomAction(dmlActions);
            } else if (seed > 10) {
                randomAction = getRandomAction(catalogActions);
            } else {
                randomAction = getRandomAction(diskActions);
            }

            state.executeStatement(randomAction.getQuery(state));
        }
        state.getManager().incrementSelectQueryCount();
    }
}
