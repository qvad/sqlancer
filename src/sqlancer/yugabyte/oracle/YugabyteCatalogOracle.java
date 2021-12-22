package sqlancer.yugabyte.oracle;

import static sqlancer.yugabyte.YugabyteProvider.CREATION_LOCK;

import java.sql.SQLException;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteProvider;
import sqlancer.yugabyte.gen.YugabyteCommon;
import sqlancer.yugabyte.gen.YugabyteTableGenerator;

public class YugabyteCatalogOracle implements TestOracle {
    protected final YugabyteGlobalState state;

    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final Main.StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;

    public YugabyteCatalogOracle(YugabyteGlobalState globalState) {
        this.state = globalState;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
        YugabyteCommon.addCommonExpressionErrors(errors);
        YugabyteCommon.addCommonFetchErrors(errors);
    }

    protected void readFunctions(YugabyteGlobalState globalState) throws SQLException {
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }
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

    protected void prepareTables(YugabyteGlobalState globalState) throws Exception {
        for (int i = 0; i < 1000; i++) {
            StatementExecutor<YugabyteGlobalState, YugabyteProvider.Action> se = new StatementExecutor<>(globalState,
                    YugabyteProvider.Action.values(), YugabyteProvider::mapActions, (q) -> {
                        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
            se.executeStatements();
            globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        }
    }

    @Override
    public void check() throws Exception {
        readFunctions(state);
        createTables(state, 10);
        prepareTables(state);
    }
}
