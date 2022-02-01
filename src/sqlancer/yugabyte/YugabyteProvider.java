package sqlancer.yugabyte;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import sqlancer.AbstractAction;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.yugabyte.YugabyteOptions.YugabyteOracleFactory;
import sqlancer.yugabyte.gen.YugabyteAlterTableGenerator;
import sqlancer.yugabyte.gen.YugabyteCommentGenerator;
import sqlancer.yugabyte.gen.YugabyteDeleteGenerator;
import sqlancer.yugabyte.gen.YugabyteDiscardGenerator;
import sqlancer.yugabyte.gen.YugabyteDropIndexGenerator;
import sqlancer.yugabyte.gen.YugabyteIndexGenerator;
import sqlancer.yugabyte.gen.YugabyteInsertGenerator;
import sqlancer.yugabyte.gen.YugabyteNotifyGenerator;
import sqlancer.yugabyte.gen.YugabyteSequenceGenerator;
import sqlancer.yugabyte.gen.YugabyteSetGenerator;
import sqlancer.yugabyte.gen.YugabyteTableGenerator;
import sqlancer.yugabyte.gen.YugabyteTransactionGenerator;
import sqlancer.yugabyte.gen.YugabyteTruncateGenerator;
import sqlancer.yugabyte.gen.YugabyteUpdateGenerator;
import sqlancer.yugabyte.gen.YugabyteVacuumGenerator;
import sqlancer.yugabyte.gen.YugabyteViewGenerator;

// EXISTS
// IN
public class YugabyteProvider extends SQLProviderAdapter<YugabyteGlobalState, YugabyteOptions> {

    public static final Object CREATION_LOCK = new Object();
    /**
     * Generate only data types and expressions that are understood by PQS.
     */
    public static boolean generateOnlyKnown;
    protected String entryURL;
    protected String username;
    protected String password;
    protected String entryPath;
    protected String host;
    protected int port;
    protected String testURL;
    protected String databaseName;
    protected String createDatabaseCommand;

    public YugabyteProvider() {
        super(YugabyteGlobalState.class, YugabyteOptions.class);
    }

    protected YugabyteProvider(Class<YugabyteGlobalState> globalClass, Class<YugabyteOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public static int mapActions(YugabyteGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed;
        switch (a) {
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DISCARD:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case COMMIT:
            nrPerformed = r.getInteger(0, 0);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case RESET:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
        case RESET_ROLE:
        case VACUUM:
        case SET_CONSTRAINTS:
        case SET:
        case COMMENT_ON:
        case NOTIFY:
        case LISTEN:
        case UNLISTEN:
        case CREATE_SEQUENCE:
        case TRUNCATE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    public void generateDatabase(YugabyteGlobalState globalState) throws Exception {
        readFunctions(globalState);
        createTables(globalState, Randomly.fromOptions(4, 5, 6));
        prepareTables(globalState);
    }

    @Override
    public SQLConnection createDatabase(YugabyteGlobalState globalState) throws SQLException {
        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == YugabyteOracleFactory.PQS)) {
            generateOnlyKnown = true;
        }

        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        entryPath = "/yugabyte";
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        String entryDatabaseName = entryPath.substring(1);
        databaseName = globalState.getDatabaseName();

        try {
            URI uri = new URI(entryURL);
            String userInfoURI = uri.getUserInfo();
            String pathURI = uri.getPath();
            if (userInfoURI != null) {
                // username and password specified in URL take precedence
                if (userInfoURI.contains(":")) {
                    String[] userInfo = userInfoURI.split(":", 2);
                    username = userInfo[0];
                    password = userInfo[1];
                } else {
                    username = userInfoURI;
                    password = null;
                }
                int userInfoIndex = entryURL.indexOf(userInfoURI);
                String preUserInfo = entryURL.substring(0, userInfoIndex);
                String postUserInfo = entryURL.substring(userInfoIndex + userInfoURI.length() + 1);
                entryURL = preUserInfo + postUserInfo;
            }
            if (pathURI != null) {
                entryPath = pathURI;
            }
            if (host == null) {
                host = uri.getHost();
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = uri.getPort();
            }
            entryURL = String.format("jdbc:postgresql://%s:%d/%s", host, port, entryDatabaseName);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }

        createDatabaseSync(globalState, entryDatabaseName);

        int databaseIndex = entryURL.indexOf(entryDatabaseName);
        String preDatabaseName = entryURL.substring(0, databaseIndex);
        String postDatabaseName = entryURL.substring(databaseIndex + entryDatabaseName.length());
        testURL = preDatabaseName + databaseName + postDatabaseName;
        globalState.getState().logStatement(String.format("\\c %s;", databaseName));

        return new SQLConnection(createConnectionSafely(testURL, username, password));
    }

    @Override
    public String getDBMSName() {
        return "yugabyte";
    }

    // for some reason yugabyte unable to create few databases simultaneously
    private void createDatabaseSync(YugabyteGlobalState globalState, String entryDatabaseName) throws SQLException {
        synchronized (CREATION_LOCK) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Connection con = createConnectionSafely(entryURL, username, password);
            globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));
            globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
            createDatabaseCommand = getCreateDatabaseCommand(globalState);
            globalState.getState().logStatement(createDatabaseCommand);
            try (Statement s = con.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + databaseName);
            }
            try (Statement s = con.createStatement()) {
                s.execute(createDatabaseCommand);
            }
            con.close();
        }
    }

    private Connection createConnectionSafely(String entryURL, String user, String password) {
        Connection con = null;
        IllegalStateException lastException = new IllegalStateException("Empty exception");
        long endTime = System.currentTimeMillis() + 30000;
        while (System.currentTimeMillis() < endTime) {
            try {
                con = DriverManager.getConnection(entryURL, user, password);
                break;
            } catch (SQLException throwables) {
                lastException = new IllegalStateException(throwables);
            }
        }

        if (con == null) {
            throw lastException;
        }

        return con;
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
                Thread.sleep(2000);

                try {
                    String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                    SQLQueryAdapter createTable = YugabyteTableGenerator.generate(tableName, generateOnlyKnown,
                            globalState);
                    globalState.executeStatement(createTable);
                } catch (IgnoreMeException e) {
                    // do nothing
                }
            }
        }
    }

    protected void prepareTables(YugabyteGlobalState globalState) throws Exception {
        StatementExecutor<YugabyteGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                YugabyteProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 15000;\n"));
    }

    private String getCreateDatabaseCommand(YugabyteGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE ").append(databaseName).append(" ");
        if (Randomly.getBoolean() && state.getDbmsSpecificOptions().testCollations) {
            sb.append("WITH ");
            if (Randomly.getBoolean()) {
                sb.append("ENCODING '");
                sb.append(Randomly.fromOptions("utf8"));
                sb.append("' ");
            }
            if (Randomly.getBoolean()) {
                sb.append("COLOCATED = true ");
            }
            for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                    sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                }
            }
            sb.append(" TEMPLATE template0");

        }
        return sb.toString();
    }

    public enum Action implements AbstractAction<YugabyteGlobalState> {
        ALTER_TABLE(g -> YugabyteAlterTableGenerator.create(g.getSchema().getRandomTable(t -> !t.isView()), g)), //
        COMMIT(g -> {
            SQLQueryAdapter query;
            if (Randomly.getBoolean()) {
                query = new SQLQueryAdapter("COMMIT", true);
            } else if (Randomly.getBoolean()) {
                query = YugabyteTransactionGenerator.executeBegin();
            } else {
                query = new SQLQueryAdapter("ROLLBACK", true);
            }
            return query;
        }), //
        DELETE(YugabyteDeleteGenerator::create), //
        DISCARD(YugabyteDiscardGenerator::create), //
        DROP_INDEX(YugabyteDropIndexGenerator::create), //
        CREATE_INDEX(YugabyteIndexGenerator::generate), //
        INSERT(YugabyteInsertGenerator::insert), //
        UPDATE(YugabyteUpdateGenerator::create), //
        TRUNCATE(YugabyteTruncateGenerator::create), //
        VACUUM(YugabyteVacuumGenerator::create), //
        SET(YugabyteSetGenerator::create), // TODO insert yugabyte sets
        SET_CONSTRAINTS((g) -> {
            String sb = "SET CONSTRAINTS ALL " + Randomly.fromOptions("DEFERRED", "IMMEDIATE");
            return new SQLQueryAdapter(sb);
        }), //
        RESET_ROLE((g) -> new SQLQueryAdapter("RESET ROLE")), //
        COMMENT_ON(YugabyteCommentGenerator::generate), //
        RESET((g) -> new SQLQueryAdapter("RESET ALL") /*
                                                       * https://www.postgres.org/docs/devel/sql-reset.html TODO: also
                                                       * configuration parameter
                                                       */), //
        NOTIFY(YugabyteNotifyGenerator::createNotify), //
        LISTEN((g) -> YugabyteNotifyGenerator.createListen()), //
        UNLISTEN((g) -> YugabyteNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(YugabyteSequenceGenerator::createSequence), //
        CREATE_VIEW(YugabyteViewGenerator::create);

        private final SQLQueryProvider<YugabyteGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<YugabyteGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(YugabyteGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

}
