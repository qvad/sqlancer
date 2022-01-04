package sqlancer.yugabyte;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.yugabyte.YugabyteOptions.YugabyteOracleFactory;
import sqlancer.yugabyte.oracle.YugabyteCatalog;
import sqlancer.yugabyte.oracle.YugabyteFuzzer;
import sqlancer.yugabyte.oracle.YugabyteNoRECOracle;
import sqlancer.yugabyte.oracle.YugabytePivotedQuerySynthesisOracle;
import sqlancer.yugabyte.oracle.tlp.YugabyteTLPAggregateOracle;
import sqlancer.yugabyte.oracle.tlp.YugabyteTLPHavingOracle;
import sqlancer.yugabyte.oracle.tlp.YugabyteTLPWhereOracle;

@Parameters(separators = "=", commandDescription = "YSQL (default port: " + YugabyteOptions.DEFAULT_PORT
        + ", default host: " + YugabyteOptions.DEFAULT_HOST)
public class YugabyteOptions implements DBMSSpecificOptions<YugabyteOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5433;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for YSQL")
    public List<YugabyteOracleFactory> oracle = Arrays.asList(YugabyteOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the YSQL server", arity = 1)
    public String connectionURL = String.format("jdbc:postgresql://%s:%d/yugabyte", YugabyteOptions.DEFAULT_HOST,
            YugabyteOptions.DEFAULT_PORT);

    @Override
    public List<YugabyteOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public enum YugabyteOracleFactory implements OracleFactory<YugabyteGlobalState> {
        FUZZER {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                return new YugabyteFuzzer(globalState);
            }
        },
        CATALOG {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                return new YugabyteCatalog(globalState);
            }
        },
        NOREC {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                return new YugabyteNoRECOracle(globalState);
            }
        },
        PQS {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                return new YugabytePivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        HAVING {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                return new YugabyteTLPHavingOracle(globalState);
            }

        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle create(YugabyteGlobalState globalState) throws SQLException {
                List<TestOracle> oracles = new ArrayList<>();
                oracles.add(new YugabyteTLPWhereOracle(globalState));
                oracles.add(new YugabyteTLPHavingOracle(globalState));
                oracles.add(new YugabyteTLPAggregateOracle(globalState));
                return new CompositeTestOracle(oracles, globalState);
            }
        }

    }

}
