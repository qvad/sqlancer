package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.YugabyteGlobalState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public final class YugabyteVacuumGenerator {

    private YugabyteVacuumGenerator() {
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        String sb = "VACUUM";
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("VACUUM cannot run inside a transaction block");
        return new SQLQueryAdapter(sb, errors);
    }

}
