package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteSelect;

public final class YugabyteViewGenerator {

    private YugabyteViewGenerator() {
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("CREATE");
        if (Randomly.getBoolean()) {
            sb.append(" OR REPLACE");
        }
        if (Randomly.getBoolean()) {
            sb.append(Randomly.fromOptions(" TEMP", " TEMPORARY"));
        }
        sb.append(" VIEW ");
        int i = 0;
        String[] name = new String[1];
        while (true) {
            name[0] = "v" + i++;
            if (globalState.getSchema().getDatabaseTables().stream()
                    .noneMatch(tab -> tab.getName().contentEquals(name[0]))) {
                break;
            }
        }
        sb.append(name[0]);
        sb.append("(");
        int nrColumns = Randomly.smallNumber() + 1;
        for (i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(DBMSCommon.createColumnName(i));
        }
        sb.append(")");
        // if (Randomly.getBoolean() && false) {
        // sb.append(" WITH(");
        // if (Randomly.getBoolean()) {
        // sb.append(String.format("security_barrier(%s)", Randomly.getBoolean()));
        // } else {
        // sb.append(String.format("check_option(%s)", Randomly.fromOptions("local1", "cascaded")));
        // }
        // sb.append(")");
        // }
        sb.append(" AS (");
        YugabyteSelect select = YugabyteRandomQueryGenerator.createRandomQuery(nrColumns, globalState);
        sb.append(YugabyteVisitor.asString(select));
        sb.append(")");
        YugabyteCommon.addGroupingErrors(errors);
        YugabyteCommon.addViewErrors(errors);
        YugabyteCommon.addCommonExpressionErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
