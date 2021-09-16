package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.YugabyteGlobalState;

import java.util.stream.Collectors;

public final class YugabyteAnalyzeGenerator {

    private YugabyteAnalyzeGenerator() {
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        YugabyteTable table = globalState.getSchema().getRandomTable();
        StringBuilder sb = new StringBuilder("ANALYZE");
        if (Randomly.getBoolean()) {
            sb.append("(");
            if (Randomly.getBoolean()) {
                sb.append(" VERBOSE");
            } else {
                sb.append(" SKIP_LOCKED");
            }
            sb.append(")");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(table.getName());
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(table.getRandomNonEmptyColumnSubset().stream().map(AbstractTableColumn::getName)
                        .collect(Collectors.joining(", ")));
                sb.append(")");
            }
        }
        // FIXME: bug in Yugabyte?
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("deadlock"));
    }

}
