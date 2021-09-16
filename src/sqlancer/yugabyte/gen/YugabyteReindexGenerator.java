package sqlancer.yugabyte.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteSchema.YugabyteIndex;
import sqlancer.yugabyte.YugabyteGlobalState;

import java.util.List;
import java.util.stream.Collectors;

public final class YugabyteReindexGenerator {

    private YugabyteReindexGenerator() {
    }

    private enum Scope {
        INDEX, TABLE, DATABASE;
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("could not create unique index"); // CONCURRENT INDEX
        StringBuilder sb = new StringBuilder();
        sb.append("REINDEX");
        // if (Randomly.getBoolean()) {
        // sb.append(" VERBOSE");
        // }
        sb.append(" ");
        Scope scope = Randomly.fromOptions(Scope.values());
        switch (scope) {
        case INDEX:
            sb.append("INDEX ");
            List<YugabyteIndex> indexes = globalState.getSchema().getRandomTable().getIndexes();
            if (indexes.isEmpty()) {
                throw new IgnoreMeException();
            }
            sb.append(indexes.stream().map(YugabyteIndex::getIndexName).collect(Collectors.joining()));
            break;
        case TABLE:
            sb.append("TABLE ");
            sb.append(globalState.getSchema().getRandomTable(t -> !t.isView()).getName());
            break;
        case DATABASE:
            sb.append("DATABASE ");
            sb.append(globalState.getSchema().getDatabaseName());
            break;
        default:
            throw new AssertionError(scope);
        }
        errors.add("already contains data"); // FIXME bug report
        errors.add("does not exist"); // internal index
        errors.add("REINDEX is not yet implemented for partitioned indexes");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

}
