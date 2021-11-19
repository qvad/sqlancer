package sqlancer.yugabyte.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;

/**
 * @see <a href="https://www.postgresql.org/docs/devel/sql-comment.html">COMMENT</a>
 */
public final class YugabyteCommentGenerator {

    private YugabyteCommentGenerator() {
    }

    public static SQLQueryAdapter generate(YugabyteGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("COMMENT ON ");
        Action type = Randomly.fromOptions(Action.values());
        YugabyteTable randomTable = globalState.getSchema().getRandomTable();
        switch (type) {
        case INDEX:
            sb.append("INDEX ");
            if (randomTable.getIndexes().isEmpty()) {
                throw new IgnoreMeException();
            } else {
                sb.append(randomTable.getRandomIndex().getIndexName());
            }
            break;
        case COLUMN:
            sb.append("COLUMN ");
            sb.append(randomTable.getRandomColumn().getFullQualifiedName());
            break;
        case STATISTICS:
            sb.append("STATISTICS ");
            if (randomTable.getStatistics().isEmpty()) {
                throw new IgnoreMeException();
            } else {
                sb.append(randomTable.getStatistics().get(0).getName());
            }
            break;
        case TABLE:
            sb.append("TABLE ");
            if (randomTable.isView()) {
                throw new IgnoreMeException();
            }
            sb.append(randomTable.getName());
            break;
        default:
            throw new AssertionError(type);
        }
        sb.append(" IS ");
        if (Randomly.getBoolean()) {
            sb.append("NULL");
        } else {
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new SQLQueryAdapter(sb.toString());
    }

    private enum Action {
        INDEX, COLUMN, STATISTICS, TABLE
    }

}
