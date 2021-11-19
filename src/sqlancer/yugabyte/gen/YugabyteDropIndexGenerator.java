package sqlancer.yugabyte.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteSchema.YugabyteIndex;

public final class YugabyteDropIndexGenerator {

    private YugabyteDropIndexGenerator() {
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        List<YugabyteIndex> indexes = globalState.getSchema().getRandomTable().getIndexes();
        StringBuilder sb = new StringBuilder();
        sb.append("DROP INDEX ");
        if (Randomly.getBoolean() || indexes.isEmpty()) {
            sb.append("IF EXISTS ");
            if (indexes.isEmpty() || Randomly.getBoolean()) {
                sb.append(DBMSCommon.createIndexName(Randomly.smallNumber()));
            } else {
                sb.append(Randomly.fromList(indexes).getIndexName());
            }
        } else {
            sb.append(Randomly.fromList(indexes).getIndexName());
        }
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("cannot drop desired object(s) because other objects depend on them",
                        "cannot drop index", "does not exist"),
                true);
    }

}
