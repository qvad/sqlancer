package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteGlobalState;

public final class YugabyteNotifyGenerator {

    private YugabyteNotifyGenerator() {
    }

    private static String getChannel() {
        return Randomly.fromOptions("asdf", "test");
    }

    public static SQLQueryAdapter createNotify(YugabyteGlobalState globalState) {
        StringBuilder sb = new StringBuilder();
        sb.append("NOTIFY ");
        sb.append(getChannel());
        if (Randomly.getBoolean()) {
            sb.append(", ");
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new SQLQueryAdapter(sb.toString());
    }

    public static SQLQueryAdapter createListen() {
        String sb = "LISTEN " + getChannel();
        return new SQLQueryAdapter(sb);
    }

    public static SQLQueryAdapter createUnlisten() {
        StringBuilder sb = new StringBuilder();
        sb.append("UNLISTEN ");
        if (Randomly.getBoolean()) {
            sb.append(getChannel());
        } else {
            sb.append("*");
        }
        return new SQLQueryAdapter(sb.toString());
    }

}
