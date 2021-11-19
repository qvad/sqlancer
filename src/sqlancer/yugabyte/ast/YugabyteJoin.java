package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteJoin implements YugabyteExpression {

    private final YugabyteExpression tableReference;
    private final YugabyteExpression onClause;
    private final YugabyteJoinType type;

    public YugabyteJoin(YugabyteExpression tableReference, YugabyteExpression onClause, YugabyteJoinType type) {
        this.tableReference = tableReference;
        this.onClause = onClause;
        this.type = type;
    }

    public YugabyteExpression getTableReference() {
        return tableReference;
    }

    public YugabyteExpression getOnClause() {
        return onClause;
    }

    public YugabyteJoinType getType() {
        return type;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        throw new AssertionError();
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        throw new AssertionError();
    }

    public enum YugabyteJoinType {
        INNER, LEFT, RIGHT, FULL, CROSS;

        public static YugabyteJoinType getRandom() {
            return Randomly.fromOptions(values());
        }

    }

}
