package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteColumnValue implements YugabyteExpression {

    private final YugabyteColumn c;
    private final YugabyteConstant expectedValue;

    public YugabyteColumnValue(YugabyteColumn c, YugabyteConstant expectedValue) {
        this.c = c;
        this.expectedValue = expectedValue;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return c.getType();
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return expectedValue;
    }

    public static YugabyteColumnValue create(YugabyteColumn c, YugabyteConstant expected) {
        return new YugabyteColumnValue(c, expected);
    }

    public YugabyteColumn getColumn() {
        return c;
    }

}
