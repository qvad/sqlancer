package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public interface YugabyteExpression {

    default YugabyteDataType getExpressionType() {
        return null;
    }

    default YugabyteConstant getExpectedValue() {
        return null;
    }
}
