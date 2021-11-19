package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteCompoundDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteCastOperation implements YugabyteExpression {

    private final YugabyteExpression expression;
    private final YugabyteCompoundDataType type;

    public YugabyteCastOperation(YugabyteExpression expression, YugabyteCompoundDataType type) {
        if (expression == null) {
            throw new AssertionError();
        }
        this.expression = expression;
        this.type = type;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return type.getDataType();
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant expectedValue = expression.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return expectedValue.cast(type.getDataType());
    }

    public YugabyteExpression getExpression() {
        return expression;
    }

    public YugabyteDataType getType() {
        return type.getDataType();
    }

    public YugabyteCompoundDataType getCompoundType() {
        return type;
    }

}
