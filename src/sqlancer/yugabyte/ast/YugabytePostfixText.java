package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabytePostfixText implements YugabyteExpression {

    private final YugabyteExpression expr;
    private final String text;
    private final YugabyteConstant expectedValue;
    private final YugabyteDataType type;

    public YugabytePostfixText(YugabyteExpression expr, String text, YugabyteConstant expectedValue,
            YugabyteDataType type) {
        this.expr = expr;
        this.text = text;
        this.expectedValue = expectedValue;
        this.type = type;
    }

    public YugabyteExpression getExpr() {
        return expr;
    }

    public String getText() {
        return text;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return type;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return expectedValue;
    }
}
