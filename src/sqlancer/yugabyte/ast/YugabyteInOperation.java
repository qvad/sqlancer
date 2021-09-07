package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

import java.util.List;

public class YugabyteInOperation implements YugabyteExpression {

    private final YugabyteExpression expr;
    private final List<YugabyteExpression> listElements;
    private final boolean isTrue;

    public YugabyteInOperation(YugabyteExpression expr, List<YugabyteExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public YugabyteExpression getExpr() {
        return expr;
    }

    public List<YugabyteExpression> getListElements() {
        return listElements;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant leftValue = expr.getExpectedValue();
        if (leftValue == null) {
            return null;
        }
        if (leftValue.isNull()) {
            return YugabyteConstant.createNullConstant();
        }
        boolean isNull = false;
        for (YugabyteExpression expr : getListElements()) {
            YugabyteConstant rightExpectedValue = expr.getExpectedValue();
            if (rightExpectedValue == null) {
                return null;
            }
            if (rightExpectedValue.isNull()) {
                isNull = true;
            } else if (rightExpectedValue.isEquals(this.expr.getExpectedValue()).isBoolean()
                    && rightExpectedValue.isEquals(this.expr.getExpectedValue()).asBoolean()) {
                return YugabyteConstant.createBooleanConstant(isTrue);
            }
        }

        if (isNull) {
            return YugabyteConstant.createNullConstant();
        } else {
            return YugabyteConstant.createBooleanConstant(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }
}
