package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteBinaryComparisonOperation.YugabyteBinaryComparisonOperator;
import sqlancer.yugabyte.ast.YugabyteBinaryLogicalOperation.BinaryLogicalOperator;

public final class YugabyteBetweenOperation implements YugabyteExpression {

    private final YugabyteExpression expr;
    private final YugabyteExpression left;
    private final YugabyteExpression right;
    private final boolean isSymmetric;

    public YugabyteBetweenOperation(YugabyteExpression expr, YugabyteExpression left, YugabyteExpression right,
            boolean symmetric) {
        this.expr = expr;
        this.left = left;
        this.right = right;
        isSymmetric = symmetric;
    }

    public YugabyteExpression getExpr() {
        return expr;
    }

    public YugabyteExpression getLeft() {
        return left;
    }

    public YugabyteExpression getRight() {
        return right;
    }

    public boolean isSymmetric() {
        return isSymmetric;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteBinaryComparisonOperation leftComparison = new YugabyteBinaryComparisonOperation(left, expr,
                YugabyteBinaryComparisonOperator.LESS_EQUALS);
        YugabyteBinaryComparisonOperation rightComparison = new YugabyteBinaryComparisonOperation(expr, right,
                YugabyteBinaryComparisonOperator.LESS_EQUALS);
        YugabyteBinaryLogicalOperation andOperation = new YugabyteBinaryLogicalOperation(leftComparison,
                rightComparison, BinaryLogicalOperator.AND);
        if (isSymmetric) {
            YugabyteBinaryComparisonOperation leftComparison2 = new YugabyteBinaryComparisonOperation(right, expr,
                    YugabyteBinaryComparisonOperator.LESS_EQUALS);
            YugabyteBinaryComparisonOperation rightComparison2 = new YugabyteBinaryComparisonOperation(expr, left,
                    YugabyteBinaryComparisonOperator.LESS_EQUALS);
            YugabyteBinaryLogicalOperation andOperation2 = new YugabyteBinaryLogicalOperation(leftComparison2,
                    rightComparison2, BinaryLogicalOperator.AND);
            YugabyteBinaryLogicalOperation orOp = new YugabyteBinaryLogicalOperation(andOperation, andOperation2,
                    BinaryLogicalOperator.OR);
            return orOp.getExpectedValue();
        } else {
            return andOperation.getExpectedValue();
        }
    }

}
