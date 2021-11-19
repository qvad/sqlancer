package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteOrderByTerm implements YugabyteExpression {

    private final YugabyteOrder order;
    private final YugabyteExpression expr;

    public YugabyteOrderByTerm(YugabyteExpression expr, YugabyteOrder order) {
        this.expr = expr;
        this.order = order;
    }

    public YugabyteOrder getOrder() {
        return order;
    }

    public YugabyteExpression getExpr() {
        return expr;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return null;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        throw new AssertionError(this);
    }

    public enum YugabyteOrder {
        ASC, DESC;

        public static YugabyteOrder getRandomOrder() {
            return Randomly.fromOptions(YugabyteOrder.values());
        }
    }

}
