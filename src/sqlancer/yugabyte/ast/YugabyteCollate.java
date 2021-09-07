package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteCollate implements YugabyteExpression {

    private final YugabyteExpression expr;
    private final String collate;

    public YugabyteCollate(YugabyteExpression expr, String collate) {
        this.expr = expr;
        this.collate = collate;
    }

    public String getCollate() {
        return collate;
    }

    public YugabyteExpression getExpr() {
        return expr;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return expr.getExpressionType();
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return null;
    }

}
