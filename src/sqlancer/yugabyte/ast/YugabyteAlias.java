package sqlancer.yugabyte.ast;

import sqlancer.common.visitor.UnaryOperation;

public class YugabyteAlias implements UnaryOperation<YugabyteExpression>, YugabyteExpression {

    private final YugabyteExpression expr;
    private final String alias;

    public YugabyteAlias(YugabyteExpression expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public YugabyteExpression getExpression() {
        return expr;
    }

    @Override
    public String getOperatorRepresentation() {
        return " as " + alias;
    }

    @Override
    public boolean omitBracketsWhenPrinting() {
        return true;
    }

    @Override
    public OperatorKind getOperatorKind() {
        return OperatorKind.POSTFIX;
    }

}
