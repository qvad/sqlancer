package sqlancer.yugabyte.ast;

import sqlancer.common.ast.BinaryNode;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteConcatOperation extends BinaryNode<YugabyteExpression> implements YugabyteExpression {

    public YugabyteConcatOperation(YugabyteExpression left, YugabyteExpression right) {
        super(left, right);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.TEXT;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant leftExpectedValue = getLeft().getExpectedValue();
        YugabyteConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        if (leftExpectedValue.isNull() || rightExpectedValue.isNull()) {
            return YugabyteConstant.createNullConstant();
        }
        String leftStr = leftExpectedValue.cast(YugabyteDataType.TEXT).getUnquotedTextRepresentation();
        String rightStr = rightExpectedValue.cast(YugabyteDataType.TEXT).getUnquotedTextRepresentation();
        return YugabyteConstant.createTextConstant(leftStr + rightStr);
    }

    @Override
    public String getOperatorRepresentation() {
        return "||";
    }

}
