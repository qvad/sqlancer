package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteBinaryBitOperation.YugabyteBinaryBitOperator;

public class YugabyteBinaryBitOperation extends BinaryOperatorNode<YugabyteExpression, YugabyteBinaryBitOperator>
        implements YugabyteExpression {

    public YugabyteBinaryBitOperation(YugabyteBinaryBitOperator op, YugabyteExpression left, YugabyteExpression right) {
        super(left, right, op);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BIT;
    }

    public enum YugabyteBinaryBitOperator implements Operator {
        CONCATENATION("||"), //
        BITWISE_AND("&"), //
        BITWISE_OR("|"), //
        BITWISE_XOR("#"), //
        BITWISE_SHIFT_LEFT("<<"), //
        BITWISE_SHIFT_RIGHT(">>");

        private final String text;

        YugabyteBinaryBitOperator(String text) {
            this.text = text;
        }

        public static YugabyteBinaryBitOperator getRandom() {
            return Randomly.fromOptions(YugabyteBinaryBitOperator.values());
        }

        @Override
        public String getTextRepresentation() {
            return text;
        }

    }

}
