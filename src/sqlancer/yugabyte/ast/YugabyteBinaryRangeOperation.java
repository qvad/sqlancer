package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteBinaryRangeOperation extends BinaryNode<YugabyteExpression> implements YugabyteExpression {

    private final String op;

    public enum YugabyteBinaryRangeOperator implements Operator {
        UNION("*"), INTERSECTION("*"), DIFFERENCE("-");

        private final String textRepresentation;

        YugabyteBinaryRangeOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static YugabyteBinaryRangeOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum YugabyteBinaryRangeComparisonOperator {
        CONTAINS_RANGE_OR_ELEMENT("@>"), RANGE_OR_ELEMENT_IS_CONTAINED("<@"), OVERLAP("&&"), STRICT_LEFT_OF("<<"),
        STRICT_RIGHT_OF(">>"), NOT_RIGHT_OF("&<"), NOT_LEFT_OF(">&"), ADJACENT("-|-");

        private final String textRepresentation;

        YugabyteBinaryRangeComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static YugabyteBinaryRangeComparisonOperator getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public YugabyteBinaryRangeOperation(YugabyteBinaryRangeComparisonOperator op, YugabyteExpression left,
            YugabyteExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    public YugabyteBinaryRangeOperation(YugabyteBinaryRangeOperator op, YugabyteExpression left,
            YugabyteExpression right) {
        super(left, right);
        this.op = op.getTextRepresentation();
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public String getOperatorRepresentation() {
        return op;
    }

}
