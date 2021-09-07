package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteBinaryArithmeticOperation.YugabyteBinaryOperator;

import java.util.function.BinaryOperator;

public class YugabyteBinaryArithmeticOperation extends BinaryOperatorNode<YugabyteExpression, YugabyteBinaryOperator>
        implements YugabyteExpression {

    public enum YugabyteBinaryOperator implements Operator {

        ADDITION("+") {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return applyBitOperation(left, right, (l, r) -> l + r);
            }

        },
        SUBTRACTION("-") {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return applyBitOperation(left, right, (l, r) -> l - r);
            }
        },
        MULTIPLICATION("*") {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return applyBitOperation(left, right, (l, r) -> l * r);
            }
        },
        DIVISION("/") {

            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l / r);

            }

        },
        MODULO("%") {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return applyBitOperation(left, right, (l, r) -> r == 0 ? -1 : l % r);

            }
        },
        EXPONENTIATION("^") {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                return null;
            }
        };

        private String textRepresentation;

        private static YugabyteConstant applyBitOperation(YugabyteConstant left, YugabyteConstant right,
                                                          BinaryOperator<Long> op) {
            if (left.isNull() || right.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else {
                long leftVal = left.cast(YugabyteDataType.INT).asInt();
                long rightVal = right.cast(YugabyteDataType.INT).asInt();
                long value = op.apply(leftVal, rightVal);
                return YugabyteConstant.createIntConstant(value);
            }
        }

        YugabyteBinaryOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        public abstract YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right);

        public static YugabyteBinaryOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public YugabyteBinaryArithmeticOperation(YugabyteExpression left, YugabyteExpression right,
                                             YugabyteBinaryOperator op) {
        super(left, right, op);
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant leftExpected = getLeft().getExpectedValue();
        YugabyteConstant rightExpected = getRight().getExpectedValue();
        if (leftExpected == null || rightExpected == null) {
            return null;
        }
        return getOp().apply(leftExpected, rightExpected);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.INT;
    }

}
