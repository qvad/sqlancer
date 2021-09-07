package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteBinaryComparisonOperation.YugabyteBinaryComparisonOperator;

public class YugabyteBinaryComparisonOperation
        extends BinaryOperatorNode<YugabyteExpression, YugabyteBinaryComparisonOperator> implements YugabyteExpression {

    public enum YugabyteBinaryComparisonOperator implements Operator {
        EQUALS("=") {
            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                return leftVal.isEquals(rightVal);
            }
        },
        IS_DISTINCT("IS DISTINCT FROM") {
            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                return YugabyteConstant
                        .createBooleanConstant(!IS_NOT_DISTINCT.getExpectedValue(leftVal, rightVal).asBoolean());
            }
        },
        IS_NOT_DISTINCT("IS NOT DISTINCT FROM") {
            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                if (leftVal.isNull()) {
                    return YugabyteConstant.createBooleanConstant(rightVal.isNull());
                } else if (rightVal.isNull()) {
                    return YugabyteConstant.createFalse();
                } else {
                    return leftVal.isEquals(rightVal);
                }
            }
        },
        NOT_EQUALS("!=") {
            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                YugabyteConstant isEquals = leftVal.isEquals(rightVal);
                if (isEquals.isBoolean()) {
                    return YugabyteConstant.createBooleanConstant(!isEquals.asBoolean());
                }
                return isEquals;
            }
        },
        LESS("<") {

            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                return leftVal.isLessThan(rightVal);
            }
        },
        LESS_EQUALS("<=") {

            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                YugabyteConstant lessThan = leftVal.isLessThan(rightVal);
                if (lessThan.isBoolean() && !lessThan.asBoolean()) {
                    return leftVal.isEquals(rightVal);
                } else {
                    return lessThan;
                }
            }
        },
        GREATER(">") {
            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                YugabyteConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return YugabyteConstant.createFalse();
                } else {
                    YugabyteConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return YugabyteConstant.createNullConstant();
                    }
                    return YugabytePrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }
        },
        GREATER_EQUALS(">=") {

            @Override
            public YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal) {
                YugabyteConstant equals = leftVal.isEquals(rightVal);
                if (equals.isBoolean() && equals.asBoolean()) {
                    return YugabyteConstant.createTrue();
                } else {
                    YugabyteConstant applyLess = leftVal.isLessThan(rightVal);
                    if (applyLess.isNull()) {
                        return YugabyteConstant.createNullConstant();
                    }
                    return YugabytePrefixOperation.PrefixOperator.NOT.getExpectedValue(applyLess);
                }
            }

        };

        private final String textRepresentation;

        @Override
        public String getTextRepresentation() {
            return textRepresentation;
        }

        YugabyteBinaryComparisonOperator(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public abstract YugabyteConstant getExpectedValue(YugabyteConstant leftVal, YugabyteConstant rightVal);

        public static YugabyteBinaryComparisonOperator getRandom() {
            return Randomly.fromOptions(YugabyteBinaryComparisonOperator.values());
        }

    }

    public YugabyteBinaryComparisonOperation(YugabyteExpression left, YugabyteExpression right,
                                             YugabyteBinaryComparisonOperator op) {
        super(left, right, op);
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant leftExpectedValue = getLeft().getExpectedValue();
        YugabyteConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().getExpectedValue(leftExpectedValue, rightExpectedValue);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

}
