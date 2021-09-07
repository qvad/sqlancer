package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteBinaryLogicalOperation.BinaryLogicalOperator;

public class YugabyteBinaryLogicalOperation extends BinaryOperatorNode<YugabyteExpression, BinaryLogicalOperator>
        implements YugabyteExpression {

    public enum BinaryLogicalOperator implements Operator {
        AND {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                YugabyteConstant leftBool = left.cast(YugabyteDataType.BOOLEAN);
                YugabyteConstant rightBool = right.cast(YugabyteDataType.BOOLEAN);
                if (leftBool.isNull()) {
                    if (rightBool.isNull()) {
                        return YugabyteConstant.createNullConstant();
                    } else {
                        if (rightBool.asBoolean()) {
                            return YugabyteConstant.createNullConstant();
                        } else {
                            return YugabyteConstant.createFalse();
                        }
                    }
                } else if (!leftBool.asBoolean()) {
                    return YugabyteConstant.createFalse();
                }
                assert leftBool.asBoolean();
                if (rightBool.isNull()) {
                    return YugabyteConstant.createNullConstant();
                } else {
                    return YugabyteConstant.createBooleanConstant(rightBool.isBoolean() && rightBool.asBoolean());
                }
            }
        },
        OR {
            @Override
            public YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right) {
                YugabyteConstant leftBool = left.cast(YugabyteDataType.BOOLEAN);
                YugabyteConstant rightBool = right.cast(YugabyteDataType.BOOLEAN);
                if (leftBool.isBoolean() && leftBool.asBoolean()) {
                    return YugabyteConstant.createTrue();
                }
                if (rightBool.isBoolean() && rightBool.asBoolean()) {
                    return YugabyteConstant.createTrue();
                }
                if (leftBool.isNull() || rightBool.isNull()) {
                    return YugabyteConstant.createNullConstant();
                }
                return YugabyteConstant.createFalse();
            }
        };

        public abstract YugabyteConstant apply(YugabyteConstant left, YugabyteConstant right);

        public static BinaryLogicalOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public YugabyteBinaryLogicalOperation(YugabyteExpression left, YugabyteExpression right, BinaryLogicalOperator op) {
        super(left, right, op);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant leftExpectedValue = getLeft().getExpectedValue();
        YugabyteConstant rightExpectedValue = getRight().getExpectedValue();
        if (leftExpectedValue == null || rightExpectedValue == null) {
            return null;
        }
        return getOp().apply(leftExpectedValue, rightExpectedValue);
    }

}
