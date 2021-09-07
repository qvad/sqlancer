package sqlancer.yugabyte.ast;

import sqlancer.IgnoreMeException;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabytePrefixOperation implements YugabyteExpression {

    public enum PrefixOperator implements Operator {
        NOT("NOT", YugabyteDataType.BOOLEAN) {

            @Override
            public YugabyteDataType getExpressionType() {
                return YugabyteDataType.BOOLEAN;
            }

            @Override
            protected YugabyteConstant getExpectedValue(YugabyteConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YugabyteConstant.createNullConstant();
                } else {
                    return YugabyteConstant
                            .createBooleanConstant(!expectedValue.cast(YugabyteDataType.BOOLEAN).asBoolean());
                }
            }
        },
        UNARY_PLUS("+", YugabyteDataType.INT) {

            @Override
            public YugabyteDataType getExpressionType() {
                return YugabyteDataType.INT;
            }

            @Override
            protected YugabyteConstant getExpectedValue(YugabyteConstant expectedValue) {
                // TODO: actual converts to double precision
                return expectedValue;
            }

        },
        UNARY_MINUS("-", YugabyteDataType.INT) {

            @Override
            public YugabyteDataType getExpressionType() {
                return YugabyteDataType.INT;
            }

            @Override
            protected YugabyteConstant getExpectedValue(YugabyteConstant expectedValue) {
                if (expectedValue.isNull()) {
                    // TODO
                    throw new IgnoreMeException();
                }
                if (expectedValue.isInt() && expectedValue.asInt() == Long.MIN_VALUE) {
                    throw new IgnoreMeException();
                }
                try {
                    return YugabyteConstant.createIntConstant(-expectedValue.asInt());
                } catch (UnsupportedOperationException e) {
                    return null;
                }
            }

        };

        private String textRepresentation;
        private YugabyteDataType[] dataTypes;

        PrefixOperator(String textRepresentation, YugabyteDataType... dataTypes) {
            this.textRepresentation = textRepresentation;
            this.dataTypes = dataTypes.clone();
        }

        public abstract YugabyteDataType getExpressionType();

        protected abstract YugabyteConstant getExpectedValue(YugabyteConstant expectedValue);

        @Override
        public String getTextRepresentation() {
            return toString();
        }

    }

    private final YugabyteExpression expr;
    private final PrefixOperator op;

    public YugabytePrefixOperation(YugabyteExpression expr, PrefixOperator op) {
        this.expr = expr;
        this.op = op;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return op.getExpressionType();
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.getExpectedValue(expectedValue);
    }

    public YugabyteDataType[] getInputDataTypes() {
        return op.dataTypes;
    }

    public String getTextRepresentation() {
        return op.textRepresentation;
    }

    public YugabyteExpression getExpression() {
        return expr;
    }

}
