package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabytePostfixOperation implements YugabyteExpression {

    private final YugabyteExpression expr;
    private final PostfixOperator op;
    private final String operatorTextRepresentation;

    public enum PostfixOperator implements Operator {
        IS_NULL("IS NULL", "ISNULL") {
            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                return YugabyteConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return YugabyteDataType.values();
            }

        },
        IS_UNKNOWN("IS UNKNOWN") {
            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                return YugabyteConstant.createBooleanConstant(expectedValue.isNull());
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return new YugabyteDataType[] { YugabyteDataType.BOOLEAN };
            }
        },

        IS_NOT_NULL("IS NOT NULL", "NOTNULL") {

            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                return YugabyteConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return YugabyteDataType.values();
            }

        },
        IS_NOT_UNKNOWN("IS NOT UNKNOWN") {
            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                return YugabyteConstant.createBooleanConstant(!expectedValue.isNull());
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return new YugabyteDataType[] { YugabyteDataType.BOOLEAN };
            }
        },
        IS_TRUE("IS TRUE") {

            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YugabyteConstant.createFalse();
                } else {
                    return YugabyteConstant
                            .createBooleanConstant(expectedValue.cast(YugabyteDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return new YugabyteDataType[] { YugabyteDataType.BOOLEAN };
            }

        },
        IS_FALSE("IS FALSE") {

            @Override
            public YugabyteConstant apply(YugabyteConstant expectedValue) {
                if (expectedValue.isNull()) {
                    return YugabyteConstant.createFalse();
                } else {
                    return YugabyteConstant
                            .createBooleanConstant(!expectedValue.cast(YugabyteDataType.BOOLEAN).asBoolean());
                }
            }

            @Override
            public YugabyteDataType[] getInputDataTypes() {
                return new YugabyteDataType[] { YugabyteDataType.BOOLEAN };
            }

        };

        private String[] textRepresentations;

        PostfixOperator(String... textRepresentations) {
            this.textRepresentations = textRepresentations.clone();
        }

        public abstract YugabyteConstant apply(YugabyteConstant expectedValue);

        public abstract YugabyteDataType[] getInputDataTypes();

        public static PostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

    public YugabytePostfixOperation(YugabyteExpression expr, PostfixOperator op) {
        this.expr = expr;
        this.operatorTextRepresentation = Randomly.fromOptions(op.textRepresentations);
        this.op = op;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        YugabyteConstant expectedValue = expr.getExpectedValue();
        if (expectedValue == null) {
            return null;
        }
        return op.apply(expectedValue);
    }

    public String getOperatorTextRepresentation() {
        return operatorTextRepresentation;
    }

    public static YugabyteExpression create(YugabyteExpression expr, PostfixOperator op) {
        return new YugabytePostfixOperation(expr, op);
    }

    public YugabyteExpression getExpression() {
        return expr;
    }

}
