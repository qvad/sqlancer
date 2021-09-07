package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteFunction implements YugabyteExpression {

    private final String func;
    private final YugabyteExpression[] args;
    private final YugabyteDataType returnType;
    private YugabyteFunctionWithResult functionWithKnownResult;

    public YugabyteFunction(YugabyteFunctionWithResult func, YugabyteDataType returnType, YugabyteExpression... args) {
        functionWithKnownResult = func;
        this.func = func.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public YugabyteFunction(YugabyteFunctionWithUnknownResult f, YugabyteDataType returnType,
                            YugabyteExpression... args) {
        this.func = f.getName();
        this.returnType = returnType;
        this.args = args.clone();
    }

    public String getFunctionName() {
        return func;
    }

    public YugabyteExpression[] getArguments() {
        return args.clone();
    }

    public enum YugabyteFunctionWithResult {
        ABS(1, "abs") {

            @Override
            public YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YugabyteConstant.createNullConstant();
                } else {
                    return YugabyteConstant
                            .createIntConstant(Math.abs(evaluatedArgs[0].cast(YugabyteDataType.INT).asInt()));
                }
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.INT;
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return new YugabyteDataType[] { returnType };
            }

        },
        LOWER(1, "lower") {

            @Override
            public YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YugabyteConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return YugabyteConstant.createTextConstant(text.toLowerCase());
                }
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.TEXT;
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return new YugabyteDataType[] { YugabyteDataType.TEXT };
            }

        },
        LENGTH(1, "length") {
            @Override
            public YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YugabyteConstant.createNullConstant();
                }
                String text = evaluatedArgs[0].asString();
                return YugabyteConstant.createIntConstant(text.length());
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.INT;
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return new YugabyteDataType[] { YugabyteDataType.TEXT };
            }
        },
        UPPER(1, "upper") {

            @Override
            public YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression... args) {
                if (evaluatedArgs[0].isNull()) {
                    return YugabyteConstant.createNullConstant();
                } else {
                    String text = evaluatedArgs[0].asString();
                    return YugabyteConstant.createTextConstant(text.toUpperCase());
                }
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.TEXT;
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return new YugabyteDataType[] { YugabyteDataType.TEXT };
            }

        },
        // NULL_IF(2, "nullif") {
        //
        // @Override
        // public YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression[] args) {
        // YugabyteConstant equals = evaluatedArgs[0].isEquals(evaluatedArgs[1]);
        // if (equals.isBoolean() && equals.asBoolean()) {
        // return YugabyteConstant.createNullConstant();
        // } else {
        // // TODO: SELECT (nullif('1', FALSE)); yields '1', but should yield TRUE
        // return evaluatedArgs[0];
        // }
        // }
        //
        // @Override
        // public boolean supportsReturnType(YugabyteDataType type) {
        // return true;
        // }
        //
        // @Override
        // public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
        // return getType(nrArguments, returnType);
        // }
        //
        // @Override
        // public boolean checkArguments(YugabyteExpression[] constants) {
        // for (YugabyteExpression e : constants) {
        // if (!(e instanceof YugabyteNullConstant)) {
        // return true;
        // }
        // }
        // return false;
        // }
        //
        // },
        NUM_NONNULLS(1, "num_nonnulls") {
            @Override
            public YugabyteConstant apply(YugabyteConstant[] args, YugabyteExpression... origArgs) {
                int nr = 0;
                for (YugabyteConstant c : args) {
                    if (!c.isNull()) {
                        nr++;
                    }
                }
                return YugabyteConstant.createIntConstant(nr);
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        },
        NUM_NULLS(1, "num_nulls") {
            @Override
            public YugabyteConstant apply(YugabyteConstant[] args, YugabyteExpression... origArgs) {
                int nr = 0;
                for (YugabyteConstant c : args) {
                    if (c.isNull()) {
                        nr++;
                    }
                }
                return YugabyteConstant.createIntConstant(nr);
            }

            @Override
            public YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments) {
                return getRandomTypes(nrArguments);
            }

            @Override
            public boolean supportsReturnType(YugabyteDataType type) {
                return type == YugabyteDataType.INT;
            }

            @Override
            public boolean isVariadic() {
                return true;
            }

        };

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        public YugabyteDataType[] getRandomTypes(int nr) {
            YugabyteDataType[] types = new YugabyteDataType[nr];
            for (int i = 0; i < types.length; i++) {
                types[i] = YugabyteDataType.getRandomType();
            }
            return types;
        }

        YugabyteFunctionWithResult(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract YugabyteConstant apply(YugabyteConstant[] evaluatedArgs, YugabyteExpression... args);

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }

        public abstract boolean supportsReturnType(YugabyteDataType type);

        public abstract YugabyteDataType[] getInputTypesForReturnType(YugabyteDataType returnType, int nrArguments);

        public boolean checkArguments(YugabyteExpression... constants) {
            return true;
        }

    }

    @Override
    public YugabyteConstant getExpectedValue() {
        if (functionWithKnownResult == null) {
            return null;
        }
        YugabyteConstant[] constants = new YugabyteConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i] == null) {
                return null;
            }
        }
        return functionWithKnownResult.apply(constants, args);
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return returnType;
    }

}
