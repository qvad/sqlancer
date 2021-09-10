package sqlancer.yugabyte.gen;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteRowValue;
import sqlancer.yugabyte.ast.YugabyteAggregate.YugabyteAggregateFunction;
import sqlancer.yugabyte.ast.YugabyteBinaryArithmeticOperation.YugabyteBinaryOperator;
import sqlancer.yugabyte.ast.YugabyteBinaryBitOperation.YugabyteBinaryBitOperator;
import sqlancer.yugabyte.ast.YugabyteBinaryLogicalOperation.BinaryLogicalOperator;
import sqlancer.yugabyte.ast.YugabyteBinaryRangeOperation.YugabyteBinaryRangeComparisonOperator;
import sqlancer.yugabyte.ast.YugabyteBinaryRangeOperation.YugabyteBinaryRangeOperator;
import sqlancer.yugabyte.ast.YugabyteFunction.YugabyteFunctionWithResult;
import sqlancer.yugabyte.ast.YugabyteOrderByTerm.YugabyteOrder;
import sqlancer.yugabyte.ast.YugabytePOSIXRegularExpression.POSIXRegex;
import sqlancer.yugabyte.ast.YugabytePostfixOperation.PostfixOperator;
import sqlancer.yugabyte.ast.YugabytePrefixOperation.PrefixOperator;
import sqlancer.yugabyte.YugabyteCompoundDataType;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteProvider;
import sqlancer.yugabyte.ast.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class YugabyteExpressionGenerator implements ExpressionGenerator<YugabyteExpression> {

    private final int maxDepth;

    private final Randomly r;

    private List<YugabyteColumn> columns;

    private YugabyteRowValue rw;

    private boolean expectedResult;

    private YugabyteGlobalState globalState;

    private boolean allowAggregateFunctions;

    private final Map<String, Character> functionsAndTypes;

    private final List<Character> allowedFunctionTypes;

    public YugabyteExpressionGenerator(YugabyteGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.maxDepth = globalState.getOptions().getMaxExpressionDepth();
        this.globalState = globalState;
        this.functionsAndTypes = globalState.getFunctionsAndTypes();
        this.allowedFunctionTypes = globalState.getAllowedFunctionTypes();
    }

    public YugabyteExpressionGenerator setColumns(List<YugabyteColumn> columns) {
        this.columns = columns;
        return this;
    }

    public YugabyteExpressionGenerator setRowValue(YugabyteRowValue rw) {
        this.rw = rw;
        return this;
    }

    public YugabyteExpression generateExpression(int depth) {
        return generateExpression(depth, YugabyteDataType.getRandomType());
    }

    public List<YugabyteExpression> generateOrderBy() {
        List<YugabyteExpression> orderBys = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            orderBys.add(new YugabyteOrderByTerm(YugabyteColumnValue.create(Randomly.fromList(columns), null),
                    YugabyteOrder.getRandomOrder()));
        }
        return orderBys;
    }

    private enum BooleanExpression {
        POSTFIX_OPERATOR, NOT, BINARY_LOGICAL_OPERATOR, BINARY_COMPARISON, FUNCTION, CAST, BETWEEN, IN_OPERATION,
        SIMILAR_TO, POSIX_REGEX, BINARY_RANGE_COMPARISON;
    }

    private YugabyteExpression generateFunctionWithUnknownResult(int depth, YugabyteDataType type) {
        List<YugabyteFunctionWithUnknownResult> supportedFunctions = YugabyteFunctionWithUnknownResult
                .getSupportedFunctions(type);
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        supportedFunctions = supportedFunctions.stream()
                .filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (supportedFunctions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YugabyteFunctionWithUnknownResult randomFunction = Randomly.fromList(supportedFunctions);
        return new YugabyteFunction(randomFunction, type, randomFunction.getArguments(type, this, depth + 1));
    }

    private YugabyteExpression generateFunctionWithKnownResult(int depth, YugabyteDataType type) {
        List<YugabyteFunctionWithResult> functions = Stream.of(YugabyteFunctionWithResult.values())
                .filter(f -> f.supportsReturnType(type)).collect(Collectors.toList());
        // filters functions by allowed type (STABLE 's', IMMUTABLE 'i', VOLATILE 'v')
        functions = functions.stream().filter(f -> allowedFunctionTypes.contains(functionsAndTypes.get(f.getName())))
                .collect(Collectors.toList());
        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }
        YugabyteFunctionWithResult randomFunction = Randomly.fromList(functions);
        int nrArgs = randomFunction.getNrArgs();
        if (randomFunction.isVariadic()) {
            nrArgs += Randomly.smallNumber();
        }
        YugabyteDataType[] argTypes = randomFunction.getInputTypesForReturnType(type, nrArgs);
        YugabyteExpression[] args = new YugabyteExpression[nrArgs];
        do {
            for (int i = 0; i < args.length; i++) {
                args[i] = generateExpression(depth + 1, argTypes[i]);
            }
        } while (!randomFunction.checkArguments(args));
        return new YugabyteFunction(randomFunction, type, args);
    }

    private YugabyteExpression generateBooleanExpression(int depth) {
        List<BooleanExpression> validOptions = new ArrayList<>(Arrays.asList(BooleanExpression.values()));
        if (YugabyteProvider.generateOnlyKnown) {
            validOptions.remove(BooleanExpression.SIMILAR_TO);
            validOptions.remove(BooleanExpression.POSIX_REGEX);
            validOptions.remove(BooleanExpression.BINARY_RANGE_COMPARISON);
        }
        BooleanExpression option = Randomly.fromList(validOptions);
        switch (option) {
        case POSTFIX_OPERATOR:
            PostfixOperator random = PostfixOperator.getRandom();
            return YugabytePostfixOperation
                    .create(generateExpression(depth + 1, Randomly.fromOptions(random.getInputDataTypes())), random);
        case IN_OPERATION:
            return inOperation(depth + 1);
        case NOT:
            return new YugabytePrefixOperation(generateExpression(depth + 1, YugabyteDataType.BOOLEAN),
                    PrefixOperator.NOT);
        case BINARY_LOGICAL_OPERATOR:
            YugabyteExpression first = generateExpression(depth + 1, YugabyteDataType.BOOLEAN);
            int nr = Randomly.smallNumber() + 1;
            for (int i = 0; i < nr; i++) {
                first = new YugabyteBinaryLogicalOperation(first,
                        generateExpression(depth + 1, YugabyteDataType.BOOLEAN), BinaryLogicalOperator.getRandom());
            }
            return first;
        case BINARY_COMPARISON:
            YugabyteDataType dataType = getMeaningfulType();
            return generateComparison(depth, dataType);
        case CAST:
            return new YugabyteCastOperation(generateExpression(depth + 1),
                    getCompoundDataType(YugabyteDataType.BOOLEAN));
        case FUNCTION:
            return generateFunction(depth + 1, YugabyteDataType.BOOLEAN);
        case BETWEEN:
            YugabyteDataType type = getMeaningfulType();
            return new YugabyteBetweenOperation(generateExpression(depth + 1, type),
                    generateExpression(depth + 1, type), generateExpression(depth + 1, type), Randomly.getBoolean());
        case SIMILAR_TO:
            assert !expectedResult;
            // TODO also generate the escape character
            return new YugabyteSimilarTo(generateExpression(depth + 1, YugabyteDataType.TEXT),
                    generateExpression(depth + 1, YugabyteDataType.TEXT), null);
        case POSIX_REGEX:
            assert !expectedResult;
            return new YugabytePOSIXRegularExpression(generateExpression(depth + 1, YugabyteDataType.TEXT),
                    generateExpression(depth + 1, YugabyteDataType.TEXT), POSIXRegex.getRandom());
        case BINARY_RANGE_COMPARISON:
            // TODO element check
            return new YugabyteBinaryRangeOperation(YugabyteBinaryRangeComparisonOperator.getRandom(),
                    generateExpression(depth + 1, YugabyteDataType.RANGE),
                    generateExpression(depth + 1, YugabyteDataType.RANGE));
        default:
            throw new AssertionError();
        }
    }

    private YugabyteDataType getMeaningfulType() {
        // make it more likely that the expression does not only consist of constant
        // expressions
        if (Randomly.getBooleanWithSmallProbability() || columns == null || columns.isEmpty()) {
            return YugabyteDataType.getRandomType();
        } else {
            return Randomly.fromList(columns).getType();
        }
    }

    private YugabyteExpression generateFunction(int depth, YugabyteDataType type) {
        if (YugabyteProvider.generateOnlyKnown || Randomly.getBoolean()) {
            return generateFunctionWithKnownResult(depth, type);
        } else {
            return generateFunctionWithUnknownResult(depth, type);
        }
    }

    private YugabyteExpression generateComparison(int depth, YugabyteDataType dataType) {
        YugabyteExpression leftExpr = generateExpression(depth + 1, dataType);
        YugabyteExpression rightExpr = generateExpression(depth + 1, dataType);
        return getComparison(leftExpr, rightExpr);
    }

    private YugabyteExpression getComparison(YugabyteExpression leftExpr, YugabyteExpression rightExpr) {
        YugabyteBinaryComparisonOperation op = new YugabyteBinaryComparisonOperation(leftExpr, rightExpr,
                YugabyteBinaryComparisonOperation.YugabyteBinaryComparisonOperator.getRandom());
        return op;
    }

    private YugabyteExpression inOperation(int depth) {
        YugabyteDataType type = YugabyteDataType.getRandomType();
        YugabyteExpression leftExpr = generateExpression(depth + 1, type);
        List<YugabyteExpression> rightExpr = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            rightExpr.add(generateExpression(depth + 1, type));
        }
        return new YugabyteInOperation(leftExpr, rightExpr, Randomly.getBoolean());
    }

    public static YugabyteExpression generateExpression(YugabyteGlobalState globalState, YugabyteDataType type) {
        return new YugabyteExpressionGenerator(globalState).generateExpression(0, type);
    }

    public YugabyteExpression generateExpression(int depth, YugabyteDataType originalType) {
        YugabyteDataType dataType = originalType;
        if (dataType == YugabyteDataType.REAL && Randomly.getBoolean()) {
            dataType = Randomly.fromOptions(YugabyteDataType.INT, YugabyteDataType.FLOAT);
        }
        if (dataType == YugabyteDataType.FLOAT && Randomly.getBoolean()) {
            dataType = YugabyteDataType.INT;
        }
        return generateExpressionInternal(depth, dataType);
    }

    private YugabyteExpression generateExpressionInternal(int depth, YugabyteDataType dataType) throws AssertionError {
        if (allowAggregateFunctions && Randomly.getBoolean()) {
            allowAggregateFunctions = false; // aggregate function calls cannot be nested
            return getAggregate(dataType);
        }
        if (Randomly.getBooleanWithRatherLowProbability() || depth > maxDepth) {
            // generic expression
            if (Randomly.getBoolean() || depth > maxDepth) {
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    return generateConstant(r, dataType);
                } else {
                    if (filterColumns(dataType).isEmpty()) {
                        return generateConstant(r, dataType);
                    } else {
                        return createColumnOfType(dataType);
                    }
                }
            } else {
                if (Randomly.getBoolean()) {
                    return new YugabyteCastOperation(generateExpression(depth + 1), getCompoundDataType(dataType));
                } else {
                    return generateFunctionWithUnknownResult(depth, dataType);
                }
            }
        } else {
            switch (dataType) {
            case BOOLEAN:
                return generateBooleanExpression(depth);
            case INT:
                return generateIntExpression(depth);
            case TEXT:
                return generateTextExpression(depth);
            case DECIMAL:
            case REAL:
            case FLOAT:
            case MONEY:
            case INET:
                return generateConstant(r, dataType);
            case BIT:
                return generateBitExpression(depth);
            case RANGE:
                return generateRangeExpression(depth);
            default:
                throw new AssertionError(dataType);
            }
        }
    }

    private static YugabyteCompoundDataType getCompoundDataType(YugabyteDataType type) {
        switch (type) {
        case BOOLEAN:
        case DECIMAL: // TODO
        case FLOAT:
        case INT:
        case MONEY:
        case RANGE:
        case REAL:
        case INET:
            return YugabyteCompoundDataType.create(type);
        case TEXT: // TODO
        case BIT:
            if (Randomly.getBoolean() || YugabyteProvider.generateOnlyKnown /*
                                                                             * The PQS implementation does not check for
                                                                             * size specifications
                                                                             */) {
                return YugabyteCompoundDataType.create(type);
            } else {
                return YugabyteCompoundDataType.create(type, (int) Randomly.getNotCachedInteger(1, 1000));
            }
        default:
            throw new AssertionError(type);
        }

    }

    private enum RangeExpression {
        BINARY_OP;
    }

    private YugabyteExpression generateRangeExpression(int depth) {
        RangeExpression option;
        List<RangeExpression> validOptions = new ArrayList<>(Arrays.asList(RangeExpression.values()));
        option = Randomly.fromList(validOptions);
        switch (option) {
        case BINARY_OP:
            return new YugabyteBinaryRangeOperation(YugabyteBinaryRangeOperator.getRandom(),
                    generateExpression(depth + 1, YugabyteDataType.RANGE),
                    generateExpression(depth + 1, YugabyteDataType.RANGE));
        default:
            throw new AssertionError(option);
        }
    }

    private enum TextExpression {
        CAST, FUNCTION, CONCAT
    }

    private YugabyteExpression generateTextExpression(int depth) {
        TextExpression option;
        List<TextExpression> validOptions = new ArrayList<>(Arrays.asList(TextExpression.values()));
        option = Randomly.fromList(validOptions);

        switch (option) {
        case CAST:
            return new YugabyteCastOperation(generateExpression(depth + 1), getCompoundDataType(YugabyteDataType.TEXT));
        case FUNCTION:
            return generateFunction(depth + 1, YugabyteDataType.TEXT);
        case CONCAT:
            return generateConcat(depth);
        default:
            throw new AssertionError();
        }
    }

    private YugabyteExpression generateConcat(int depth) {
        YugabyteExpression left = generateExpression(depth + 1, YugabyteDataType.TEXT);
        YugabyteExpression right = generateExpression(depth + 1);
        return new YugabyteConcatOperation(left, right);
    }

    private enum BitExpression {
        BINARY_OPERATION
    };

    private YugabyteExpression generateBitExpression(int depth) {
        BitExpression option;
        option = Randomly.fromOptions(BitExpression.values());
        switch (option) {
        case BINARY_OPERATION:
            return new YugabyteBinaryBitOperation(YugabyteBinaryBitOperator.getRandom(),
                    generateExpression(depth + 1, YugabyteDataType.BIT),
                    generateExpression(depth + 1, YugabyteDataType.BIT));
        default:
            throw new AssertionError();
        }
    }

    private enum IntExpression {
        UNARY_OPERATION, FUNCTION, CAST, BINARY_ARITHMETIC_EXPRESSION
    }

    private YugabyteExpression generateIntExpression(int depth) {
        IntExpression option;
        option = Randomly.fromOptions(IntExpression.values());
        switch (option) {
        case CAST:
            return new YugabyteCastOperation(generateExpression(depth + 1), getCompoundDataType(YugabyteDataType.INT));
        case UNARY_OPERATION:
            YugabyteExpression intExpression = generateExpression(depth + 1, YugabyteDataType.INT);
            return new YugabytePrefixOperation(intExpression,
                    Randomly.getBoolean() ? PrefixOperator.UNARY_PLUS : PrefixOperator.UNARY_MINUS);
        case FUNCTION:
            return generateFunction(depth + 1, YugabyteDataType.INT);
        case BINARY_ARITHMETIC_EXPRESSION:
            return new YugabyteBinaryArithmeticOperation(generateExpression(depth + 1, YugabyteDataType.INT),
                    generateExpression(depth + 1, YugabyteDataType.INT), YugabyteBinaryOperator.getRandom());
        default:
            throw new AssertionError();
        }
    }

    private YugabyteExpression createColumnOfType(YugabyteDataType type) {
        List<YugabyteColumn> columns = filterColumns(type);
        YugabyteColumn fromList = Randomly.fromList(columns);
        YugabyteConstant value = rw == null ? null : rw.getValues().get(fromList);
        return YugabyteColumnValue.create(fromList, value);
    }

    final List<YugabyteColumn> filterColumns(YugabyteDataType type) {
        if (columns == null) {
            return Collections.emptyList();
        } else {
            return columns.stream().filter(c -> c.getType() == type).collect(Collectors.toList());
        }
    }

    public YugabyteExpression generateExpressionWithExpectedResult(YugabyteDataType type) {
        this.expectedResult = true;
        YugabyteExpressionGenerator gen = new YugabyteExpressionGenerator(globalState).setColumns(columns)
                .setRowValue(rw);
        YugabyteExpression expr;
        do {
            expr = gen.generateExpression(type);
        } while (expr.getExpectedValue() == null);
        return expr;
    }

    public static YugabyteExpression generateConstant(Randomly r, YugabyteDataType type) {
        if (Randomly.getBooleanWithSmallProbability()) {
            return YugabyteConstant.createNullConstant();
        }
        // if (Randomly.getBooleanWithSmallProbability()) {
        // return YugabyteConstant.createTextConstant(r.getString());
        // }
        switch (type) {
        case INT:
            if (Randomly.getBooleanWithSmallProbability()) {
                return YugabyteConstant.createTextConstant(String.valueOf(r.getInteger()));
            } else {
                return YugabyteConstant.createIntConstant(r.getInteger());
            }
        case BOOLEAN:
            if (Randomly.getBooleanWithSmallProbability() && !YugabyteProvider.generateOnlyKnown) {
                return YugabyteConstant
                        .createTextConstant(Randomly.fromOptions("TR", "TRUE", "FA", "FALSE", "0", "1", "ON", "off"));
            } else {
                return YugabyteConstant.createBooleanConstant(Randomly.getBoolean());
            }
        case TEXT:
            return YugabyteConstant.createTextConstant(r.getString());
        case DECIMAL:
            return YugabyteConstant.createDecimalConstant(r.getRandomBigDecimal());
        case FLOAT:
            return YugabyteConstant.createFloatConstant((float) r.getDouble());
        case REAL:
            return YugabyteConstant.createDoubleConstant(r.getDouble());
        case RANGE:
            return YugabyteConstant.createRange(r.getInteger(), Randomly.getBoolean(), r.getInteger(),
                    Randomly.getBoolean());
        case MONEY:
            return new YugabyteCastOperation(generateConstant(r, YugabyteDataType.FLOAT),
                    getCompoundDataType(YugabyteDataType.MONEY));
        case INET:
            return YugabyteConstant.createInetConstant(getRandomInet(r));
        case BIT:
            return YugabyteConstant.createBitConstant(r.getInteger());
        default:
            throw new AssertionError(type);
        }
    }

    private static String getRandomInet(Randomly r) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append('.');
            }
            sb.append(r.getInteger() & 255);
        }
        return sb.toString();
    }

    public static YugabyteExpression generateExpression(YugabyteGlobalState globalState, List<YugabyteColumn> columns,
                                                        YugabyteDataType type) {
        return new YugabyteExpressionGenerator(globalState).setColumns(columns).generateExpression(0, type);
    }

    public static YugabyteExpression generateExpression(YugabyteGlobalState globalState, List<YugabyteColumn> columns) {
        return new YugabyteExpressionGenerator(globalState).setColumns(columns).generateExpression(0);

    }

    public List<YugabyteExpression> generateExpressions(int nr) {
        List<YugabyteExpression> expressions = new ArrayList<>();
        for (int i = 0; i < nr; i++) {
            expressions.add(generateExpression(0));
        }
        return expressions;
    }

    public YugabyteExpression generateExpression(YugabyteDataType dataType) {
        return generateExpression(0, dataType);
    }

    public YugabyteExpressionGenerator setGlobalState(YugabyteGlobalState globalState) {
        this.globalState = globalState;
        return this;
    }

    public YugabyteExpression generateHavingClause() {
        this.allowAggregateFunctions = true;
        YugabyteExpression expression = generateExpression(YugabyteDataType.BOOLEAN);
        this.allowAggregateFunctions = false;
        return expression;
    }

    public YugabyteExpression generateAggregate() {
        return getAggregate(YugabyteDataType.getRandomType());
    }

    private YugabyteExpression getAggregate(YugabyteDataType dataType) {
        List<YugabyteAggregateFunction> aggregates = YugabyteAggregateFunction.getAggregates(dataType);
        YugabyteAggregateFunction agg = Randomly.fromList(aggregates);
        return generateArgsForAggregate(dataType, agg);
    }

    public YugabyteAggregate generateArgsForAggregate(YugabyteDataType dataType, YugabyteAggregateFunction agg) {
        List<YugabyteDataType> types = agg.getTypes(dataType);
        List<YugabyteExpression> args = new ArrayList<>();
        for (YugabyteDataType argType : types) {
            args.add(generateExpression(argType));
        }
        return new YugabyteAggregate(args, agg);
    }

    public YugabyteExpressionGenerator allowAggregates(boolean value) {
        allowAggregateFunctions = value;
        return this;
    }

    @Override
    public YugabyteExpression generatePredicate() {
        return generateExpression(YugabyteDataType.BOOLEAN);
    }

    @Override
    public YugabyteExpression negatePredicate(YugabyteExpression predicate) {
        return new YugabytePrefixOperation(predicate, PrefixOperator.NOT);
    }

    @Override
    public YugabyteExpression isNull(YugabyteExpression expr) {
        return new YugabytePostfixOperation(expr, PostfixOperator.IS_NULL);
    }

}
