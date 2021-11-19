package sqlancer.yugabyte.ast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteAggregate.YugabyteAggregateFunction;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class YugabyteAggregate extends FunctionNode<YugabyteAggregateFunction, YugabyteExpression>
        implements YugabyteExpression {

    public YugabyteAggregate(List<YugabyteExpression> args, YugabyteAggregateFunction func) {
        super(func, args);
    }

    public enum YugabyteAggregateFunction {
        AVG(YugabyteDataType.INT, YugabyteDataType.FLOAT, YugabyteDataType.REAL, YugabyteDataType.DECIMAL),
        BIT_AND(YugabyteDataType.INT), BIT_OR(YugabyteDataType.INT), BOOL_AND(YugabyteDataType.BOOLEAN),
        BOOL_OR(YugabyteDataType.BOOLEAN), COUNT(YugabyteDataType.INT), EVERY(YugabyteDataType.BOOLEAN), MAX, MIN,
        // STRING_AGG
        SUM(YugabyteDataType.INT, YugabyteDataType.FLOAT, YugabyteDataType.REAL, YugabyteDataType.DECIMAL);

        private final YugabyteDataType[] supportedReturnTypes;

        YugabyteAggregateFunction(YugabyteDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public static List<YugabyteAggregateFunction> getAggregates(YugabyteDataType type) {
            return Arrays.stream(values()).filter(p -> p.supportsReturnType(type)).collect(Collectors.toList());
        }

        public List<YugabyteDataType> getTypes(YugabyteDataType returnType) {
            return Collections.singletonList(returnType);
        }

        public boolean supportsReturnType(YugabyteDataType returnType) {
            return Arrays.stream(supportedReturnTypes).anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public YugabyteDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(YugabyteDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

}
