package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.FunctionNode;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteAggregate.YugabyteAggregateFunction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @see <a href="https://www.sqlite.org/lang_aggfunc.html">Built-in Aggregate Functions</a>
 */
public class YugabyteAggregate extends FunctionNode<YugabyteAggregateFunction, YugabyteExpression>
        implements YugabyteExpression {

    public enum YugabyteAggregateFunction {
        AVG(YugabyteDataType.INT, YugabyteDataType.FLOAT, YugabyteDataType.REAL, YugabyteDataType.DECIMAL),
        BIT_AND(YugabyteDataType.INT), BIT_OR(YugabyteDataType.INT), BOOL_AND(YugabyteDataType.BOOLEAN),
        BOOL_OR(YugabyteDataType.BOOLEAN), COUNT(YugabyteDataType.INT), EVERY(YugabyteDataType.BOOLEAN), MAX, MIN,
        // STRING_AGG
        SUM(YugabyteDataType.INT, YugabyteDataType.FLOAT, YugabyteDataType.REAL, YugabyteDataType.DECIMAL);

        private YugabyteDataType[] supportedReturnTypes;

        YugabyteAggregateFunction(YugabyteDataType... supportedReturnTypes) {
            this.supportedReturnTypes = supportedReturnTypes.clone();
        }

        public List<YugabyteDataType> getTypes(YugabyteDataType returnType) {
            return Arrays.asList(returnType);
        }

        public boolean supportsReturnType(YugabyteDataType returnType) {
            return Arrays.asList(supportedReturnTypes).stream().anyMatch(t -> t == returnType)
                    || supportedReturnTypes.length == 0;
        }

        public static List<YugabyteAggregateFunction> getAggregates(YugabyteDataType type) {
            return Arrays.asList(values()).stream().filter(p -> p.supportsReturnType(type))
                    .collect(Collectors.toList());
        }

        public YugabyteDataType getRandomReturnType() {
            if (supportedReturnTypes.length == 0) {
                return Randomly.fromOptions(YugabyteDataType.getRandomType());
            } else {
                return Randomly.fromOptions(supportedReturnTypes);
            }
        }

    }

    public YugabyteAggregate(List<YugabyteExpression> args, YugabyteAggregateFunction func) {
        super(func, args);
    }

}
