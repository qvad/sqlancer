package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabytePOSIXRegularExpression implements YugabyteExpression {

    private final YugabyteExpression string;
    private final YugabyteExpression regex;
    private final POSIXRegex op;

    public YugabytePOSIXRegularExpression(YugabyteExpression string, YugabyteExpression regex, POSIXRegex op) {
        this.string = string;
        this.regex = regex;
        this.op = op;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return null;
    }

    public YugabyteExpression getRegex() {
        return regex;
    }

    public YugabyteExpression getString() {
        return string;
    }

    public POSIXRegex getOp() {
        return op;
    }

    public enum POSIXRegex implements Operator {
        MATCH_CASE_SENSITIVE("~"), MATCH_CASE_INSENSITIVE("~*"), NOT_MATCH_CASE_SENSITIVE("!~"),
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private final String repr;

        POSIXRegex(String repr) {
            this.repr = repr;
        }

        public static POSIXRegex getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getStringRepresentation() {
            return repr;
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

}
