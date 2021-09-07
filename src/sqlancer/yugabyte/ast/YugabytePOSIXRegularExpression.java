package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabytePOSIXRegularExpression implements YugabyteExpression {

    private YugabyteExpression string;
    private YugabyteExpression regex;
    private POSIXRegex op;

    public enum POSIXRegex implements Operator {
        MATCH_CASE_SENSITIVE("~"), MATCH_CASE_INSENSITIVE("~*"), NOT_MATCH_CASE_SENSITIVE("!~"),
        NOT_MATCH_CASE_INSENSITIVE("!~*");

        private String repr;

        POSIXRegex(String repr) {
            this.repr = repr;
        }

        public String getStringRepresentation() {
            return repr;
        }

        public static POSIXRegex getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return toString();
        }
    }

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

}
