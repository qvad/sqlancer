package sqlancer.yugabyte.ast;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

public class YugabyteSimilarTo implements YugabyteExpression {

    private final YugabyteExpression string;
    private final YugabyteExpression similarTo;
    private final YugabyteExpression escapeCharacter;

    public YugabyteSimilarTo(YugabyteExpression string, YugabyteExpression similarTo,
            YugabyteExpression escapeCharacter) {
        this.string = string;
        this.similarTo = similarTo;
        this.escapeCharacter = escapeCharacter;
    }

    public YugabyteExpression getString() {
        return string;
    }

    public YugabyteExpression getSimilarTo() {
        return similarTo;
    }

    public YugabyteExpression getEscapeCharacter() {
        return escapeCharacter;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return YugabyteDataType.BOOLEAN;
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return null;
    }

}
