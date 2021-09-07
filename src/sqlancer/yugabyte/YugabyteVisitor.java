package sqlancer.yugabyte;

import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteSubquery;
import sqlancer.yugabyte.ast.*;
import sqlancer.yugabyte.gen.YugabyteExpressionGenerator;

import java.util.List;

public interface YugabyteVisitor {

    void visit(YugabyteConstant constant);

    void visit(YugabytePostfixOperation op);

    void visit(YugabyteColumnValue c);

    void visit(YugabytePrefixOperation op);

    void visit(YugabyteSelect op);

    void visit(YugabyteOrderByTerm op);

    void visit(YugabyteFunction f);

    void visit(YugabyteCastOperation cast);

    void visit(YugabyteBetweenOperation op);

    void visit(YugabyteInOperation op);

    void visit(YugabytePostfixText op);

    void visit(YugabyteAggregate op);

    void visit(YugabyteSimilarTo op);

    void visit(YugabyteCollate op);

    void visit(YugabytePOSIXRegularExpression op);

    void visit(YugabyteFromTable from);

    void visit(YugabyteSubquery subquery);

    void visit(YugabyteBinaryLogicalOperation op);

    default void visit(YugabyteExpression expression) {
        if (expression instanceof YugabyteConstant) {
            visit((YugabyteConstant) expression);
        } else if (expression instanceof YugabytePostfixOperation) {
            visit((YugabytePostfixOperation) expression);
        } else if (expression instanceof YugabyteColumnValue) {
            visit((YugabyteColumnValue) expression);
        } else if (expression instanceof YugabytePrefixOperation) {
            visit((YugabytePrefixOperation) expression);
        } else if (expression instanceof YugabyteSelect) {
            visit((YugabyteSelect) expression);
        } else if (expression instanceof YugabyteOrderByTerm) {
            visit((YugabyteOrderByTerm) expression);
        } else if (expression instanceof YugabyteFunction) {
            visit((YugabyteFunction) expression);
        } else if (expression instanceof YugabyteCastOperation) {
            visit((YugabyteCastOperation) expression);
        } else if (expression instanceof YugabyteBetweenOperation) {
            visit((YugabyteBetweenOperation) expression);
        } else if (expression instanceof YugabyteInOperation) {
            visit((YugabyteInOperation) expression);
        } else if (expression instanceof YugabyteAggregate) {
            visit((YugabyteAggregate) expression);
        } else if (expression instanceof YugabytePostfixText) {
            visit((YugabytePostfixText) expression);
        } else if (expression instanceof YugabyteSimilarTo) {
            visit((YugabyteSimilarTo) expression);
        } else if (expression instanceof YugabytePOSIXRegularExpression) {
            visit((YugabytePOSIXRegularExpression) expression);
        } else if (expression instanceof YugabyteCollate) {
            visit((YugabyteCollate) expression);
        } else if (expression instanceof YugabyteFromTable) {
            visit((YugabyteFromTable) expression);
        } else if (expression instanceof YugabyteSubquery) {
            visit((YugabyteSubquery) expression);
        } else {
            throw new AssertionError(expression);
        }
    }

    static String asString(YugabyteExpression expr) {
        YugabyteToStringVisitor visitor = new YugabyteToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }

    static String asExpectedValues(YugabyteExpression expr) {
        YugabyteExpectedValueVisitor v = new YugabyteExpectedValueVisitor();
        v.visit(expr);
        return v.get();
    }

    static String getExpressionAsString(YugabyteGlobalState globalState, YugabyteDataType type,
                                        List<YugabyteColumn> columns) {
        YugabyteExpression expression = YugabyteExpressionGenerator.generateExpression(globalState, columns, type);
        YugabyteToStringVisitor visitor = new YugabyteToStringVisitor();
        visitor.visit(expression);
        return visitor.get();
    }

}
