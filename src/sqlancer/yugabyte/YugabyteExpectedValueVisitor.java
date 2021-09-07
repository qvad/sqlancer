package sqlancer.yugabyte;

import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteFromTable;
import sqlancer.yugabyte.ast.YugabyteSelect.YugabyteSubquery;
import sqlancer.yugabyte.ast.*;

public final class YugabyteExpectedValueVisitor implements YugabyteVisitor {

    private final StringBuilder sb = new StringBuilder();
    private static final int NR_TABS = 0;

    private void print(YugabyteExpression expr) {
        YugabyteToStringVisitor v = new YugabyteToStringVisitor();
        v.visit(expr);
        for (int i = 0; i < NR_TABS; i++) {
            sb.append("\t");
        }
        sb.append(v.get());
        sb.append(" -- ");
        sb.append(expr.getExpectedValue());
        sb.append("\n");
    }

    // @Override
    // public void visit(YugabyteExpression expr) {
    // nrTabs++;
    // try {
    // super.visit(expr);
    // } catch (IgnoreMeException e) {
    //
    // }
    // nrTabs--;
    // }

    @Override
    public void visit(YugabyteConstant constant) {
        print(constant);
    }

    @Override
    public void visit(YugabytePostfixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    public String get() {
        return sb.toString();
    }

    @Override
    public void visit(YugabyteColumnValue c) {
        print(c);
    }

    @Override
    public void visit(YugabytePrefixOperation op) {
        print(op);
        visit(op.getExpression());
    }

    @Override
    public void visit(YugabyteSelect op) {
        visit(op.getWhereClause());
    }

    @Override
    public void visit(YugabyteOrderByTerm op) {

    }

    @Override
    public void visit(YugabyteFunction f) {
        print(f);
        for (int i = 0; i < f.getArguments().length; i++) {
            visit(f.getArguments()[i]);
        }
    }

    @Override
    public void visit(YugabyteCastOperation cast) {
        print(cast);
        visit(cast.getExpression());
    }

    @Override
    public void visit(YugabyteBetweenOperation op) {
        print(op);
        visit(op.getExpr());
        visit(op.getLeft());
        visit(op.getRight());
    }

    @Override
    public void visit(YugabyteInOperation op) {
        print(op);
        visit(op.getExpr());
        for (YugabyteExpression right : op.getListElements()) {
            visit(right);
        }
    }

    @Override
    public void visit(YugabytePostfixText op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(YugabyteAggregate op) {
        print(op);
        for (YugabyteExpression expr : op.getArgs()) {
            visit(expr);
        }
    }

    @Override
    public void visit(YugabyteSimilarTo op) {
        print(op);
        visit(op.getString());
        visit(op.getSimilarTo());
        if (op.getEscapeCharacter() != null) {
            visit(op.getEscapeCharacter());
        }
    }

    @Override
    public void visit(YugabytePOSIXRegularExpression op) {
        print(op);
        visit(op.getString());
        visit(op.getRegex());
    }

    @Override
    public void visit(YugabyteCollate op) {
        print(op);
        visit(op.getExpr());
    }

    @Override
    public void visit(YugabyteFromTable from) {
        print(from);
    }

    @Override
    public void visit(YugabyteSubquery subquery) {
        print(subquery);
    }

    @Override
    public void visit(YugabyteBinaryLogicalOperation op) {
        print(op);
        visit(op.getLeft());
        visit(op.getRight());
    }

}
