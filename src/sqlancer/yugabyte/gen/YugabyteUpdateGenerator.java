package sqlancer.yugabyte.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.YugabyteSchema.YugabyteColumn;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.YugabyteGlobalState;
import sqlancer.yugabyte.YugabyteVisitor;
import sqlancer.yugabyte.ast.YugabyteExpression;

import java.util.List;

public final class YugabyteUpdateGenerator {

    private YugabyteUpdateGenerator() {
    }

    public static SQLQueryAdapter create(YugabyteGlobalState globalState) {
        YugabyteTable randomTable = globalState.getSchema().getRandomTable(t -> t.isInsertable());
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(randomTable.getName());
        sb.append(" SET ");
        ExpectedErrors errors = ExpectedErrors.from("conflicting key value violates exclusion constraint",
                "reached maximum value of sequence", "violates foreign key constraint", "violates not-null constraint",
                "violates unique constraint", "out of range", "cannot cast", "must be type boolean", "is not unique",
                " bit string too long", "can only be updated to DEFAULT", "division by zero",
                "You might need to add explicit type casts.", "invalid regular expression",
                "View columns that are not columns of their base relation are not updatable");
        errors.add("multiple assignments to same column"); // view whose columns refer to a column in the referenced
                                                           // table multiple times
        errors.add("new row violates check option for view");
        List<YugabyteColumn> columns = randomTable.getRandomNonEmptyColumnSubset();
        YugabyteCommon.addCommonInsertUpdateErrors(errors);

        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            YugabyteColumn column = columns.get(i);
            sb.append(column.getName());
            sb.append(" = ");
            if (!Randomly.getBoolean()) {
                YugabyteExpression constant = YugabyteExpressionGenerator.generateConstant(globalState.getRandomly(),
                        column.getType());
                sb.append(YugabyteVisitor.asString(constant));
            } else if (Randomly.getBoolean()) {
                sb.append("DEFAULT");
            } else {
                sb.append("(");
                YugabyteExpression expr = YugabyteExpressionGenerator.generateExpression(globalState,
                        randomTable.getColumns(), column.getType());
                // caused by casts
                sb.append(YugabyteVisitor.asString(expr));
                sb.append(")");
            }
        }
        errors.add("invalid input syntax for ");
        errors.add("operator does not exist: text = boolean");
        errors.add("violates check constraint");
        errors.add("could not determine which collation to use for string comparison");
        errors.add("but expression is of type");
        YugabyteCommon.addCommonExpressionErrors(errors);
        if (!Randomly.getBooleanWithSmallProbability()) {
            sb.append(" WHERE ");
            YugabyteExpression where = YugabyteExpressionGenerator.generateExpression(globalState,
                    randomTable.getColumns(), YugabyteDataType.BOOLEAN);
            sb.append(YugabyteVisitor.asString(where));
        }

        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
