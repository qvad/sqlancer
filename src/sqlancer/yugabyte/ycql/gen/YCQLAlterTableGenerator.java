package sqlancer.yugabyte.ycql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.yugabyte.ycql.YCQLErrors;
import sqlancer.yugabyte.ycql.YCQLProvider.YCQLGlobalState;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLCompositeDataType;
import sqlancer.yugabyte.ycql.YCQLSchema.YCQLTable;
import sqlancer.yugabyte.ycql.YCQLToStringVisitor;

public final class YCQLAlterTableGenerator {

    private YCQLAlterTableGenerator() {
    }

    enum Action {
        ADD_COLUMN, ALTER_COLUMN, DROP_COLUMN
    }

    public static SQLQueryAdapter getQuery(YCQLGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add(" does not have a column with name \"rowid\"");
        errors.add("Table does not contain column rowid referenced in alter statement");
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        YCQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        YCQLExpressionGenerator gen = new YCQLExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append(table.getName());
        sb.append(" ");
        Action action = Randomly.fromOptions(Action.values());
        switch (action) {
        case ADD_COLUMN:
            sb.append("ADD COLUMN ");
            String columnName = table.getFreeColumnName();
            sb.append(columnName);
            sb.append(" ");
            sb.append(YCQLCompositeDataType.getRandom().toString());
            break;
        case ALTER_COLUMN:
            sb.append("ALTER COLUMN ");
            sb.append(table.getRandomColumn().getName());
            sb.append(" SET DATA TYPE ");
            sb.append(YCQLCompositeDataType.getRandom().toString());
            if (Randomly.getBoolean()) {
                sb.append(" USING ");
                YCQLErrors.addExpressionErrors(errors);
                sb.append(YCQLToStringVisitor.asString(gen.generateExpression()));
            }
            errors.add("Cannot change the type of this column: an index depends on it!");
            errors.add("Cannot change the type of a column that has a UNIQUE or PRIMARY KEY constraint specified");
            errors.add("Unimplemented type for cast");
            errors.add("Conversion:");
            errors.add("Cannot change the type of a column that has a CHECK constraint specified");
            break;
        case DROP_COLUMN:
            sb.append("DROP COLUMN ");
            sb.append(table.getRandomColumn().getName());
            errors.add("named in key does not exist"); // TODO
            errors.add("Cannot drop this column:");
            errors.add("Cannot drop column: table only has one column remaining!");
            errors.add("because there is a CHECK constraint that depends on it");
            errors.add("because there is a UNIQUE constraint that depends on it");
            break;
        default:
            throw new AssertionError(action);
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
