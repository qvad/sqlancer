package sqlancer.yugabyte.ast;

import java.util.Collections;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;

public class YugabyteSelect extends SelectBase<YugabyteExpression> implements YugabyteExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<YugabyteJoin> joinClauses = Collections.emptyList();
    private YugabyteExpression distinctOnClause;
    private ForClause forClause;

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public SelectType getSelectOption() {
        return selectOption;
    }

    public void setSelectOption(SelectType fromOptions) {
        this.selectOption = fromOptions;
    }

    @Override
    public YugabyteDataType getExpressionType() {
        return null;
    }

    public List<YugabyteJoin> getJoinClauses() {
        return joinClauses;
    }

    public void setJoinClauses(List<YugabyteJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public YugabyteExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setDistinctOnClause(YugabyteExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public static ForClause getRandom() {
            return Randomly.fromOptions(values());
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }
    }

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public static class YugabyteFromTable implements YugabyteExpression {
        private final YugabyteTable t;
        private final boolean only;

        public YugabyteFromTable(YugabyteTable t, boolean only) {
            this.t = t;
            this.only = only;
        }

        public YugabyteTable getTable() {
            return t;
        }

        public boolean isOnly() {
            return only;
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return null;
        }
    }

    public static class YugabyteSubquery implements YugabyteExpression {
        private final YugabyteSelect s;
        private final String name;

        public YugabyteSubquery(YugabyteSelect s, String name) {
            this.s = s;
            this.name = name;
        }

        public YugabyteSelect getSelect() {
            return s;
        }

        public String getName() {
            return name;
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return null;
        }
    }

}
