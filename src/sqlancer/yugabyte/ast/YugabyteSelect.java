package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.SelectBase;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;

import java.util.Collections;
import java.util.List;

public class YugabyteSelect extends SelectBase<YugabyteExpression> implements YugabyteExpression {

    private SelectType selectOption = SelectType.ALL;
    private List<YugabyteJoin> joinClauses = Collections.emptyList();
    private YugabyteExpression distinctOnClause;
    private ForClause forClause;

    public enum ForClause {
        UPDATE("UPDATE"), NO_KEY_UPDATE("NO KEY UPDATE"), SHARE("SHARE"), KEY_SHARE("KEY SHARE");

        private final String textRepresentation;

        ForClause(String textRepresentation) {
            this.textRepresentation = textRepresentation;
        }

        public String getTextRepresentation() {
            return textRepresentation;
        }

        public static ForClause getRandom() {
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

    public enum SelectType {
        DISTINCT, ALL;

        public static SelectType getRandom() {
            return Randomly.fromOptions(values());
        }
    }

    public void setSelectType(SelectType fromOptions) {
        this.setSelectOption(fromOptions);
    }

    public void setDistinctOnClause(YugabyteExpression distinctOnClause) {
        if (selectOption != SelectType.DISTINCT) {
            throw new IllegalArgumentException();
        }
        this.distinctOnClause = distinctOnClause;
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

    public void setJoinClauses(List<YugabyteJoin> joinStatements) {
        this.joinClauses = joinStatements;

    }

    public List<YugabyteJoin> getJoinClauses() {
        return joinClauses;
    }

    public YugabyteExpression getDistinctOnClause() {
        return distinctOnClause;
    }

    public void setForClause(ForClause forClause) {
        this.forClause = forClause;
    }

    public ForClause getForClause() {
        return forClause;
    }

}
