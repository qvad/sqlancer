package sqlancer.common.gen;

import java.util.List;

public abstract class AbstractInsertGenerator<C> {

    protected StringBuilder sb = new StringBuilder();

    protected void insertColumns(List<C> columns) {
        // TODO MULTIPLE VALUES ARE NOT SUPPORTED YET
        sb.append("(");
        for (int nrColumn = 0; nrColumn < columns.size(); nrColumn++) {
            if (nrColumn != 0) {
                sb.append(", ");
            }
            insertValue(columns.get(nrColumn));
        }
        sb.append(")");
    }

    protected abstract void insertValue(C tiDBColumn);

}
