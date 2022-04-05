package sqlancer.yugabyte;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PSQLException;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.yugabyte.YugabyteSchema.YugabyteTable;
import sqlancer.yugabyte.ast.YugabyteConstant;

public class YugabyteSchema extends AbstractSchema<YugabyteGlobalState, YugabyteTable> {

    private final String databaseName;

    public YugabyteSchema(List<YugabyteTable> databaseTables, String databaseName) {
        super(databaseTables);
        this.databaseName = databaseName;
    }

    public static YugabyteDataType getColumnType(String typeString) {
        switch (typeString) {
        case "smallint":
        case "integer":
        case "bigint":
            return YugabyteDataType.INT;
        case "boolean":
            return YugabyteDataType.BOOLEAN;
        case "text":
        case "character":
        case "character varying":
        case "name":
            return YugabyteDataType.TEXT;
        case "numeric":
            return YugabyteDataType.DECIMAL;
        case "double precision":
            return YugabyteDataType.FLOAT;
        case "real":
            return YugabyteDataType.REAL;
        case "int4range":
            return YugabyteDataType.RANGE;
        case "money":
            return YugabyteDataType.MONEY;
        case "bytea":
            return YugabyteDataType.BYTEA;
        case "bit":
        case "bit varying":
            return YugabyteDataType.BIT;
        case "inet":
            return YugabyteDataType.INET;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static YugabyteSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        try {
            List<YugabyteTable> databaseTables = new ArrayList<>();
            try (Statement s = con.createStatement()) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT table_name, table_schema, table_type, is_insertable_into FROM information_schema.tables WHERE table_schema='public' OR table_schema LIKE 'pg_temp_%' ORDER BY table_name;")) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String tableTypeSchema = rs.getString("table_schema");
                        boolean isInsertable = rs.getBoolean("is_insertable_into");
                        // TODO: also check insertable
                        // TODO: insert into view?
                        boolean isView = tableName.startsWith("v"); // tableTypeStr.contains("VIEW") ||
                        // tableTypeStr.contains("LOCAL TEMPORARY") &&
                        // !isInsertable;
                        YugabyteTable.TableType tableType = getTableType(tableTypeSchema);
                        List<YugabyteColumn> databaseColumns = getTableColumns(con, tableName);
                        List<YugabyteIndex> indexes = getIndexes(con, tableName);
                        List<YugabyteStatisticsObject> statistics = getStatistics(con);
                        YugabyteTable t = new YugabyteTable(tableName, databaseColumns, indexes, tableType, statistics,
                                isView, isInsertable);
                        for (YugabyteColumn c : databaseColumns) {
                            c.setTable(t);
                        }
                        databaseTables.add(t);
                    }
                }
            }
            return new YugabyteSchema(databaseTables, databaseName);
        } catch (SQLIntegrityConstraintViolationException e) {
            throw new AssertionError(e);
        }
    }

    protected static List<YugabyteStatisticsObject> getStatistics(SQLConnection con) throws SQLException {
        List<YugabyteStatisticsObject> statistics = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT stxname FROM pg_statistic_ext ORDER BY stxname;")) {
                while (rs.next()) {
                    statistics.add(new YugabyteStatisticsObject(rs.getString("stxname")));
                }
            }
        }
        return statistics;
    }

    protected static YugabyteTable.TableType getTableType(String tableTypeStr) throws AssertionError {
        YugabyteTable.TableType tableType;
        if (tableTypeStr.contentEquals("public")) {
            tableType = YugabyteTable.TableType.STANDARD;
        } else if (tableTypeStr.startsWith("pg_temp")) {
            tableType = YugabyteTable.TableType.TEMPORARY;
        } else {
            throw new AssertionError(tableTypeStr);
        }
        return tableType;
    }

    protected static List<YugabyteIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<YugabyteIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String
                    .format("SELECT indexname FROM pg_indexes WHERE tablename='%s' ORDER BY indexname;", tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("indexname");
                    if (DBMSCommon.matchesIndexName(indexName)) {
                        indexes.add(YugabyteIndex.create(indexName));
                    }
                }
            }
        }
        return indexes;
    }

    protected static List<YugabyteColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<YugabyteColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s
                    .executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"
                            + tableName + "' ORDER BY column_name")) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    YugabyteColumn c = new YugabyteColumn(columnName, getColumnType(dataType));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public YugabyteTables getRandomTableNonEmptyTables() {
        return new YugabyteTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public enum YugabyteDataType {
        // TODO: 23.02.2022 Planned types
        // SMALLINT, INT, BIGINT, NUMERIC, DECIMAL, REAL, DOUBLE_PRECISION, VARCHAR, CHAR, TEXT, DATE, TIME,
        // TIMESTAMP, TIMESTAMPZ, INTERVAL, INTEGER_ARR
        INT, BOOLEAN, BYTEA, VARCHAR, CHAR, TEXT, DECIMAL, FLOAT, REAL, RANGE, MONEY, BIT, INET;

        public static YugabyteDataType getRandomType() {
            List<YugabyteDataType> dataTypes = new ArrayList<>(Arrays.asList(values()));
            if (YugabyteProvider.generateOnlyKnown) {
                dataTypes.remove(YugabyteDataType.DECIMAL);
                dataTypes.remove(YugabyteDataType.FLOAT);
                dataTypes.remove(YugabyteDataType.REAL);
                dataTypes.remove(YugabyteDataType.INET);
                dataTypes.remove(YugabyteDataType.RANGE);
                dataTypes.remove(YugabyteDataType.MONEY);
                dataTypes.remove(YugabyteDataType.BIT);
            }
            return Randomly.fromList(dataTypes);
        }
    }

    public static class YugabyteColumn extends AbstractTableColumn<YugabyteTable, YugabyteDataType> {

        public YugabyteColumn(String name, YugabyteDataType columnType) {
            super(name, null, columnType);
        }

        public static YugabyteColumn createDummy(String name) {
            return new YugabyteColumn(name, YugabyteDataType.INT);
        }

    }

    public static class YugabyteTables extends AbstractTables<YugabyteTable, YugabyteColumn> {

        public YugabyteTables(List<YugabyteTable> tables) {
            super(tables);
        }

        public YugabyteRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RANDOM() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<YugabyteColumn, YugabyteConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    YugabyteColumn column = getColumns().get(i);
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    YugabyteConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = YugabyteConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            constant = YugabyteConstant.createIntConstant(randomRowValues.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = YugabyteConstant.createBooleanConstant(randomRowValues.getBoolean(columnIndex));
                            break;
                        case TEXT:
                            constant = YugabyteConstant.createTextConstant(randomRowValues.getString(columnIndex));
                            break;
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new YugabyteRowValue(this, values);
            } catch (PSQLException e) {
                throw new IgnoreMeException();
            }

        }

    }

    public static class YugabyteRowValue extends AbstractRowValue<YugabyteTables, YugabyteColumn, YugabyteConstant> {

        protected YugabyteRowValue(YugabyteTables tables, Map<YugabyteColumn, YugabyteConstant> values) {
            super(tables, values);
        }

    }

    public static class YugabyteTable
            extends AbstractRelationalTable<YugabyteColumn, YugabyteIndex, YugabyteGlobalState> {

        private final TableType tableType;
        private final List<YugabyteStatisticsObject> statistics;
        private final boolean isInsertable;

        public YugabyteTable(String tableName, List<YugabyteColumn> columns, List<YugabyteIndex> indexes,
                TableType tableType, List<YugabyteStatisticsObject> statistics, boolean isView, boolean isInsertable) {
            super(tableName, columns, indexes, isView);
            this.statistics = statistics;
            this.isInsertable = isInsertable;
            this.tableType = tableType;
        }

        public List<YugabyteStatisticsObject> getStatistics() {
            return statistics;
        }

        public TableType getTableType() {
            return tableType;
        }

        public boolean isInsertable() {
            return isInsertable;
        }

        public enum TableType {
            STANDARD, TEMPORARY
        }

    }

    public static final class YugabyteStatisticsObject {
        private final String name;

        public YugabyteStatisticsObject(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static final class YugabyteIndex extends TableIndex {

        private YugabyteIndex(String indexName) {
            super(indexName);
        }

        public static YugabyteIndex create(String indexName) {
            return new YugabyteIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

}
