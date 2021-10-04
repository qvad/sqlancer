package sqlancer.yugabyte.ast;

import sqlancer.Randomly;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;
import sqlancer.yugabyte.gen.YugabyteExpressionGenerator;

import java.util.ArrayList;
import java.util.List;

public enum YugabyteFunctionWithUnknownResult {

    ABBREV("abbrev", YugabyteDataType.TEXT, YugabyteDataType.INET),
    BROADCAST("broadcast", YugabyteDataType.INET, YugabyteDataType.INET),
    FAMILY("family", YugabyteDataType.INT, YugabyteDataType.INET),
    HOSTMASK("hostmask", YugabyteDataType.INET, YugabyteDataType.INET),
    MASKLEN("masklen", YugabyteDataType.INT, YugabyteDataType.INET),
    NETMASK("netmask", YugabyteDataType.INET, YugabyteDataType.INET),
    SET_MASKLEN("set_masklen", YugabyteDataType.INET, YugabyteDataType.INET, YugabyteDataType.INT),
    TEXT("text", YugabyteDataType.TEXT, YugabyteDataType.INET),
    INET_SAME_FAMILY("inet_same_family", YugabyteDataType.BOOLEAN, YugabyteDataType.INET, YugabyteDataType.INET),

    // https://www.postgres.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-SIGNAL-TABLE
    // PG_RELOAD_CONF("pg_reload_conf", YugabyteDataType.BOOLEAN), // too much output
    // PG_ROTATE_LOGFILE("pg_rotate_logfile", YugabyteDataType.BOOLEAN), prints warning

    // https://www.postgresql.org/docs/devel/functions-info.html#FUNCTIONS-INFO-SESSION-TABLE
    CURRENT_DATABASE("current_database", YugabyteDataType.TEXT), // name
    CURRENT_QUERY("current_query", YugabyteDataType.TEXT), // can generate false positives
    CURRENT_SCHEMA("current_schema", YugabyteDataType.TEXT), // name
    // CURRENT_SCHEMAS("current_schemas", YugabyteDataType.TEXT, YugabyteDataType.BOOLEAN),
    INET_CLIENT_PORT("inet_client_port", YugabyteDataType.INT),
    INET_SERVER_PORT("inet_server_port", YugabyteDataType.INT),
    PG_BACKEND_PID("pg_backend_pid", YugabyteDataType.INT),
    PG_CURRENT_LOGFILE("pg_current_logfile", YugabyteDataType.TEXT),
//    PG_IS_OTHER_TEMP_SCHEMA("pg_is_other_temp_schema", YugabyteDataType.BOOLEAN),
//    PG_JIT_AVAILABLE("pg_is_other_temp_schema", YugabyteDataType.BOOLEAN),
    PG_NOTIFICATION_QUEUE_USAGE("pg_notification_queue_usage", YugabyteDataType.REAL),
    PG_TRIGGER_DEPTH("pg_trigger_depth", YugabyteDataType.INT), VERSION("version", YugabyteDataType.TEXT),

    //
    TO_CHAR("to_char", YugabyteDataType.TEXT, YugabyteDataType.BYTEA, YugabyteDataType.TEXT) {
        @Override
        public YugabyteExpression[] getArguments(YugabyteDataType returnType, YugabyteExpressionGenerator gen,
                                                 int depth) {
            YugabyteExpression[] args = super.getArguments(returnType, gen, depth);
            args[0] = gen.generateExpression(YugabyteDataType.getRandomType());
            return args;
        }
    },

    // String functions
    ASCII("ascii", YugabyteDataType.INT, YugabyteDataType.TEXT),
    BTRIM("btrim", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    CHR("chr", YugabyteDataType.TEXT, YugabyteDataType.INT),
    CONVERT_FROM("convert_from", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT) {
        @Override
        public YugabyteExpression[] getArguments(YugabyteDataType returnType, YugabyteExpressionGenerator gen,
                                                 int depth) {
            YugabyteExpression[] args = super.getArguments(returnType, gen, depth);
            args[1] = YugabyteConstant.createTextConstant("UTF8");
            return args;
        }
    },
    // concat
    // segfault
    BIT_LENGTH("bit_length", YugabyteDataType.INT, YugabyteDataType.BYTEA),
    INITCAP("initcap", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    LEFT("left", YugabyteDataType.TEXT, YugabyteDataType.INT, YugabyteDataType.TEXT),
    LOWER("lower", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    MD5("md5", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    UPPER("upper", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    // PG_CLIENT_ENCODING("pg_client_encoding", YugabyteDataType.TEXT),
    QUOTE_LITERAL("quote_literal", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    QUOTE_IDENT("quote_ident", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    REGEX_REPLACE("regexp_replace", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    REPEAT("repeat", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.INT),
    REPLACE("replace", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    REVERSE("reverse", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    RIGHT("right", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.INT),
    RPAD("rpad", YugabyteDataType.TEXT, YugabyteDataType.INT, YugabyteDataType.TEXT),
    RTRIM("rtrim", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    SPLIT_PART("split_part", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.INT),
    STRPOS("strpos", YugabyteDataType.INT, YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    SUBSTR("substr", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.INT, YugabyteDataType.INT),
    TO_ASCII("to_ascii", YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    TO_HEX("to_hex", YugabyteDataType.INT, YugabyteDataType.TEXT),
    TRANSLATE("translate", YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT, YugabyteDataType.TEXT),
    // mathematical functions
    // https://www.postgresql.org/docs/9.5/functions-math.html
    ABS("abs", YugabyteDataType.REAL, YugabyteDataType.REAL),
    CBRT("cbrt", YugabyteDataType.REAL, YugabyteDataType.REAL), CEILING("ceiling", YugabyteDataType.REAL), //
    DEGREES("degrees", YugabyteDataType.REAL), EXP("exp", YugabyteDataType.REAL), LN("ln", YugabyteDataType.REAL),
    LOG("log", YugabyteDataType.REAL), LOG2("log", YugabyteDataType.REAL, YugabyteDataType.REAL),
    PI("pi", YugabyteDataType.REAL), POWER("power", YugabyteDataType.REAL, YugabyteDataType.REAL),
    TRUNC("trunc", YugabyteDataType.REAL, YugabyteDataType.INT),
    TRUNC2("trunc", YugabyteDataType.REAL, YugabyteDataType.INT, YugabyteDataType.REAL),
    FLOOR("floor", YugabyteDataType.REAL),

    // trigonometric functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-TRIG-TABLE
    ACOS("acos", YugabyteDataType.REAL), //
    ACOSD("acosd", YugabyteDataType.REAL), //
    ASIN("asin", YugabyteDataType.REAL), //
    ASIND("asind", YugabyteDataType.REAL), //
    ATAN("atan", YugabyteDataType.REAL), //
    ATAND("atand", YugabyteDataType.REAL), //
    ATAN2("atan2", YugabyteDataType.REAL, YugabyteDataType.REAL), //
    ATAN2D("atan2d", YugabyteDataType.REAL, YugabyteDataType.REAL), //
    COS("cos", YugabyteDataType.REAL), //
    COSD("cosd", YugabyteDataType.REAL), //
    COT("cot", YugabyteDataType.REAL), //
    COTD("cotd", YugabyteDataType.REAL), //
    SIN("sin", YugabyteDataType.REAL), //
    SIND("sind", YugabyteDataType.REAL), //
    TAN("tan", YugabyteDataType.REAL), //
    TAND("tand", YugabyteDataType.REAL), //

    // hyperbolic functions - complete
    // https://www.postgresql.org/docs/12/functions-math.html#FUNCTIONS-MATH-HYP-TABLE
    SINH("sinh", YugabyteDataType.REAL), //
    COSH("cosh", YugabyteDataType.REAL), //
    TANH("tanh", YugabyteDataType.REAL), //
    ASINH("asinh", YugabyteDataType.REAL), //
    ACOSH("acosh", YugabyteDataType.REAL), //
    ATANH("atanh", YugabyteDataType.REAL), //

    // https://www.postgresql.org/docs/devel/functions-binarystring.html
    GET_BIT("get_bit", YugabyteDataType.INT, YugabyteDataType.TEXT, YugabyteDataType.INT),
    GET_BYTE("get_byte", YugabyteDataType.INT, YugabyteDataType.TEXT, YugabyteDataType.INT),

    // range functions
    // https://www.postgresql.org/docs/devel/functions-range.html#RANGE-FUNCTIONS-TABLE
    RANGE_LOWER("lower", YugabyteDataType.INT, YugabyteDataType.RANGE), //
    RANGE_UPPER("upper", YugabyteDataType.INT, YugabyteDataType.RANGE), //
    RANGE_ISEMPTY("isempty", YugabyteDataType.BOOLEAN, YugabyteDataType.RANGE), //
    RANGE_LOWER_INC("lower_inc", YugabyteDataType.BOOLEAN, YugabyteDataType.RANGE), //
    RANGE_UPPER_INC("upper_inc", YugabyteDataType.BOOLEAN, YugabyteDataType.RANGE), //
    RANGE_LOWER_INF("lower_inf", YugabyteDataType.BOOLEAN, YugabyteDataType.RANGE), //
    RANGE_UPPER_INF("upper_inf", YugabyteDataType.BOOLEAN, YugabyteDataType.RANGE), //
    RANGE_MERGE("range_merge", YugabyteDataType.RANGE, YugabyteDataType.RANGE, YugabyteDataType.RANGE), //

    // https://www.postgresql.org/docs/devel/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
    GET_COLUMN_SIZE("get_column_size", YugabyteDataType.INT, YugabyteDataType.TEXT);
    // PG_DATABASE_SIZE("pg_database_size", YugabyteDataType.INT, YugabyteDataType.INT);
    // PG_SIZE_BYTES("pg_size_bytes", YugabyteDataType.INT, YugabyteDataType.TEXT);

    private final String functionName;
    private final YugabyteDataType returnType;
    private final YugabyteDataType[] argTypes;

    YugabyteFunctionWithUnknownResult(String functionName, YugabyteDataType returnType, YugabyteDataType... indexType) {
        this.functionName = functionName;
        this.returnType = returnType;
        this.argTypes = indexType.clone();
    }

    public boolean isCompatibleWithReturnType(YugabyteDataType t) {
        return t == returnType;
    }

    public YugabyteExpression[] getArguments(YugabyteDataType returnType, YugabyteExpressionGenerator gen, int depth) {
        YugabyteExpression[] args = new YugabyteExpression[argTypes.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = gen.generateExpression(depth, argTypes[i]);
        }
        return args;

    }

    public String getName() {
        return functionName;
    }

    public static List<YugabyteFunctionWithUnknownResult> getSupportedFunctions(YugabyteDataType type) {
        List<YugabyteFunctionWithUnknownResult> functions = new ArrayList<>();
        for (YugabyteFunctionWithUnknownResult func : values()) {
            if (func.isCompatibleWithReturnType(type)) {
                functions.add(func);
            }
        }
        return functions;
    }

}
