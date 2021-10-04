package sqlancer.yugabyte.ast;

import sqlancer.IgnoreMeException;
import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

import java.math.BigDecimal;

public abstract class YugabyteConstant implements YugabyteExpression {

    public abstract String getTextRepresentation();

    public abstract String getUnquotedTextRepresentation();

    public static class BooleanConstant extends YugabyteConstant {

        private final boolean value;

        public BooleanConstant(boolean value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return value ? "TRUE" : "FALSE";
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.BOOLEAN;
        }

        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public YugabyteConstant isEquals(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return YugabyteConstant.createBooleanConstant(value == rightVal.asBoolean());
            } else if (rightVal.isString()) {
                return YugabyteConstant
                        .createBooleanConstant(value == rightVal.cast(YugabyteDataType.BOOLEAN).asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected YugabyteConstant isLessThan(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return isLessThan(rightVal.cast(YugabyteDataType.BOOLEAN));
            } else {
                assert rightVal.isBoolean();
                return YugabyteConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            }
        }

        @Override
        public YugabyteConstant cast(YugabyteDataType type) {
            switch (type) {
            case BOOLEAN:
                return this;
            case INT:
                return YugabyteConstant.createIntConstant(value ? 1 : 0);
            case TEXT:
                return YugabyteConstant.createTextConstant(value ? "true" : "false");
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class YugabyteNullConstant extends YugabyteConstant {

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return null;
        }

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public YugabyteConstant isEquals(YugabyteConstant rightVal) {
            return YugabyteConstant.createNullConstant();
        }

        @Override
        protected YugabyteConstant isLessThan(YugabyteConstant rightVal) {
            return YugabyteConstant.createNullConstant();
        }

        @Override
        public YugabyteConstant cast(YugabyteDataType type) {
            return YugabyteConstant.createNullConstant();
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class StringConstant extends YugabyteConstant {

        protected final String value;

        public StringConstant(String value) {
            this.value = value;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'", value.replace("'", "''"));
        }

        @Override
        public YugabyteConstant isEquals(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(YugabyteDataType.INT).isEquals(rightVal.cast(YugabyteDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(YugabyteDataType.BOOLEAN).isEquals(rightVal.cast(YugabyteDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return YugabyteConstant.createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected YugabyteConstant isLessThan(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return cast(YugabyteDataType.INT).isLessThan(rightVal.cast(YugabyteDataType.INT));
            } else if (rightVal.isBoolean()) {
                return cast(YugabyteDataType.BOOLEAN).isLessThan(rightVal.cast(YugabyteDataType.BOOLEAN));
            } else if (rightVal.isString()) {
                return YugabyteConstant.createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public YugabyteConstant cast(YugabyteDataType type) {
            if (type == YugabyteDataType.TEXT) {
                return this;
            }
            String s = value.trim();
            switch (type) {
            case BOOLEAN:
                try {
                    return YugabyteConstant.createBooleanConstant(Long.parseLong(s) != 0);
                } catch (NumberFormatException e) {
                }
                switch (s.toUpperCase()) {
                case "T":
                case "TR":
                case "TRU":
                case "TRUE":
                case "1":
                case "YES":
                case "YE":
                case "Y":
                case "ON":
                    return YugabyteConstant.createTrue();
                case "F":
                case "FA":
                case "FAL":
                case "FALS":
                case "FALSE":
                case "N":
                case "NO":
                case "OF":
                case "OFF":
                default:
                    return YugabyteConstant.createFalse();
                }
            case INT:
                try {
                    return YugabyteConstant.createIntConstant(Long.parseLong(s));
                } catch (NumberFormatException e) {
                    return YugabyteConstant.createIntConstant(-1);
                }
            case TEXT:
                return this;
            default:
                return null;
            }
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.TEXT;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String asString() {
            return value;
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return value;
        }

    }

    public static class IntConstant extends YugabyteConstant {

        private final long val;

        public IntConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.INT;
        }

        @Override
        public long asInt() {
            return val;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public YugabyteConstant isEquals(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return cast(YugabyteDataType.BOOLEAN).isEquals(rightVal);
            } else if (rightVal.isInt()) {
                return YugabyteConstant.createBooleanConstant(val == rightVal.asInt());
            } else if (rightVal.isString()) {
                return YugabyteConstant.createBooleanConstant(val == rightVal.cast(YugabyteDataType.INT).asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected YugabyteConstant isLessThan(YugabyteConstant rightVal) {
            if (rightVal.isNull()) {
                return YugabyteConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return YugabyteConstant.createBooleanConstant(val < rightVal.asInt());
            } else if (rightVal.isBoolean()) {
                throw new AssertionError(rightVal);
            } else if (rightVal.isString()) {
                return YugabyteConstant.createBooleanConstant(val < rightVal.cast(YugabyteDataType.INT).asInt());
            } else {
                throw new IgnoreMeException();
            }

        }

        @Override
        public YugabyteConstant cast(YugabyteDataType type) {
            switch (type) {
            case BOOLEAN:
                return YugabyteConstant.createBooleanConstant(val != 0);
            case INT:
                return this;
            case TEXT:
                return YugabyteConstant.createTextConstant(String.valueOf(val));
            default:
                return null;
            }
        }

        @Override
        public String getUnquotedTextRepresentation() {
            return getTextRepresentation();
        }

    }

    public static class ByteConstant extends StringConstant {

        public ByteConstant(String value) {
            super(value);
        }

        @Override
        public String getTextRepresentation() {
            return String.format("'%s'::bytea", value.replace("'", "''"));
        }
    }

    public static YugabyteConstant createNullConstant() {
        return new YugabyteNullConstant();
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isString() {
        return false;
    }

    public static YugabyteConstant createIntConstant(long val) {
        return new IntConstant(val);
    }

    public static YugabyteConstant createBooleanConstant(boolean val) {
        return new BooleanConstant(val);
    }

    @Override
    public YugabyteConstant getExpectedValue() {
        return this;
    }

    public boolean isNull() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public static YugabyteConstant createFalse() {
        return createBooleanConstant(false);
    }

    public static YugabyteConstant createTrue() {
        return createBooleanConstant(true);
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public boolean isBoolean() {
        return false;
    }

    public abstract YugabyteConstant isEquals(YugabyteConstant rightVal);

    public boolean isInt() {
        return false;
    }

    protected abstract YugabyteConstant isLessThan(YugabyteConstant rightVal);

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract YugabyteConstant cast(YugabyteDataType type);

    public static YugabyteConstant createTextConstant(String string) {
        return new StringConstant(string);
    }

    public static YugabyteConstant createByteConstant(String string) {
        return new ByteConstant(string);
    }

    public abstract static class YugabyteConstantBase extends YugabyteConstant {

        @Override
        public String getUnquotedTextRepresentation() {
            return null;
        }

        @Override
        public YugabyteConstant isEquals(YugabyteConstant rightVal) {
            return null;
        }

        @Override
        protected YugabyteConstant isLessThan(YugabyteConstant rightVal) {
            return null;
        }

        @Override
        public YugabyteConstant cast(YugabyteDataType type) {
            return null;
        }
    }

    public static class DecimalConstant extends YugabyteConstantBase {

        private final BigDecimal val;

        public DecimalConstant(BigDecimal val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.DECIMAL;
        }

    }

    public static class InetConstant extends YugabyteConstantBase {

        private final String val;

        public InetConstant(String val) {
            this.val = "'" + val + "'";
        }

        @Override
        public String getTextRepresentation() {
            return String.valueOf(val);
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.INET;
        }

    }

    public static class FloatConstant extends YugabyteConstantBase {

        private final float val;

        public FloatConstant(float val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.FLOAT;
        }

    }

    public static class DoubleConstant extends YugabyteConstantBase {

        private final double val;

        public DoubleConstant(double val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            if (Double.isFinite(val)) {
                return String.valueOf(val);
            } else {
                return "'" + val + "'";
            }
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.FLOAT;
        }

    }

    public static class BitConstant extends YugabyteConstantBase {

        private final long val;

        public BitConstant(long val) {
            this.val = val;
        }

        @Override
        public String getTextRepresentation() {
            return String.format("B'%s'", Long.toBinaryString(val));
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.BIT;
        }

    }

    public static class RangeConstant extends YugabyteConstantBase {

        private final long left;
        private final boolean leftIsInclusive;
        private final long right;
        private final boolean rightIsInclusive;

        public RangeConstant(long left, boolean leftIsInclusive, long right, boolean rightIsInclusive) {
            this.left = left;
            this.leftIsInclusive = leftIsInclusive;
            this.right = right;
            this.rightIsInclusive = rightIsInclusive;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            if (leftIsInclusive) {
                sb.append("[");
            } else {
                sb.append("(");
            }
            sb.append(left);
            sb.append(",");
            sb.append(right);
            if (rightIsInclusive) {
                sb.append("]");
            } else {
                sb.append(")");
            }
            sb.append("'");
            sb.append("::int4range");
            return sb.toString();
        }

        @Override
        public YugabyteDataType getExpressionType() {
            return YugabyteDataType.RANGE;
        }

    }

    public static YugabyteConstant createDecimalConstant(BigDecimal bigDecimal) {
        return new DecimalConstant(bigDecimal);
    }

    public static YugabyteConstant createFloatConstant(float val) {
        return new FloatConstant(val);
    }

    public static YugabyteConstant createDoubleConstant(double val) {
        return new DoubleConstant(val);
    }

    public static YugabyteConstant createRange(long left, boolean leftIsInclusive, long right,
                                               boolean rightIsInclusive) {
        long realLeft;
        long realRight;
        if (left > right) {
            realRight = left;
            realLeft = right;
        } else {
            realLeft = left;
            realRight = right;
        }
        return new RangeConstant(realLeft, leftIsInclusive, realRight, rightIsInclusive);
    }

    public static YugabyteExpression createBitConstant(long integer) {
        return new BitConstant(integer);
    }

    public static YugabyteExpression createInetConstant(String val) {
        return new InetConstant(val);
    }

}
