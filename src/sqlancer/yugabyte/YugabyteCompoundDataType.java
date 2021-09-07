package sqlancer.yugabyte;

import sqlancer.yugabyte.YugabyteSchema.YugabyteDataType;

import java.util.Optional;

public final class YugabyteCompoundDataType {

    private final YugabyteDataType dataType;
    private final YugabyteCompoundDataType elemType;
    private final Integer size;

    private YugabyteCompoundDataType(YugabyteDataType dataType, YugabyteCompoundDataType elemType, Integer size) {
        this.dataType = dataType;
        this.elemType = elemType;
        this.size = size;
    }

    public YugabyteDataType getDataType() {
        return dataType;
    }

    public YugabyteCompoundDataType getElemType() {
        if (elemType == null) {
            throw new AssertionError();
        }
        return elemType;
    }

    public Optional<Integer> getSize() {
        if (size == null) {
            return Optional.empty();
        } else {
            return Optional.of(size);
        }
    }

    public static YugabyteCompoundDataType create(YugabyteDataType type, int size) {
        return new YugabyteCompoundDataType(type, null, size);
    }

    public static YugabyteCompoundDataType create(YugabyteDataType type) {
        return new YugabyteCompoundDataType(type, null, null);
    }
}
