package org.embulk.input.cassandra;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class CassandraRowHandler {
    private static final Map<DataType.Name, RowHandler> map = ImmutableMap.of(
            DataType.Name.VARCHAR, factory(DataType.Name.VARCHAR),
            DataType.Name.UUID, factory(DataType.Name.UUID)
            );

    public interface RowHandler {
        void handle(Row row, int index);
    }

    public static class IntHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
            System.out.print(row.getInt(index) + " ");
        }
    }

    public static class VarCharHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
            System.out.print(row.getString(index) + " ");
        }
    }

    public static class UuidHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
            System.out.print(row.getUUID(index) + " ");
        }
    }

    public static class TimeUuidHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
            System.out.print(row.getUUID(index) + " ");
        }
    }

    public static class BlobHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
            System.out.print(row.getBytes(index) + " ");
        }
    }

    public static class EmptyHandler implements RowHandler {
        @Override
        public void handle(Row row, int index) {
        }
    }

    public static RowHandler getHandler(DataType.Name dataTypeName) {
        return map.get(dataTypeName);
    }


    private static RowHandler factory(DataType.Name dataTypeName) {
        switch (dataTypeName) {
            case INT:
                return new IntHandler();
            case VARCHAR:
                return new VarCharHandler();
            case UUID:
                return new UuidHandler();
            case TIMEUUID:
                return new TimeUuidHandler();
            case BLOB:
                return new BlobHandler();
            default:
                return new EmptyHandler();
        }
    }
}
