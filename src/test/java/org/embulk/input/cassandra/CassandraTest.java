package org.embulk.input.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class CassandraTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void cassandraConnectionTest() {
        CassandraSettings settings = CassandraSettings.builder()
                .hosts(new String[] {"172.16.210.177"})
                .nativeTransportPort(9042)
                .clusterName("scheduler")
                .schema("scheduler")
                .user("dev").password("dev")
                .build();
        try (CassandraConnection connection = new CassandraConnection(settings)) {
            ResultSet resultSet = connection.execute(
                    connection.buildSelectQuery("scheduler_country_holiday", Optional.<String>absent(),
                            Optional.<String>absent(), Optional.<String>absent()));
            List<CassandraRowHandler.RowHandler> handlers = new ArrayList<>();
            for (ColumnDefinitions.Definition definition : resultSet.getColumnDefinitions()) {
                handlers.add(CassandraRowHandler.getHandler(definition.getType().getName()));
            }
            for (Row row : resultSet) {
                int ix = 0;
                for (CassandraRowHandler.RowHandler handler : handlers) {
                    handler.handle(row, ix);
                    ix++;
                }
                System.out.println("");
            }
        }
    }
}