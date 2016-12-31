package org.embulk.input.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CassandraTest {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        // 172.17.0.2
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                    .addContactPoint("172.17.0.2")
                    .build();
            Session session = cluster.connect();
            ResultSet rs = session.execute("select release_version from system.local");
            Row row = rs.one();
            System.out.println(row.getString("release_version"));
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (cluster != null) cluster.close();
        }
    }
}