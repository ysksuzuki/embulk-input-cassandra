package org.embulk.input.cassandra;

import com.datastax.driver.core.*;
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
            Cluster.Builder clusterBuilder = Cluster.builder()
                    .addContactPoint("172.17.0.2")
                    //.withQueryOptions(options)
                    //.withSocketOptions(socketOptions)
                    //.withPoolingOptions(poolOptions)
                    .withPort(9042)
                    .withClusterName("test");
                    //.withTimestampGenerator(ControllableTimestampGenerator.INSTANCE)
                    //.withCompression(setting.getCompression().getCompressionType())
                    //.withReconnectionPolicy(setting.getReconnectionPolicy().getDriverInstance())
                    //.withLoadBalancingPolicy(setting.getLoadBalancingPolicy().getDriverInstance())
                    //.withRetryPolicy(setting.getRetryPolicy().getDriverInstance());

            AuthProvider authProvider = new PlainTextAuthProvider("dev", "dev");
            clusterBuilder.withAuthProvider(authProvider);

            cluster = clusterBuilder.build();
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