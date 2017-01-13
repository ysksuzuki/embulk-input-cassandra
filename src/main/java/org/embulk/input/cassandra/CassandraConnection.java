package org.embulk.input.cassandra;

import com.datastax.driver.core.*;
import com.google.common.base.Optional;

import java.io.Closeable;

public class CassandraConnection implements Closeable {

    private Cluster cluster;
    private Session session;

    public CassandraConnection(CassandraSettings settings) {
        Cluster.Builder clusterBuilder = clusterBuilder(settings);
        this.cluster = clusterBuilder.build();
        this.session = this.cluster.connect(settings.getSchema());
    }

    private Cluster.Builder clusterBuilder(CassandraSettings settings) {
        QueryOptions options = new QueryOptions();
        options.setConsistencyLevel(settings.getDefaultConsistencyLevel());
        options.setSerialConsistencyLevel(settings.getSerialLevel());
        options.setFetchSize(settings.getFetchSize());

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReadTimeoutMillis(settings.getReadTimeoutSecond() * 1000);
        socketOptions.setConnectTimeoutMillis(settings.getConnectionTimeoutSecond() * 1000);

        PoolingOptions poolOptions = new PoolingOptions();
        poolOptions
                .setConnectionsPerHost(HostDistance.LOCAL, settings.getCoreConnectionsPerHostLocal(),
                        settings.getMaxConnectionsPerHostLocal())
                .setConnectionsPerHost(HostDistance.REMOTE, settings.getCoreConnectionsPerHostRemote(),
                        settings.getMaxConnectionsPerHostRemote())
                .setMaxRequestsPerConnection(HostDistance.LOCAL, settings.getMaxRequestsPerConnectionLocal())
                .setMaxRequestsPerConnection(HostDistance.REMOTE, settings.getMaxRequestsPerConnectionRemote());

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withClusterName(settings.getClusterName())
                .withQueryOptions(options)
                .withSocketOptions(socketOptions)
                .withPoolingOptions(poolOptions)
                .withPort(settings.getNativeTransportPort())
                .addContactPoints(settings.getHosts());

/*                .withCompression(setting.getCompression().getCompressionType())
                .withReconnectionPolicy(setting.getReconnectionPolicy().getDriverInstance())
                .withLoadBalancingPolicy(setting.getLoadBalancingPolicy().getDriverInstance())
                .withRetryPolicy(setting.getRetryPolicy().getDriverInstance());*/

        if (settings.hasAuth()) {
            AuthProvider authProvider = new PlainTextAuthProvider(settings.getUser(), settings.getPassword());
            clusterBuilder.withAuthProvider(authProvider);
        }

/*        if (setting.isWithoutJMXReporting()) {
            clusterBuilder.withoutJMXReporting();
        }
        if (setting.isWithoutMetrics()) {
            clusterBuilder.withoutMetrics();
        }*/

        return clusterBuilder;
    }

    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    public ResultSet execute(String query) {
        return session.execute(query);
    }

    public String buildTableName(String tableName) {
        return tableName;
    }

    public String buildSelectQuery(String tableName,
                                   Optional<String> selectExpression, Optional<String> whereCondition,
                                   Optional<String> orderByExpression) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        sb.append(selectExpression.or("*"));
        sb.append(" FROM ").append(buildTableName(tableName));

        if (whereCondition.isPresent()) {
            sb.append(" WHERE ").append(whereCondition.get());
        }

        if (orderByExpression.isPresent()) {
            sb.append(" ORDER BY ").append(orderByExpression.get());
        }

        return sb.toString();
    }

    @Override
    public void close() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }
}
