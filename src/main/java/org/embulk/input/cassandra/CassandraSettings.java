package org.embulk.input.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.policies.*;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class CassandraSettings {
    private String[] hosts;
    private int nativeTransportPort = DefaultSettingValues.NATIVE_TRANSPORT_PORT;
    private int thriftPort = DefaultSettingValues.THRIFT_PORT;
    private String clusterName;
    private String schema;
    private String user;
    private String password;
    private String table;
    private String query;
    private String select;
    private String where;
    private String orderBy;

    private ConsistencyLevel serialLevel = DefaultSettingValues.SERIAL_LEVEL;
    private ConsistencyLevel readLevel = DefaultSettingValues.READ_LEVEL;
    private ConsistencyLevel writeLevel = DefaultSettingValues.WRITE_LEVEL;
    // as in {@linkSocketOptions#DEFAULT_READ_TIMEOUT_MILLIS}
    private Integer readTimeoutSecond = DefaultSettingValues.READ_TIMEOUT_SECOND;
    // as in {@link SocketOptions#DEFAULT_CONNECT_TIMEOUT_MILLIS}
    private Integer connectionTimeoutSecond = DefaultSettingValues.CONNECTION_TIMEOUT_SECOND;
    private Integer fetchSize = DefaultSettingValues.FETCH_SIZE;
    private ProtocolOptions.Compression compression = DefaultSettingValues.COMPRESSION;
    private Boolean withoutJMXReporting = DefaultSettingValues.WITHOUT_JMX_REPORTING;
    private Boolean withoutMetrics = DefaultSettingValues.WITHOUT_METRICS;
    private RetryPolicy retryPolicy = DefaultRetryPolicy.INSTANCE;

    // connection pool setting
    private Integer coreConnectionsPerHostLocal = DefaultSettingValues.CORE_CONNECTIONS_PER_HOST_LOCAL;
    private Integer maxConnectionsPerHostLocal = DefaultSettingValues.MAX_CONNECTIONS_PER_HOST_LOCAL;
    private Integer coreConnectionsPerHostRemote = DefaultSettingValues.CORE_CONNECTIONS_PER_HOST_REMOTE;
    private Integer maxConnectionsPerHostRemote = DefaultSettingValues.MAX_CONNECTIONS_PER_HOST_REMOTE;
    private Integer maxRequestsPerConnectionLocal = DefaultSettingValues.MAX_REQUESTS_PER_CONNECTION_LOCAL;
    private Integer maxRequestsPerConnectionRemote = DefaultSettingValues.MAX_REQUESTS_PER_CONNECTION_REMOTE;

    public ConsistencyLevel getDefaultConsistencyLevel() {
        if (this.getReadLevel().compareTo(this.getWriteLevel()) < 0) {
            return this.getWriteLevel();
        }
        return this.getReadLevel();
    }

    public boolean hasAuth() {
        return user != null && !user.isEmpty();
    }

    public ConsistencyLevel getSerialLevel() {
        return serialLevel != null ? serialLevel : DefaultSettingValues.SERIAL_LEVEL;
    }
    public ConsistencyLevel getReadLevel() {
        return readLevel != null ? readLevel : DefaultSettingValues.READ_LEVEL;
    }

    public ConsistencyLevel getWriteLevel() {
        return writeLevel != null ? writeLevel : DefaultSettingValues.WRITE_LEVEL;
    }

    public Integer getFetchSize() {
        return fetchSize != null ? fetchSize : DefaultSettingValues.FETCH_SIZE;
    }
    public Integer getReadTimeoutSecond() {
        return readTimeoutSecond != null ? readTimeoutSecond : DefaultSettingValues.READ_TIMEOUT_SECOND;
    }
    public Integer getConnectionTimeoutSecond() {
        return connectionTimeoutSecond != null ? connectionTimeoutSecond : DefaultSettingValues.CONNECTION_TIMEOUT_SECOND;
    }
    public Integer getCoreConnectionsPerHostLocal() {
        return coreConnectionsPerHostLocal != null ? coreConnectionsPerHostLocal : DefaultSettingValues.CORE_CONNECTIONS_PER_HOST_LOCAL;
    }
    public Integer getMaxConnectionsPerHostLocal() {
        return maxConnectionsPerHostLocal != null ? maxConnectionsPerHostLocal : DefaultSettingValues.MAX_CONNECTIONS_PER_HOST_LOCAL;
    }
    public Integer getCoreConnectionsPerHostRemote() {
        return coreConnectionsPerHostRemote != null ? coreConnectionsPerHostRemote : DefaultSettingValues.CORE_CONNECTIONS_PER_HOST_REMOTE;
    }
    public Integer getMaxConnectionsPerHostRemote() {
        return maxConnectionsPerHostRemote != null ? maxConnectionsPerHostRemote : DefaultSettingValues.MAX_CONNECTIONS_PER_HOST_REMOTE;
    }
    public Integer getMaxRequestsPerConnectionLocal() {
        return maxRequestsPerConnectionLocal != null ? maxRequestsPerConnectionLocal : DefaultSettingValues.MAX_REQUESTS_PER_CONNECTION_LOCAL;
    }
    public Integer getMaxRequestsPerConnectionRemote() {
        return maxRequestsPerConnectionRemote != null ? maxConnectionsPerHostRemote : DefaultSettingValues.MAX_REQUESTS_PER_CONNECTION_REMOTE;
    }


}
