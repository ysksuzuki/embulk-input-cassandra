package org.embulk.input.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import java.util.List;

public class DefaultSettingValues {

    public static final ConsistencyLevel SERIAL_LEVEL = ConsistencyLevel.LOCAL_SERIAL;
    public static final ConsistencyLevel READ_LEVEL = ConsistencyLevel.LOCAL_QUORUM;
    public static final ConsistencyLevel WRITE_LEVEL = ConsistencyLevel.LOCAL_QUORUM;

    public static final int NATIVE_TRANSPORT_PORT = 9042;
    public static final int THRIFT_PORT = 9160;
    public static final int READ_TIMEOUT_SECOND = 12;
    public static final int CONNECTION_TIMEOUT_SECOND = 12;
    public static final int FETCH_SIZE = 1000;
    public static final ProtocolOptions.Compression COMPRESSION = ProtocolOptions.Compression.LZ4;

    public static final boolean WITHOUT_JMX_REPORTING = false;
    public static final boolean WITHOUT_METRICS = false;

    public static final String RETRY_POLICY = "AGGRESSIVE";
    public static final boolean RETRY_POLICY_ENABLE_LOGGING = false;

    public static final String RECONNECTION_POLICY = "EXPONENTIAL";
    public static final Long RECONNECTION_POLICY_CONSTANT_DELAY_MS = 5000L;
    public static final Long RECONNECTION_POLICY_EXPONENTIAL_BASE_DELAY_MS = 1000L;
    public static final Long RECONNECTION_POLICY_EXPONENTIAL_MAX_DELAY_MS = 600000L;

    public static final int CORE_CONNECTIONS_PER_HOST_LOCAL = 1;
    public static final int MAX_CONNECTIONS_PER_HOST_LOCAL = 1;
    public static final int CORE_CONNECTIONS_PER_HOST_REMOTE = 1;
    public static final int MAX_CONNECTIONS_PER_HOST_REMOTE = 1;
    public static final int MAX_REQUESTS_PER_CONNECTION_LOCAL = 1024;
    public static final int MAX_REQUESTS_PER_CONNECTION_REMOTE = 256;

    public static final boolean LOAD_BALANCING_POLICY_WITH_TOKEN_AWARE = true;
    public static final boolean LOAD_BALANCING_POLICY_WITH_TOKEN_AWARE_SHUFFLE = true;
    public static final boolean LOAD_BALANCING_POLICY_WITH_LATENCY_AWARE = false;
    public static final String LOAD_BALANCING_POLICY_LOCAL_DC = null;
    public static final List<String> LOAD_BALANCING_POLICY_WHITELIST = null;
}
