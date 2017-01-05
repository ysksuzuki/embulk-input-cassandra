package org.embulk.input.cassandra;

import java.util.List;
import com.google.common.base.Optional;
import org.embulk.config.*;
import org.embulk.spi.*;

public class CassandraInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("host")
        @ConfigDefault("null")
        Optional<String> getHost();

        @Config("port")
        @ConfigDefault("1521")
        int getPort();

        @Config("schema")
        @ConfigDefault("null")
        Optional<String> getSchema();

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("table")
        @ConfigDefault("null")
        Optional<String> getTable();

        @Config("query")
        @ConfigDefault("null")
        Optional<String> getQuery();

        @Config("select")
        @ConfigDefault("null")
        Optional<String> getSelect();

        @Config("where")
        @ConfigDefault("null")
        Optional<String> getWhere();

        @Config("order_by")
        @ConfigDefault("null")
        Optional<String> getOrderBy();

        @Config("columns")
        SchemaConfig getColumns();

        @ConfigInject
         BufferAllocator getBufferAllocator();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        int taskCount = 1;  // number of run() method calls

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        // Write your code here :)
        throw new UnsupportedOperationException("CassandraInputPlugin.run method is not implemented yet");
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private CassandraConnection newConnection(PluginTask task) {
        // TODO
        return null;
    }

    private String getRawQuery(PluginTask task, CassandraConnection con) {
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent() || task.getSelect().isPresent() ||
                    task.getWhere().isPresent() || task.getOrderBy().isPresent()) {
                throw new ConfigException("'table', 'select', 'where' and 'order_by' parameters are unnecessary if 'query' parameter is set.");
            }
            return task.getQuery().get();
        } else if (task.getTable().isPresent()) {
            return con.buildSelectQuery(task.getTable().get(), task.getSelect(),
                    task.getWhere(), task.getOrderBy());
        } else {
            throw new ConfigException("'table' or 'query' parameter is required");
        }
    }
}
