package pt.it.av.atnog.cql;

import java.net.InetSocketAddress;

public abstract class Table extends CQLClient {
    protected final String keyspace, table;

    public Table(String keyspace, String table, InetSocketAddress add) {
        this(keyspace, table, add.getAddress().getHostAddress(), add.getPort());
    }

    public Table(String keyspace, String table, String add, int port) {
        super(add, port);
        this.keyspace = keyspace;
        this.table = table;
        createSchema();
    }

    public Table(String keyspace, String table, String add) {
        super(add);
        this.keyspace = keyspace;
        this.table = table;
        createSchema();
    }

    private void createSchema() {
        if (!containsKeySpace(keyspace)) {
            createKeySpace(keyspace);
            createTable();
        } else {
            if (!containsTable(keyspace, table))
                createTable();
        }
    }

    protected abstract void createTable();

    public void clear() {
        truncateTable(keyspace, table);
    }
}
