package pt.it.av.atnog.cql;

import com.datastax.driver.core.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class CQLClient implements AutoCloseable {
    public static final int DEFAULT_PORT = 9042;
    private Cluster cluster = null;
    protected Session session = null;
    private PreparedStatement containsKeySpace, listTables, containsTable;
    private SimpleStatement listKeySpaces;

    public CQLClient() {
        this("localhost", DEFAULT_PORT);
    }

    public CQLClient(String add) {
        this(add, DEFAULT_PORT);
    }

    public CQLClient(InetSocketAddress add) {
        this(add.getAddress().getHostAddress(), add.getPort());
    }

    public CQLClient(String add, int port) {
        cluster = Cluster.builder().withPort(port).addContactPoint(add).build();
        session = cluster.connect();
        listKeySpaces = new SimpleStatement("SELECT * FROM "
                + "system.schema_keyspaces");
        containsKeySpace = session.prepare("SELECT * FROM "
                + "system.schema_keyspaces WHERE keyspace_name=?");
        listTables = session.prepare("SELECT columnfamily_name "
                + "FROM system.schema_columnfamilies WHERE keyspace_name=?");
        containsTable = session.prepare("SELECT columnfamily_name "
                + "FROM system.schema_columnfamilies WHERE "
                + "keyspace_name=? and columnfamily_name=?");
    }

    public void createKeySpace(String keyspace) {
        createKeySpace(keyspace, CQLClient.StrategyClass.SimpleStrategy, 1);
    }

    public void createKeySpace(String keyspace, StrategyClass strategy,
                               int replicationFactor) {
        switch (strategy) {
            case SimpleStrategy:
                session.execute("CREATE KEYSPACE " + keyspace + " WITH "
                        + "replication={'class':'SimpleStrategy',"
                        + "'replication_factor':" + replicationFactor + "}");
                break;
            case NetworkTopologyStrategy:
                session.execute("CREATE KEYSPACE " + keyspace + " WITH "
                        + "replication={'class':'NetworkTopologyStrategy',"
                        + "'replication_factor':" + replicationFactor + "}");
                break;
            default:
                session.execute("CREATE KEYSPACE " + keyspace + " WITH "
                        + "replication={'class':'SimpleStrategy',"
                        + "'replication_factor':" + replicationFactor + "}");
                break;
        }

    }

    public List<String> listKeySpaces() {
        ResultSet results = session.execute(listKeySpaces);
        List<Row> rows = results.all();
        List<String> rv = new ArrayList<String>(rows.size());
        for (Row row : rows)
            rv.add(row.getString("keyspace_name"));
        return rv;
    }

    public boolean containsKeySpace(String keyspace) {
        boolean rv = false;
        ResultSet rs = session.execute(containsKeySpace.bind(keyspace));
        if (rs.one() != null)
            rv = true;
        return rv;
    }

    public void dropKeySpace(String keyspace) {
        session.execute("DROP KEYSPACE " + keyspace);
    }

    public List<String> listTables(String keyspace) {
        ResultSet rs = session.execute(listTables.bind(keyspace));
        List<String> rv = new ArrayList<String>();
        for (Row row : rs)
            rv.add(row.getString("columnfamily_name"));
        return rv;
    }

    public boolean containsTable(String keyspace, String table) {
        boolean rv = false;
        ResultSet rs = session.execute(containsTable.bind(keyspace, table));
        if (rs.one() != null)
            rv = true;
        return rv;
    }

    public void dropTable(String keyspace, String table) {
        session.execute("DROP TABLE " + keyspace + "." + table);
    }

    public void truncateTable(String keyspace, String table) {
        session.execute("TRUNCATE " + keyspace + "." + table);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        if (cluster == null) {
            builder.append("Not connected.\n");
        } else {
            Metadata metadata = cluster.getMetadata();
            builder.append("Connected to cluster:" + metadata.getClusterName()
                    + "\n");
            for (Host host : metadata.getAllHosts()) {
                builder.append("Datacenter: " + host.getDatacenter()
                        + "; Host: " + host.getAddress() + "; Rack: "
                        + host.getRack() + "\n");
            }
        }
        return builder.toString();
    }

    @Override
    public void close() throws Exception {
        cluster.close();
        cluster = null;
        session.close();
        session = null;
    }

    public enum StrategyClass {
        SimpleStrategy, NetworkTopologyStrategy
    }
}