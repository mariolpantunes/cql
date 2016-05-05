package pt.it.av.atnog.cql;

import com.datastax.driver.core.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class CQLClient implements AutoCloseable {
    public static final int DEFAULT_PORT = 9042;
    private Cluster cluster = null;
    protected Session session = null;
    private PreparedStatement listTables, containsTable;

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

    /**
     *
     * @return
     */
    public List<String> listKeySpaces() {
        List<KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
        List<String> rv = new ArrayList<>();
        for(KeyspaceMetadata meta : keyspaces)
            rv.add(meta.getName());
        return rv;
    }

    /**
     *
     * @param keyspace
     * @return
     */
    public boolean containsKeySpace(String keyspace) {
        boolean rv = false;
        KeyspaceMetadata meta = cluster.getMetadata().getKeyspace(keyspace);
        if (meta != null)
            rv = true;
        return rv;
    }

    /**
     *
     * @param keyspace
     */
    public void dropKeySpace(String keyspace) {
        session.execute("DROP KEYSPACE " + keyspace);
    }

    /**
     *
     * @param keyspace
     * @return
     */
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

    /**
     *
     * @param keyspace
     * @param table
     */
    public void dropTable(String keyspace, String table) {
        session.execute("DROP TABLE " + keyspace + "." + table);
    }

    /**
     *
     * @param keyspace
     * @param table
     */
    public void truncateTable(String keyspace, String table) {
        session.execute("TRUNCATE " + keyspace + "." + table);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (cluster == null) {
            sb.append("Not connected.\n");
        } else {
            Metadata metadata = cluster.getMetadata();
            sb.append("Connected to cluster:" + metadata.getClusterName()
                    + "\n");
            for (Host host : metadata.getAllHosts())
                sb.append("Datacenter: " + host.getDatacenter()+ "; Host: " + host.getAddress() + "; Rack: "
                        + host.getRack() + "\n");
        }
        return sb.toString();
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