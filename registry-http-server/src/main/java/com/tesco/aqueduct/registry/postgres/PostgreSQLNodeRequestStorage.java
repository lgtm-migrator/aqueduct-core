package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeRequest;
import com.tesco.aqueduct.registry.model.NodeRequestStorage;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class PostgreSQLNodeRequestStorage implements NodeRequestStorage {

    private final DataSource dataSource;
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgreSQLNodeRequestStorage.class));
    private static final String QUERY_INSERT_OR_UPDATE_NODE_REQUEST =
        "INSERT INTO node_requests (host_id, bootstrap_requested, bootstrap_type)" +
            "VALUES (" +
            "?, " +
            "?, " +
            "? " +
            ")" +
        "ON CONFLICT (host_id) DO UPDATE SET " +
            "host_id = EXCLUDED.host_id, " +
            "bootstrap_requested = EXCLUDED.bootstrap_requested, " +
            "bootstrap_type = EXCLUDED.bootstrap_type, " +
            "bootstrap_received = null;";
    private static final String QUERY_READ_NODE_REQUEST =
        "SELECT bootstrap_type " +
        "FROM node_requests " +
        "WHERE host_id = ? AND bootstrap_received IS null;";
    private static final String QUERY_UPDATE_NODE_REQUEST_RECEIVED =
        "UPDATE node_requests " +
        "SET bootstrap_received = ? " +
        "WHERE host_id = ?;";

    public PostgreSQLNodeRequestStorage(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(NodeRequest nodeRequest) throws SQLException {
         try (Connection connection = getConnection()) {
             insertOrUpdate(connection, nodeRequest);
         } catch (SQLException exception) {
             LOG.error("save", "insert a node request", exception);
             throw exception;
         }
    }

    @Override
    public BootstrapType requiresBootstrap(Node node) throws SQLException {

        if(node.getLastRegistrationTime() != null
            && node.getLastRegistrationTime().isBefore(ZonedDateTime.now().minusDays(30))) {
            LOG.info("requiresBootstrap", node.getHost() + " stale device");
        }

        try (Connection connection = getConnection()) {
            BootstrapType bootstrapType = readBootstrapType(node.getHost(), connection);
            if (bootstrapType != BootstrapType.NONE) {
                updateReceivedBootstrap(connection, node.getHost());
            }
            return bootstrapType;
        } catch (SQLException exception) {
            LOG.error("read", "read a node request", exception);
            throw exception;
        }
    }

    private BootstrapType readBootstrapType(String hostId, Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_READ_NODE_REQUEST)) {
            statement.setString(1, hostId);
            return getBootstrapType(statement.executeQuery());
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
    }

    private BootstrapType getBootstrapType(ResultSet executeQuery) throws SQLException {
         if(executeQuery.next()) {
             String bootstrapType = executeQuery.getString("bootstrap_type");
             return BootstrapType.valueOf(bootstrapType);
         }
         return BootstrapType.NONE;
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        try {
            return dataSource.getConnection();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getConnection:time", Long.toString(end - start));
        }
    }

    private void updateReceivedBootstrap(Connection connection, String hostId) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(ZonedDateTime.now(ZoneOffset.UTC).toLocalDateTime());
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_NODE_REQUEST_RECEIVED)) {
            statement.setTimestamp(1, timestamp);
            statement.setString(2, hostId);
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("updateReceived:time", Long.toString(end - start));
        }
    }

    private void insertOrUpdate(
        final Connection connection,
        final NodeRequest nodeRequest
    ) throws SQLException {
        long start = System.currentTimeMillis();
        Timestamp timestamp = Timestamp.valueOf(nodeRequest.getBootstrap().getRequestedDate().atOffset(ZoneOffset.UTC).toLocalDateTime());

        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_OR_UPDATE_NODE_REQUEST)) {
            statement.setString(1, nodeRequest.getHostId());
            statement.setTimestamp(2, timestamp);
            statement.setString(3, nodeRequest.getBootstrap().getType().toString());
            statement.execute();
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("insert:time", Long.toString(end - start));
        }
    }
}
