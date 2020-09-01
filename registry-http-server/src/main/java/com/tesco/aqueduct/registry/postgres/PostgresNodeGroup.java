package com.tesco.aqueduct.registry.postgres;

import com.tesco.aqueduct.registry.model.NodeFactory;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.NodeGroup;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PostgresNodeGroup extends NodeGroup {
    private static final int UNPERSISTED_GROUP_VERSION = Integer.MIN_VALUE;

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PostgresNodeGroup.class));

    private static final String QUERY_INSERT_GROUP =
        "INSERT INTO registry (group_id, entry, version)" +
                "VALUES (" +
                "?, " +
                "?::JSON, " +
                "0 " +
                ")" +
                "ON CONFLICT DO NOTHING; ";
    private static final String QUERY_UPDATE_GROUP =
        "UPDATE registry SET " +
                "entry = ?::JSON , " +
                "version = registry.version + 1 " +
                "WHERE " +
                "registry.group_id = ? " +
                "; ";
    private static final String QUERY_DELETE_GROUP =
        "DELETE from registry where group_id = ? and version = ? ;";

    private static List<Node> readGroupEntry(final String entry) throws IOException {
        NodeFactory nodeFactory = new NodeFactory();
        return nodeFactory.getNodesFromJson(entry);
    }

    private final String groupId;
    private final int version;

    public PostgresNodeGroup(final ResultSet rs) throws SQLException, IOException {
        super(readGroupEntry(rs.getString("entry")));
        this.version = rs.getInt("version");
        this.groupId = rs.getString("group_id");
    }

    public PostgresNodeGroup(final String groupId) {
        super();
        this.groupId = groupId;
        this.version = UNPERSISTED_GROUP_VERSION;
    }

    private PostgresNodeGroup(final String groupId, final int version, final List<Node> nodes) {
        super(nodes);
        this.groupId = groupId;
        this.version = version;
    }

    public void persist(final Connection connection) throws IOException, SQLException {
        if (version == UNPERSISTED_GROUP_VERSION) {
            insert(connection);
        } else {
            update(connection);
        }
    }

    private void insert(final Connection connection) throws IOException, SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_INSERT_GROUP)) {
            statement.setString(1, groupId);
            statement.setString(2, nodesToJson());

            if(statement.executeUpdate() == 0) {
                LOG.info("Insert new PostgresNodeGroup", "conflict on insert");
            }
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("node group insert:time", Long.toString(end - start));
        }
    }

    private void update(final Connection connection) throws SQLException, IOException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_UPDATE_GROUP)) {
            statement.setString(1, nodesToJson());
            statement.setString(2, groupId);

            if(statement.executeUpdate() == 0) {
                throw new RuntimeException("Locking failed on update");
            }
        }finally {
            long end = System.currentTimeMillis();
            LOG.info("node group update:time", Long.toString(end - start));
        }
    }

    public void delete(final Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(QUERY_DELETE_GROUP)) {
            statement.setString(1, groupId);
            statement.setInt(2, version);

            if(statement.executeUpdate() == 0) {
                throw new RuntimeException("Locking failed on delete");
            }

        }finally {
            long end = System.currentTimeMillis();
            LOG.info("node group delete:time", Long.toString(end - start));
        }
    }
}