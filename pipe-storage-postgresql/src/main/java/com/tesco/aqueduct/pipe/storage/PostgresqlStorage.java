package com.tesco.aqueduct.pipe.storage;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PostgresqlStorage implements CentralStorage {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(PostgresqlStorage.class));

    private final int limit;
    private final DataSource pipeDataSource;
    private final DataSource compactionDataSource;
    private final long maxBatchSize;
    private final long retryAfter;
    private final GlobalLatestOffsetCache globalLatestOffsetCache;
    private final int nodeCount;
    private final long clusterDBPoolSize;
    private final int workMemMb;
    private ClusterStorage clusterStorage;

    public PostgresqlStorage(
        final DataSource pipeDataSource,
        final DataSource compactionDataSource,
        final int limit,
        final long retryAfter,
        final long maxBatchSize,
        final GlobalLatestOffsetCache globalLatestOffsetCache,
        int nodeCount,
        long clusterDBPoolSize,
        int workMemMb,
        ClusterStorage clusterStorage
    ) {
        this.retryAfter = retryAfter;
        this.limit = limit;
        this.pipeDataSource = pipeDataSource;
        this.compactionDataSource = compactionDataSource;
        this.globalLatestOffsetCache = globalLatestOffsetCache;
        this.nodeCount = nodeCount;
        this.clusterDBPoolSize = clusterDBPoolSize;
        this.maxBatchSize = maxBatchSize + (((long)Message.MAX_OVERHEAD_SIZE) * limit);
        this.workMemMb = workMemMb;
        this.clusterStorage = clusterStorage;

        //initialise connection pool eagerly
        try (Connection connection = this.pipeDataSource.getConnection()) {
            LOG.debug("postgresql storage", "initialised connection pool");
        } catch (SQLException e) {
            LOG.error("postgresql storage", "Error initializing connection pool", e);
        }
    }

    @Override
    public MessageResults read(
        final List<String> types,
        final long startOffset,
        final String locationUuid
    ) {
        long start = System.currentTimeMillis();
        Connection connection = null;
        try {
            connection = getConnectionAndStartTransaction();

            final Optional<ClusterCacheEntry> entry = clusterStorage.getClusterCacheEntry(locationUuid, connection);

            if (isValidAndUnexpired(entry)) {
                return readMessages(types, start, startOffset, entry.get().getClusterIds(), connection);
            } else {
                commit(connection);
                close(connection);

                final List<String> clusterUuids = clusterStorage.resolveClustersFor(locationUuid);

                connection = getConnectionAndStartTransaction();

                final Optional<List<Long>> newClusterIds = clusterStorage.updateAndGetClusterIds(locationUuid, clusterUuids, entry, connection);

                if (newClusterIds.isPresent()) {
                    return readMessages(types, start, startOffset, newClusterIds.get(), connection);
                } else {
                    LOG.info("postgresql storage", "Recursive read due to Cluster Cache invalidation race condition");
                    return read(types, startOffset, locationUuid);
                }
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "read", exception);
            close(connection);
            throw new RuntimeException(exception);
        } finally {
            if (connection != null) {
                commit(connection);
                close(connection);
            }
            long end = System.currentTimeMillis();
            LOG.info("read:time", Long.toString(end - start));
        }
    }

    private Connection getConnectionAndStartTransaction() throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = pipeDataSource.getConnection();
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.setAutoCommit(false);
        LOG.info("getConnection:time", Long.toString(System.currentTimeMillis() - start));
        return connection;
    }

    private boolean isValidAndUnexpired(Optional<ClusterCacheEntry> entry) {
        return entry.map(ClusterCacheEntry::isValidAndUnexpired).orElse(false);
    }

    private MessageResults readMessages(
        List<String> types,
        long start,
        long startOffset,
        List<Long> clusterIds,
        Connection connection
    ) throws SQLException {

        setWorkMem(connection);

        final long globalLatestOffset = globalLatestOffsetCache.get(connection);

        try(PreparedStatement getOffsetsQuery = getOffsetsStatement(connection, startOffset, globalLatestOffset, clusterIds, types)) {
            TreeSet<Long> offsets = runGetOffsetQuery(getOffsetsQuery);

            List<Message> messages;
            try(PreparedStatement getMessagesQuery = getMessagesStatement(connection, offsets)) {
                messages = runMessagesQuery(getMessagesQuery);
            }

            long end = System.currentTimeMillis();

            final long retry = calculateRetryAfter(end - start, messages.size());

            LOG.info("PostgresSqlStorage:retry", String.valueOf(retry));
            return new MessageResults(messages, retry, OptionalLong.of(globalLatestOffset), PipeState.UP_TO_DATE);
        }
    }

    private TreeSet<Long> runGetOffsetQuery(PreparedStatement query) throws SQLException {
        final TreeSet<Long> orderedOffset = new TreeSet<>();
        long start = System.currentTimeMillis();

        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                final Long offset = rs.getLong("msg_offset");
                orderedOffset.add(offset);
            }
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runOffsetQuery:time", Long.toString(end - start));
        }

        return orderedOffset;
    }

    private PreparedStatement getOffsetsStatement(Connection connection, long startOffset, long endOffset, List<Long> clusterIds, List<String> types) {
        long start = System.currentTimeMillis();
        try {
            PreparedStatement query;

            final Array clusterIdArray = connection.createArrayOf("BIGINT", clusterIds.toArray());

            if(types == null || types.isEmpty()) {
                query = connection.prepareStatement(getOffsetsWithoutTypes());
                query.setArray(1, clusterIdArray);
                query.setLong(2, startOffset);
                query.setLong(3, endOffset);
                query.setLong(4, limit);
            } else {
                String strTypes = String.join(",", types);
                query = connection.prepareStatement(getOffsetsWithTypes());
                query.setArray(1, clusterIdArray);
                query.setString(2, strTypes);
                query.setLong(3, startOffset);
                query.setLong(4, endOffset);
                query.setLong(5, limit);
            }

            return query;
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get offsets statement", exception);
            throw new RuntimeException(exception);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getOffsetsStatement:time", Long.toString(end - start));
        }
    }

    private void close(Connection connection) {
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "close", exception);
            throw new RuntimeException(exception);
        }
    }

    private void commit(Connection connection) {
        try {
            if (!connection.isClosed()) {
             connection.commit();
            }
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "commit", exception);
            throw new RuntimeException(exception);
        }
    }

    // Setting work_mem here to avoid disk based sorts in Postgres
    private void setWorkMem(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getWorkMemQuery())) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private long calculateRetryAfter(long queryTimeMs, int messagesCount) {
        if (messagesCount == 0) {
            return retryAfterWithRandomJitter();
        }

        if (queryTimeMs == 0) {
            return 1;
        }

        // retry after = readers / (connections / query time)
        final double dbThreshold = this.clusterDBPoolSize / (double) queryTimeMs;
        final double retryAfterMs = this.nodeCount / dbThreshold;
        final long calculatedRetryAfter = (long) Math.ceil(retryAfterMs);

        LOG.info("PostgresSqlStorage:calculateRetryAfter:messagesCount", String.valueOf(messagesCount));
        LOG.info("PostgresSqlStorage:calculateRetryAfter:calculatedRetryAfter", String.valueOf(calculatedRetryAfter));

        return Math.min(calculatedRetryAfter, retryAfter);
    }

    private long retryAfterWithRandomJitter() {
        return retryAfter + (long) (retryAfter * Math.random());
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        try (Connection connection = pipeDataSource.getConnection()) {
            return OptionalLong.of(globalLatestOffsetCache.get(connection));
        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get latest offset", exception);
            throw new RuntimeException(exception);
        }
    }

    @Override
    public long getOffsetConsistencySum(long offset, List<String> targetUuids) {
        throw new UnsupportedOperationException("Offset consistency sum isn't implemented yet");
    }

    @Override
    @Deprecated
    public void runVisibilityCheck() {
        try (Connection connection = compactionDataSource.getConnection()) {
            Map<String, Long> messageCountByType = getMessageCountByType(connection);

            messageCountByType.forEach((key, value) ->
                LOG.info("count:type:" + key, String.valueOf(value))
            );
        } catch (SQLException exception) {
            LOG.error("postgres storage", "run visibility check", exception);
            throw new RuntimeException(exception);
        }
    }

    private void runVisibilityCheck(Connection connection) {
        try {
            Map<String, Long> messageCountByType = getMessageCountByType(connection);
            messageCountByType.forEach((key, value) ->
                LOG.info("count:type:" + key, String.valueOf(value))
            );
        } catch (SQLException exception) {
            LOG.error("postgres storage", "run visibility check", exception);
            throw new RuntimeException(exception);
        }
    }

    private Map<String, Long> getMessageCountByType(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        try (PreparedStatement statement = connection.prepareStatement(getMessageCountByTypeQuery())) {
            return runMessageCountByTypeQuery(statement);
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("getMessageCountByType:time", Long.toString(end - start));
        }
    }

    private Map<String, Long> runMessageCountByTypeQuery(final PreparedStatement preparedStatement) throws SQLException {
        Map<String, Long> messageCountByType = new HashMap<>();

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while(resultSet.next()) {
                final String type = resultSet.getString("type");
                final Long typeCount = resultSet.getLong("count");

                messageCountByType.put(type, typeCount);
            }
        }
        return messageCountByType;
    }

    private List<Message> runMessagesQuery(final PreparedStatement query) throws SQLException {
        final List<Message> messages = new ArrayList<>();
        long start = System.currentTimeMillis();

        try (ResultSet rs = query.executeQuery()) {
            while (rs.next()) {
                final String type = rs.getString("type");
                final String key = rs.getString("msg_key");
                final String contentType = rs.getString("content_type");
                final Long offset = rs.getLong("msg_offset");
                final ZonedDateTime created = ZonedDateTime.of(rs.getTimestamp("created_utc").toLocalDateTime(), ZoneId.of("UTC"));
                final String data = rs.getString("data");

                messages.add(new Message(type, key, contentType, offset, created, data, 0L));
            }
        } finally {
            long end = System.currentTimeMillis();
            LOG.info("runMessagesQuery:time", Long.toString(end - start));
        }
        return messages;
    }

    private PreparedStatement getMessagesStatement(
            Connection connection,
        final TreeSet<Long> sortedOffsets) {
        try {
            PreparedStatement query;

            Long[] sortedOffsetsArray = sortedOffsets.toArray(new Long[0]);
            Long[] sortedAndLimitedOffsets = Arrays.copyOfRange(sortedOffsetsArray, 0, Math.min(sortedOffsetsArray.length, limit));

            final Array offsetsPGArray = connection.createArrayOf("BIGINT", sortedAndLimitedOffsets);

            query = connection.prepareStatement(getOptimizedSelectEventsWithoutTypeQuery(maxBatchSize));
            query.setArray(1, offsetsPGArray);

            return query;

        } catch (SQLException exception) {
            LOG.error("postgresql storage", "get message statement", exception);
            throw new RuntimeException(exception);
        }
    }

    public boolean compactAndMaintain(LocalDateTime compactDeletionsThreshold, final boolean compactDeletions) {
        boolean compacted = false;
        try (Connection connection = compactionDataSource.getConnection()) {
            try {
                connection.setAutoCommit(false);
                if (attemptToLock(connection)) {
                    compacted = true;
                    LOG.info("compact and maintain", "obtained lock, compacting");
                    compact(connection, compactDeletionsThreshold, compactDeletions);
                    runVisibilityCheck(connection);

                    //start a new transaction for vacuuming
                    connection.commit();
                    connection.setAutoCommit(true);

                    vacuumAnalyseEvents(connection);
                } else {
                    LOG.info("compact and maintain", "didn't obtain lock");
                }
            } catch (SQLException e) {
                connection.rollback();
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return compacted;
    }

    private void compact(Connection connection, LocalDateTime compactDeletionsThreshold, boolean compactDeletions) throws SQLException {
        if (compactDeletions) {
            setTimeToLiveForDeletions(connection, compactDeletionsThreshold);
        }
        int messageCompacted = compactMessages(connection);

        LOG.info("compaction", "compacted " + messageCompacted + " rows");
    }

    private void setTimeToLiveForDeletions(Connection connection, LocalDateTime compactDeletionsThreshold) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(setTimeToLiveForDeletionsQuery())) {
            statement.setTimestamp(1, Timestamp.valueOf(compactDeletionsThreshold));
            statement.executeUpdate();
        }
    }

    private int compactMessages(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(getCompactionQuery())) {
            return statement.executeUpdate();
        }
    }

    private boolean attemptToLock(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getLockingQuery())) {
            return statement.execute();
        } catch (SQLException e) {
            if(e.getSQLState().equals("55P03")) {
                //lock was not available
                return false;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void vacuumAnalyseEvents(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(getVacuumAnalyseQuery())) {
            statement.executeUpdate();
            LOG.info("vacuum analyse", "vacuum analyse complete");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getOptimizedSelectEventsWithoutTypeQuery(long maxBatchSize) {
        return
            "SELECT type, msg_key, content_type, msg_offset, created_utc, data FROM ( " +
                "SELECT type, msg_key, content_type, msg_offset, created_utc, data, SUM(event_size) OVER (ORDER BY msg_offset ASC) AS running_size " +
                "FROM EVENTS WHERE msg_offset=ANY(?)) " +
            " aggregatedEvents WHERE running_size <= " + maxBatchSize;
    }

    private String getOffsetsWithoutTypes() {
        return
            "SELECT msg_offset FROM unnest(?) as cid, lateral( " +
                "SELECT msg_offset FROM events " +
                "WHERE routing_id = cid " +
                "AND msg_offset >= ? " +
                "AND msg_offset <= ? " +
                "ORDER by msg_offset LIMIT ? " +
            ") as eventsByCluster;";
    }

    private String getOffsetsWithTypes() {
        return
            "SELECT msg_offset FROM unnest(?) as cid, lateral( " +
                "SELECT msg_offset FROM events " +
                "WHERE routing_id = cid " +
                "AND type = ANY (string_to_array(?, ',')) " +
                "AND msg_offset >= ? " +
                "AND msg_offset <= ? " +
                "ORDER by msg_offset LIMIT ? " +
            ") as eventsByCluster;";
    }

    private static String getCompactionQuery() {
        return "DELETE FROM events WHERE time_to_live <= CURRENT_TIMESTAMP;";
    }

    private static String setTimeToLiveForDeletionsQuery() {
        return
        "UPDATE EVENTS SET time_to_live = CURRENT_TIMESTAMP " +
        "FROM (" +
                "SELECT max(msg_offset) as last_delete_offset, msg_key, type, cluster_id FROM EVENTS " +
                "WHERE created_utc <= ? " +
                "AND data IS NULL " +
                "AND time_to_live IS NULL " +
                "AND cluster_id = routing_id " +
                "GROUP BY msg_key, type, cluster_id" +
            ") as LATEST_DELETIONS " +
        "WHERE EVENTS.msg_key = LATEST_DELETIONS.msg_key " +
        "AND EVENTS.type = LATEST_DELETIONS.type " +
        "AND EVENTS.cluster_id = LATEST_DELETIONS.cluster_id " +
        "AND EVENTS.msg_offset <= LATEST_DELETIONS.last_delete_offset;";
    }

    private static String getVacuumAnalyseQuery() {
        return
            " VACUUM ANALYSE EVENTS; " +
            " VACUUM ANALYSE EVENTS_BUFFER; " +
            " VACUUM ANALYSE CLUSTERS; " +
            " VACUUM ANALYSE REGISTRY; " +
            " VACUUM ANALYSE NODE_REQUESTS; ";
    }

    private String getWorkMemQuery() {
        return " SET LOCAL work_mem TO '" + workMemMb + "MB';";
    }

    private String getLockingQuery() {
        return "SELECT * from locks where name='maintenance_lock' FOR UPDATE NOWAIT;";
    }

    private static String getMessageCountByTypeQuery() {
        return "SELECT type, COUNT(type) FROM events GROUP BY type;";
    }
}
