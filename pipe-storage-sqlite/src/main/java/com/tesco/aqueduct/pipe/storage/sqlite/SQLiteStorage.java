package com.tesco.aqueduct.pipe.storage.sqlite;

import com.tesco.aqueduct.pipe.api.*;
import com.tesco.aqueduct.pipe.logger.PipeLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET;
import static com.tesco.aqueduct.pipe.storage.sqlite.SQLiteQueries.maxOffsetForConsumersQuery;

public class SQLiteStorage implements DistributedStorage {

    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(SQLiteStorage.class));
    private static final Logger DEBUG_LOGGER = LoggerFactory.getLogger("pipe-debug-logger");

    private final int limit;
    private final int retryAfterMs;
    private final long maxBatchSize;
    private final DataSource dataSource;

    private boolean corrupt = false;

    public SQLiteStorage(
            final DataSource dataSource,
            final int limit,
            final int retryAfterMs,
            final long maxBatchSize
    ) {
        this.dataSource = dataSource;
        this.limit = limit;
        this.retryAfterMs = retryAfterMs;
        this.maxBatchSize = maxBatchSize + (((long) Message.MAX_OVERHEAD_SIZE) * limit);

        createEventTableIfNotExists();
        createOffsetTableIfNotExists();
        createPipeStateTableIfNotExists();
        dropIndexOnTypes();
    }

    private void createEventTableIfNotExists() {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CREATE_EVENT_TABLE)) {
                return statement.execute();
            }
        });
    }

    private void createOffsetTableIfNotExists() {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.OFFSET_TABLE)) {
                return statement.execute();
            }
        });
    }

    private void createPipeStateTableIfNotExists() {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.PIPE_STATE_TABLE)) {
                return statement.execute();
            }
        });
    }

    private void dropIndexOnTypes() {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DROP_TYPES_INDEX)) {
                return statement.execute();
            }
        });
    }

    @Override
    public MessageResults read(final List<String> types, final long offset, final String locationUuid) {
        return execute(connection -> {
            connection.setAutoCommit(false);

            OptionalLong globalLatestOffset = getOffset(connection, GLOBAL_LATEST_OFFSET);
            PipeState pipeState = getPipeState(connection);
            List<Message> retrievedMessages = getMessages(connection, types, offset);

            if (retrievedMessages.isEmpty() && pipeState.equals(PipeState.UP_TO_DATE) && globalLatestOffset.isPresent()) {
                DEBUG_LOGGER.info("Read from: " + offset + ", Global Latest Offset: " + globalLatestOffset.getAsLong() + ", PipeState: UP_TO_DATE, Messages: [ ]");
            }

            return new MessageResults(retrievedMessages, calculateRetryAfter(retrievedMessages.size()), globalLatestOffset, pipeState);
        });
    }

    @Override
    public PipeState getPipeState() {
        return execute(this::getPipeState);
    }

    @Override
    public long getOffsetConsistencySum(long offset, List<String> targetUuids) {
        return execute(connection -> getOffsetConsistencySumBasedOn(offset, connection));
    }

    @Override
    public OptionalLong getOffset(OffsetName offsetName) {
        if (offsetName == OffsetName.MAX_OFFSET_PREVIOUS_HOUR) {
            return getMaxOffsetInPreviousHour(ZonedDateTime.now(ZoneId.of("UTC")));
        }

        return execute(connection -> getOffset(connection, offsetName));
    }

    @Override
    public void write(final Iterable<Message> messages) {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.INSERT_EVENT)) {
                connection.setAutoCommit(false);
                insertMessagesAsBatch(statement, messages);
                connection.commit();

                return true;
            }
        });
    }

    @Override
    public void write(final PipeEntity pipeEntity) {
        if (pipeEntity == null || nothingToWriteIn(pipeEntity)) {
            throw new IllegalArgumentException("Pipe entity data cannot be null.");
        }

        execute(connection -> {
            try (final PreparedStatement insertMessageStmt = connection.prepareStatement(SQLiteQueries.INSERT_EVENT);
                 final PreparedStatement upsertOffsetStmt = connection.prepareStatement(SQLiteQueries.UPSERT_OFFSET);
                 final PreparedStatement upsertPipeStateStmt = connection.prepareStatement(SQLiteQueries.UPSERT_PIPE_STATE)) {

                // Start transaction
                connection.setAutoCommit(false);

                // Insert messages
                if (pipeEntity.getMessages() != null && !pipeEntity.getMessages().isEmpty()) {
                    insertMessagesAsBatch(insertMessageStmt, pipeEntity.getMessages());
                }

                // Insert offsets
                if (pipeEntity.getOffsets() != null && !pipeEntity.getOffsets().isEmpty()) {
                    upsertOffsetsAsBatch(upsertOffsetStmt, pipeEntity.getOffsets());
                }

                // Insert pipe state
                if (pipeEntity.getPipeState() != null) {
                    upsertPipeState(upsertPipeStateStmt, pipeEntity.getPipeState());
                }

                // commit transaction
                connection.commit();

                return true;
            } catch (Exception exception) {
                rollback(connection);
                throw exception;
            }
        });
    }

    @Override
    public void write(final Message message) {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.INSERT_EVENT)) {
                setStatementParametersForInsertMessageQuery(statement, message);
                return statement.execute();
            }
        });
    }

    @Override
    public void write(OffsetEntity offset) {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.UPSERT_OFFSET)) {
                setStatementParametersForOffsetQuery(statement, offset);
                return statement.execute();
            }
        });
    }

    @Override
    public void write(PipeState pipeState) {
        execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.UPSERT_PIPE_STATE)) {
                upsertPipeState(statement, pipeState);

                return true;
            }
        });
    }

    @Override
    public void runVisibilityCheck() {
        execute(connection -> {
                if (!isIntegrityCheckPassed(connection)) {
                    reindex(connection);
                    if(isIntegrityCheckPassed(connection))
                    {
                        LOG.info("integrity check", "integrity check passed after rebuild");
                    }
                    else {
                        LOG.info("integrity check", "integrity check failed after rebuild");
                    }
                    return false;
                }
                return true;
            }
        );
    }

    public boolean  isIntegrityCheckPassed(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.QUICK_INTEGRITY_CHECK);
             ResultSet resultSet = statement.executeQuery()) {
             String result = resultSet.getString(1);
             if (!"ok".equals(result)) {
                    LOG.error("integrity check", "integrity check failed", result);
                    return false;
             }
             return true;
            }
    }

    @Override
    public Long getMaxOffsetForConsumers(List<String> types) {
        return execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(maxOffsetForConsumersQuery(types.size()))) {
                for (int i = 0; i < types.size(); i++) {
                    statement.setString(i + 1, types.get(i));
                }

                ResultSet resultSet = statement.executeQuery();

                return resultSet.next() ?
                        resultSet.getLong(1) :
                        0L;
            }
        });
    }

    @Override
    public void deleteAll() {
        execute(connection -> {
            deleteEvents(connection);
            deleteOffsets(connection);
            deletePipeState(connection);
            vacuumDatabase(connection);
            checkpointWalFile(connection);

            return true;
        });
    }

    public void runMaintenanceTasks() {
        execute(connection -> {
            vacuumDatabase(connection);
            checkpointWalFile(connection);

            return true;
        });
    }

    public boolean runFullIntegrityCheck() {
        if (corrupt) return false;

        return execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK);
                 ResultSet resultSet = statement.executeQuery()) {

                String result = resultSet.getString(1);
                if (!result.equals("ok")) {
                    LOG.error("full integrity check", "full integrity check failed", result);
                    return false;
                }

                return true;
            } catch (SQLiteException exception) {
                if (SQLiteErrorCode.SQLITE_CORRUPT.equals(exception.getResultCode())) {
                    return false;
                }

                throw exception;
            }
        });
    }

    public void compactUpTo(
            final ZonedDateTime compactionThreshold,
            final ZonedDateTime deletionCompactionThreshold,
            final boolean compactDeletions
    ) {
        execute(connection -> {
            runCompactionInTransaction(compactionThreshold, deletionCompactionThreshold, connection, compactDeletions);

            return true;
        });
    }

    private <T> T execute(ConnectionFunction<T> connectionFunction) {
        try (Connection connection = dataSource.getConnection()) {
            return connectionFunction.apply(connection);
        } catch (SQLiteException exception) {
            LOG.error("execute", "failed to execute SQLite query", exception);

            if (SQLiteErrorCode.SQLITE_CORRUPT.equals(exception.getResultCode())) {
                dbCorrupted();
            }

            throw new RuntimeException(exception);
        } catch (SQLException exception) {
            LOG.error("execute", "failed to execute SQLite query", exception);
            throw new RuntimeException(exception);
        }
    }

    private void dbCorrupted()
    {
        corrupt=true;
    }

    private List<Message> getMessages(Connection connection, List<String> types, long offset) throws SQLException {
        List<Message> retrievedMessages = new ArrayList<>();
        int typesCount = types == null ? 0 : types.size();

        try (PreparedStatement statement = connection
                .prepareStatement(SQLiteQueries.getReadEvent(typesCount, maxBatchSize))) {

            int parameterIndex = 1;
            statement.setLong(parameterIndex++, offset);

            for (int i = 0; i < typesCount; i++, parameterIndex++) {
                statement.setString(parameterIndex, types.get(i));
            }

            statement.setLong(parameterIndex, limit);

            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    retrievedMessages.add(mapRetrievedMessageFromResultSet(resultSet));
                }
            }
        }

        return retrievedMessages;
    }

    private PipeState getPipeState(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.GET_PIPE_STATE)) {
            ResultSet resultSet = statement.executeQuery();

            return resultSet.next() ?
                    PipeState.valueOf(resultSet.getString("value")) :
                    PipeState.UNKNOWN;
        }
    }

    private int calculateRetryAfter(final int messageCount) {
        return messageCount > 0 ? 0 : retryAfterMs;
    }

    private Message mapRetrievedMessageFromResultSet(final ResultSet resultSet) throws SQLException {
        Message retrievedMessage;
        final ZonedDateTime time = ZonedDateTime.of(
                resultSet.getTimestamp("created_utc").toLocalDateTime(),
                ZoneId.of("UTC")
        );

        retrievedMessage = new Message(
                resultSet.getString("type"),
                resultSet.getString("msg_key"),
                resultSet.getString("content_type"),
                resultSet.getLong("msg_offset"),
                time,
                resultSet.getString("data"),
                resultSet.getLong("event_size")
        );

        return retrievedMessage;
    }

    private OptionalLong getOffset(Connection connection, OffsetName offsetName) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.getOffset(offsetName))) {
            ResultSet resultSet = statement.executeQuery();

            return resultSet.next() ?
                    OptionalLong.of(resultSet.getLong("value")) :
                    OptionalLong.empty();
        }
    }

    private boolean nothingToWriteIn(PipeEntity pipeEntity) {
        return pipeEntity.getPipeState() == null
                && (pipeEntity.getOffsets() == null || pipeEntity.getOffsets().isEmpty())
                && (pipeEntity.getMessages() == null || pipeEntity.getMessages().isEmpty());
    }

    private void upsertPipeState(PreparedStatement upsertPipeStateStmt, PipeState pipeState) throws SQLException {
        upsertPipeStateStmt.setString(1, "pipe_state");
        upsertPipeStateStmt.setString(2, pipeState.toString());
        upsertPipeStateStmt.setString(3, pipeState.toString());

        upsertPipeStateStmt.execute();
    }

    private void rollback(Connection connection) {
        if (connection != null) {
            try {
                if (!connection.getAutoCommit()) {
                    connection.rollback();
                }
            } catch (SQLException sqlException) {
                LOG.error("write", "Could not rollback pipe entity transaction.", sqlException);
                throw new RuntimeException(sqlException);
            }
        }
    }

    private void upsertOffsetsAsBatch(PreparedStatement insertOffsetStmt, List<OffsetEntity> offsets)
            throws SQLException {
        for (final OffsetEntity offset : offsets) {
            setStatementParametersForOffsetQuery(insertOffsetStmt, offset);
            insertOffsetStmt.addBatch();
        }

        insertOffsetStmt.executeBatch();
    }

    private void insertMessagesAsBatch(PreparedStatement insertMessageStmt, Iterable<Message> messages)
            throws SQLException {
        for (final Message message : messages) {
            setStatementParametersForInsertMessageQuery(insertMessageStmt, message);
            insertMessageStmt.addBatch();
        }

        insertMessageStmt.executeBatch();
    }

    private void setStatementParametersForOffsetQuery(PreparedStatement insertOffsetStmt, OffsetEntity offset)
            throws SQLException {
        insertOffsetStmt.setString(1, offset.getName().toString());
        insertOffsetStmt.setLong(2, offset.getValue().getAsLong());
        insertOffsetStmt.setLong(3, offset.getValue().getAsLong());
    }

    private void reindex(Connection connection) {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.REINDEX_EVENTS)) {
            statement.execute();
            LOG.info("reindex", "reindexed event table");
        } catch (SQLException exception) {
            throw new RuntimeException(exception);
        }
    }

    private long getOffsetConsistencySumBasedOn(long offsetThreshold, Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.OFFSET_CONSISTENCY_SUM)) {
            statement.setLong(1, offsetThreshold);
            statement.setLong(2, offsetThreshold);
            return queryResult(statement);
        }
    }

    private OptionalLong getMaxOffsetInPreviousHour(ZonedDateTime currentTime) {
        return execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CHOOSE_MAX_OFFSET)) {
                Timestamp threshold = Timestamp.valueOf(currentTime.withMinute(0).withSecond(0).withNano(0).toLocalDateTime());
                statement.setTimestamp(1, threshold);

                return OptionalLong.of(queryResult(statement));
            }
        });
    }

    private long queryResult(PreparedStatement statement) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery()) {
            return resultSet.getLong(1);
        }
    }

    private void deleteEvents(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_EVENTS)) {
            statement.execute();
            LOG.info("deleteAllEvents", String.format("Delete events result: %d", statement.getUpdateCount()));
        }
    }

    private void deleteOffsets(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_OFFSETS)) {
            statement.execute();
            LOG.info("deleteOffsets", String.format("Delete offsets result: %d", statement.getUpdateCount()));
        }
    }

    private void deletePipeState(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.DELETE_PIPE_STATE)) {
            statement.execute();
            LOG.info("deletePipeState", String.format("Delete pipe state result: %d", statement.getUpdateCount()));
        }
    }

    private void vacuumDatabase(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.VACUUM_DB)) {
            statement.execute();
            LOG.info("vacuumDatabase", String.format("Vacuum result: %d", statement.getUpdateCount()));
        }
    }

    private void checkpointWalFile(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.CHECKPOINT_DB)) {
            statement.execute();
            LOG.info("checkPointDatabase", "checkpointed database");
        }
    }

    private void runCompactionInTransaction(ZonedDateTime compactionThreshold,
                                            ZonedDateTime deletionCompactionThreshold,
                                            Connection connection,
                                            boolean compactionDeletions) throws SQLException {
        connection.setAutoCommit(false);
        try {
            int compactedCount = compactMessagesOlderThan(compactionThreshold, connection);
            int deletionCompactedCount = 0;

            if (compactionDeletions) {
                deletionCompactedCount = compactDeletionsOlderThan(deletionCompactionThreshold, connection);
            }

            connection.commit();
            LOG.info("compaction", "compacted " + (compactedCount + deletionCompactedCount) + " rows");
        } catch (SQLException exception) {
            connection.rollback();
            throw exception;
        }
    }

    private int compactDeletionsOlderThan(ZonedDateTime deletionCompactionThreshold, Connection connection)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.COMPACT_DELETIONS)) {
            Timestamp deletionCompactThreshold = Timestamp.valueOf(deletionCompactionThreshold.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
            statement.setTimestamp(1, deletionCompactThreshold);
            return statement.executeUpdate();
        }
    }

    private int compactMessagesOlderThan(ZonedDateTime compactionThreshold, Connection connection)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SQLiteQueries.COMPACT)) {
            Timestamp compactThreshold = Timestamp.valueOf(compactionThreshold.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime());
            statement.setTimestamp(1, compactThreshold);
            statement.setTimestamp(2, compactThreshold);
            return statement.executeUpdate();
        }
    }

    private void setStatementParametersForInsertMessageQuery(final PreparedStatement statement,
                                                             final Message message) throws SQLException {
        try {
            statement.setLong(1, message.getOffset());
            statement.setString(2, message.getKey());
            statement.setString(3, message.getContentType());
            statement.setString(4, message.getType());
            statement.setTimestamp(5, Timestamp.valueOf(message.getCreated().withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime()));
            statement.setString(6, message.getData());
            statement.setInt(7, JsonHelper.toJson(message).length());
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private interface ConnectionFunction<T> {
        T apply(Connection connection) throws SQLException;
    }
}