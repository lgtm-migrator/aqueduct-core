package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.*
import org.sqlite.SQLiteErrorCode
import org.sqlite.SQLiteException
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.sql.*
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static com.tesco.aqueduct.pipe.api.OffsetName.GLOBAL_LATEST_OFFSET
import static com.tesco.aqueduct.pipe.api.OffsetName.LOCAL_LATEST_OFFSET

class SQLiteStorageSpec extends Specification {

    private static final def LIMIT = 1000
    private static final def BATCH_SIZE = 1000L
    private static final def CONNECTION_URL = "jdbc:sqlite:aqueduct-pipe.db"

    def dataSource
    @Shared def sqliteStorage

    def setup() {
        dataSource = Mock(DataSource)
        dataSource.getConnection() >> { DriverManager.getConnection(CONNECTION_URL) }
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)
        //make sure everything is clear before starting
        sqliteStorage.deleteAll()
    }

    def 'throws an exception if a problem with the database arises when reading messages'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                { throw new SQLException() }
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        when: 'messages are requested to be read from a given offset'
        sqliteStorage.read([], 0, "abc")

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing messages'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                { throw new SQLException() }
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        when: 'the latest offset is requested'
        sqliteStorage.write(message(1))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if a problem with the database arises when writing latest offset'() {
        given: 'a data store controller exists with a broken connection url'
        dataSource = Mock(DataSource)
        dataSource.getConnection() >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                DriverManager.getConnection(CONNECTION_URL) >>
                { throw new SQLException() }
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        when: 'the latest offset is written'
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(100)))

        then: 'a runtime exception is thrown'
        thrown(RuntimeException)
    }

    def 'throws an exception if no offset value provided when writing latest offset'() {
        given: 'a data store controller and sqlite storage exists'
        def dataSource = Mock(DataSource)
        dataSource.getConnection() >>> [
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL)
        ]

        def sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        when: 'the latest offset is written with empty value'
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.empty()))

        then: 'no such element exception is thrown'
        thrown(NoSuchElementException)
    }

    def 'retry read time limit should be activated only when the amount of received messages is 0'() {
        given: 'a data store controller'
        def retryAfter = 10
        def sqliteStorage = new SQLiteStorage(dataSource, testLimit, retryAfter, BATCH_SIZE)

        when: 'the retry after is calculated'
        def actualRetryAfter = sqliteStorage.calculateRetryAfter(messageCount)

        then: 'the calculated retry after is 0 if more than 0 messages were returned'
        actualRetryAfter == expectedRetryAfter

        where:
        testLimit | messageCount | expectedRetryAfter
        100       | 50           | 0
        100       | 0            | 10
        100       | 99           | 0
        99        | 98           | 0
        99        | 0            | 10
        100       | 101          | 0
        101       | 102          | 0
        100       | 100          | 0
        99        | 100          | 0
        0         | 1            | 0
    }

    def 'concurrent write of global latest offset doesnt cause inconsistencies'() {
        given: 'sqlite storage with a message and offsets loaded'
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)
        sqliteStorage.write(message(1L))
        sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(1L)))
        sqliteStorage.write(PipeState.UP_TO_DATE)

        when: 'read is called and write happens concurrently'
        ExecutorService pool = Executors.newFixedThreadPool(1)
        pool.execute {
            //sleep just long enough for the offset write to run during the read
            sleep 5
            sqliteStorage.write(new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(10L)))
        }
        MessageResults messageResults = sqliteStorage.read(["some-type"], 0, "na")

        then: 'global offset that is read is not the concurrently written one, but a consistent one'
        messageResults.globalLatestOffset.asLong == 1L
    }

    def "Illegal argument error when pipe entity is null"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write((PipeEntity) null)

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when pipe entity data is null"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity(null, null, null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when messages within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity([], null, null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when messages and offsets within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity([], [], null))

        then:
        thrown(IllegalArgumentException)
    }

    def "Illegal argument error when only offsets within pipe entity is empty"() {
        given:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when:
        sqliteStorage.write(new PipeEntity(null, [], null))

        then:
        thrown(IllegalArgumentException)
    }

    def "running management tasks attempt vacuum and checkpoint onto sqlite storage"() {
        given: "mock datasource"
        def dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        def statement = Mock(PreparedStatement)

        and: "data source giving out connection on demand"
        dataSource.getConnection() >>> [
                // first four calls are for setting up database schema
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                DriverManager.getConnection(CONNECTION_URL),
                connection,
                connection
        ]

        and:
        sqliteStorage = new SQLiteStorage(dataSource, 1, 1, 1)

        when: "tuning is invoked"
        sqliteStorage.runMaintenanceTasks()

        then: "vacuum is attempted"
        1 * connection.prepareStatement(SQLiteQueries.VACUUM_DB) >> statement
        1 * statement.execute()
        1 * statement.getUpdateCount() >> 0

        and: "checkpoint is attempted"
        1 * connection.prepareStatement(SQLiteQueries.CHECKPOINT_DB) >> statement
        1 * statement.execute()
    }

    def 'calculate max offset throws Runtime exception if error during processing'() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        def statement = Mock(PreparedStatement)

        dataSource.getConnection() >>>
                [
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        connection,
                ]
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "error thrown during query execution"
        dataSource.getConnection() >> connection
        connection.prepareStatement(_ as String) >> statement
        statement.executeQuery() >> { throw new SQLException() }

        when:
        sqliteStorage.getMaxOffsetForConsumers(["type"])

        then:
        def exception = thrown(RuntimeException)
    }

    def "nothing is committed in compactUpTo if either compactMessages throws an exception"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        def compactMessagesStatement = Mock(PreparedStatement)
        def compactDeletionsStatement = Mock(PreparedStatement)

        dataSource.getConnection() >>>
                [
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        connection
                ]

        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "exception thrown during compact messages"
        connection.prepareStatement(SQLiteQueries.COMPACT) >> compactMessagesStatement
        compactMessagesStatement.executeUpdate() >> { throw new SQLException() }

        and: "no exception thrown during compact deletions"
        connection.prepareStatement(SQLiteQueries.COMPACT_DELETIONS) >> compactDeletionsStatement
        compactDeletionsStatement.executeUpdate() >> 1L

        when:
        sqliteStorage.compactUpTo(ZonedDateTime.now(), ZonedDateTime.now(), true)

        then:
        def exception = thrown(RuntimeException)
        exception.getCause().class == SQLException

        and:
        0 * connection.commit()

        and:
        1 * connection.rollback()
    }

    def "nothing is committed in compactUpTo if either compactDeletions throws an exception"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        def compactMessagesStatement = Mock(PreparedStatement)
        def compactDeletionsStatement = Mock(PreparedStatement)

        dataSource.getConnection() >>>
                [
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        DriverManager.getConnection(CONNECTION_URL),
                        connection
                ]

        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "no exception thrown during compact messages"
        connection.prepareStatement(SQLiteQueries.COMPACT) >> compactMessagesStatement
        compactMessagesStatement.executeUpdate() >> 1L

        and: "exception thrown during compact deletions"
        connection.prepareStatement(SQLiteQueries.COMPACT_DELETIONS) >> compactDeletionsStatement
        compactDeletionsStatement.executeUpdate() >> { throw new SQLException() }

        when:
        sqliteStorage.compactUpTo(ZonedDateTime.now(), ZonedDateTime.now(), true)

        then:
        def exception = thrown(RuntimeException)
        exception.getCause().class == SQLException

        and:
        0 * connection.commit()

        and:
        1 * connection.rollback()
    }

    def "is data base corrupted returns true if result is not OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "NOT_OK"

        when: "run full integrity check"
        def result = SQLiteStorage.isDBCorrupted(dataSource);

        then:
        result
    }

    def "is data base corrupted returns false if result is  OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "ok"

        when: "run full integrity check"
        def result = SQLiteStorage.isDBCorrupted(dataSource);

        then:
        !result
    }

    def "full integrity check returns true if result is OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "ok"

        when: "run full integrity check"
        def result = sqliteStorage.runFullIntegrityCheck()

        then:
        result
    }

    def "full integrity check returns false if result is not OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "NOT_OK"

        when: "run full integrity check"
        def result = sqliteStorage.runFullIntegrityCheck()

        then:
        !result
    }

    def "integrity check returns false if result is not OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.QUICK_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "NOT_OK"

        when: "run integrity check"
        sqliteStorage.runVisibilityCheck();

        then:
        sqliteStorage.isIntegrityCheckPassed(connection) == false
    }

    def "integrity check returns true if result is not OK"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an 'ok' result"
        connection.prepareStatement(SQLiteQueries.QUICK_INTEGRITY_CHECK) >> preparedStatement
        def resultSet = Mock(ResultSet)
        preparedStatement.executeQuery() >> resultSet
        resultSet.getString(1) >> "ok"

        when: "run integrity check"
        sqliteStorage.runVisibilityCheck();

        then:
        sqliteStorage.isIntegrityCheckPassed(connection)==true
  
    }

    def "full integrity should rethrow RuntimeException on SQLException"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an SQLException"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        preparedStatement.executeQuery() >> { throw new SQLException() }

        when: "run full integrity check"
        sqliteStorage.runFullIntegrityCheck()

        then:
        def exception = thrown(RuntimeException)
        exception.getCause().class == SQLException
    }

    def "full integrity should return false for a SQL Corrupt Exception"() {
        given: "mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a mocked prepared statement"
        def preparedStatement = Mock(PreparedStatement)
        connection.prepareStatement(*_) >> preparedStatement

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "an SQLException"
        connection.prepareStatement(SQLiteQueries.FULL_INTEGRITY_CHECK) >> preparedStatement
        preparedStatement.executeQuery() >> { throw new SQLiteException("", SQLiteErrorCode.SQLITE_CORRUPT) }

        when: "run full integrity check"
        def result = sqliteStorage.runFullIntegrityCheck()

        then:
        !result
    }

    @Unroll
    def "full integrity should return false when #method has encountered an SQLITE_CORRUPT exception"() {
        given: "a mocked datasource"
        dataSource = Mock(DataSource)
        def connection = Mock(Connection)
        dataSource.getConnection() >> connection

        and: "a prepared statement that throws SQLite Corrupt exception"
        def setupPreparedStatement = Mock(PreparedStatement)
        def integrityCheckPreparedStatement = Mock(PreparedStatement)
        def throwingPreparedStatement = Mock(PreparedStatement) {
            executeQuery() >> { throw new SQLiteException("", SQLiteErrorCode.SQLITE_CORRUPT) }
            executeBatch() >> { throw new SQLiteException("", SQLiteErrorCode.SQLITE_CORRUPT) }
            execute() >> { throw new SQLiteException("", SQLiteErrorCode.SQLITE_CORRUPT) }
        }
        def otherPreparedStatement = Mock(PreparedStatement) {
            executeQuery() >> Mock(ResultSet) {
                next() >> false
            }
        }
        connection.prepareStatement(_) >> {
            args ->
                {
                    def query = args[0]

                    switch (query) {
                        case SQLiteQueries.CREATE_EVENT_TABLE:
                        case SQLiteQueries.OFFSET_TABLE:
                        case SQLiteQueries.PIPE_STATE_TABLE:
                        case SQLiteQueries.DROP_TYPES_INDEX:
                            return setupPreparedStatement
                        case SQLiteQueries.FULL_INTEGRITY_CHECK:
                            return integrityCheckPreparedStatement
                        case failingQuery:
                            return throwingPreparedStatement;
                        default:
                            return otherPreparedStatement
                    }
                }
        }

        and: "integrity check SQL query returns OK"
        integrityCheckPreparedStatement.executeQuery() >> Mock(ResultSet) {
            getString(1) >> "ok"
        }

        and: "an SQLite storage"
        sqliteStorage = new SQLiteStorage(dataSource, LIMIT, 10, BATCH_SIZE)

        and: "a query which encounters corruption is called"
        try {
            corruptQuery()
        } catch (Exception ignored) {
            // do nothing
        }

        when: "we run full integrity check"
        def result = sqliteStorage.runFullIntegrityCheck()

        then: "the integrity check fails"
        !result

        where:
        method                               | corruptQuery                                                                        | failingQuery
        "read:getOffset"                     | { sqliteStorage.read([], 0, "") }                                                   | SQLiteQueries.getOffset(GLOBAL_LATEST_OFFSET)
        "read:GET_PIPE_STATE"                | { sqliteStorage.read([], 0, "") }                                                   | SQLiteQueries.GET_PIPE_STATE
        "read:getReadEvent"                  | { sqliteStorage.read([], 0, "") }                                                   | SQLiteQueries.getReadEvent(0, 90000)
        "getPipeState"                       | { sqliteStorage.getPipeState() }                                                    | SQLiteQueries.GET_PIPE_STATE
        "getOffsetConsistencySum"            | { sqliteStorage.getOffsetConsistencySum(1, []) }                                    | SQLiteQueries.OFFSET_CONSISTENCY_SUM
        "getOffset:CHOOSE_MAX_OFFSET"        | { sqliteStorage.getOffset(OffsetName.MAX_OFFSET_PREVIOUS_HOUR) }                    | SQLiteQueries.CHOOSE_MAX_OFFSET
        "getOffset:getOffset"                | { sqliteStorage.getOffset(OffsetName.PIPE_OFFSET) }                                 | SQLiteQueries.getOffset(OffsetName.PIPE_OFFSET)
        "write:Messages:INSERT_EVENT"        | { sqliteStorage.write([message()]) }                                                | SQLiteQueries.INSERT_EVENT
        "write:PipeEntity:INSERT_EVENT"      | { sqliteStorage.write(new PipeEntity([message()], [], PipeState.UP_TO_DATE)) }      | SQLiteQueries.INSERT_EVENT
        "write:PipeEntity:UPSERT_OFFSET"     | { sqliteStorage.write(new PipeEntity([], [offsetEntity()], PipeState.UP_TO_DATE)) } | SQLiteQueries.UPSERT_OFFSET
        "write:PipeEntity:UPSERT_PIPE_STATE" | { sqliteStorage.write(new PipeEntity([], [], PipeState.UP_TO_DATE)) }               | SQLiteQueries.UPSERT_PIPE_STATE
        "write:Message:INSERT_EVENT"         | { sqliteStorage.write(message()) }                                                  | SQLiteQueries.INSERT_EVENT
        "write:OffsetEntity:UPSERT_OFFSET"   | { sqliteStorage.write(new OffsetEntity(LOCAL_LATEST_OFFSET, OptionalLong.of(1))) }  | SQLiteQueries.UPSERT_OFFSET
        "write:PipeState:UPSERT_PIPE_STATE"  | { sqliteStorage.write(PipeState.OUT_OF_DATE) }                                      | SQLiteQueries.UPSERT_PIPE_STATE
        "runVisibilityCheck"                 | { sqliteStorage.runVisibilityCheck() }                                              | SQLiteQueries.QUICK_INTEGRITY_CHECK
    }

    private static def message(long offset, String type) {
        def timeNow = ZonedDateTime.now().withZoneSameInstant(ZoneId.of("UTC"))
        return new Message(
                type,
                "some-key",
                "text/plain",
                offset,
                timeNow,
                null
        )
    }

    private static def message(long offset) {
        return message(offset, "some-type")
    }

    private static def message() {
        return message(1L)
    }

    private static def offsetEntity() {
        return new OffsetEntity(GLOBAL_LATEST_OFFSET, OptionalLong.of(0L))
    }
}
