package com.tesco.aqueduct.pipe.storage

import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

import static java.sql.DriverManager.getConnection

@MicronautTest(rebuildContext = true)
@Property(name="micronaut.caches.latest-offset-cache.expire-after-write", value="1h")
class GlobalLatestOffsetCacheIntegrationSpec extends Specification {

    @Shared @ClassRule
    SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()

    @AutoCleanup
    Sql sql

    @Inject
    private ApplicationContext applicationContext

    private GlobalLatestOffsetCache globalLatestOffsetCache

    void setup() {
        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        sql.execute("""
        DROP TABLE IF EXISTS OFFSETS;

        CREATE TABLE OFFSETS(
            name varchar PRIMARY KEY,
            value BIGINT NOT NULL
        );
        """)


        globalLatestOffsetCache = applicationContext.getBean(GlobalLatestOffsetCache)
    }

    def "Offset is cached once fetched from db storage"() {
        given:
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and:
        insertGlobalLatestOffset(100)

        when:
        globalLatestOffsetCache.get(connection) == 100

        and:
        insertGlobalLatestOffset(101)

        then:
        globalLatestOffsetCache.get(connection) == 100

    }

    def "max offset is fetched from events table when not looking in cache"() {
        given: "connection to database"
        def connection = getConnection(pg.embeddedPostgres.getJdbcUrl("postgres", "postgres"))

        and: "some events exists"
        insertGlobalLatestOffset(3)
        insertGlobalLatestOffset(4)

        when:
        def globalLatestOffset = globalLatestOffsetCache.get(connection)

        then:
        globalLatestOffset == 4
    }

    def "returns 0 if nothing is returned from getGlobalLatestOffset query"() {
        given:
        def connection = Mock(Connection)
        def preparedStatement = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)

        when:
        def globalLatestOffset = globalLatestOffsetCache.get(connection)

        then:
        1 * connection.prepareStatement(*_) >> preparedStatement
        1 * preparedStatement.executeQuery() >> resultSet
        1 * resultSet.next() >> false

        globalLatestOffset == 0
    }

    void insertGlobalLatestOffset(Long offset) {
        sql.execute("INSERT INTO OFFSETS (name, value) VALUES ('global_latest_offset', ?) ON CONFLICT(name) DO UPDATE SET VALUE = ?;", offset, offset)
    }
}
