import com.opentable.db.postgres.junit.EmbeddedPostgresRules
import com.opentable.db.postgres.junit.SingleInstancePostgresRule
import com.tesco.aqueduct.pipe.api.LocationResolver
import com.tesco.aqueduct.pipe.api.Message
import groovy.sql.Sql
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.ClassRule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource
import java.time.LocalDateTime

import static org.hamcrest.Matchers.equalTo

class PipeCloudServerIntegrationSpec extends Specification {
    static LocalDateTime time = LocalDateTime.parse("2018-12-20T15:13:01")

    // Starts real PostgreSQL database, takes some time to create it and clean it up.
    @Shared @ClassRule SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance()
    @AutoCleanup Sql sql
    @AutoCleanup("stop") ApplicationContext context

    DataSource dataSource
    LocationResolver locationResolver

    def setup() {

        sql = new Sql(pg.embeddedPostgres.postgresDatabase.connection)

        dataSource = Mock()
        locationResolver = Mock()

        dataSource.connection >>> [
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection,
            new Sql(pg.embeddedPostgres.postgresDatabase.connection).connection
        ]
        locationResolver.resolve(_) >> []

        //TODO: remove "tags" once they are removed from SCHEMA
        sql.execute("""
          DROP TABLE IF EXISTS EVENTS;
          
          CREATE TABLE EVENTS(
            msg_offset bigint PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY NOT NULL,
            msg_key varchar NOT NULL, 
            content_type varchar NOT NULL, 
            type varchar NOT NULL, 
            created_utc timestamp NOT NULL,
            tags JSONB NULL, 
            data text NULL,
            event_size int NOT NULL
          );
        """)

        context = ApplicationContext
            .build()
            .properties(
                "pipe.server.url": "http://cloud.pipe",
                "persistence.read.limit": 1000,
                "persistence.read.retry-after": 10000,
                "persistence.read.max-batch-size": "10485760"
            )
            .mainClass(EmbeddedServer)
            .build()
            .registerSingleton(DataSource, dataSource, Qualifiers.byName("postgres"))
            .registerSingleton(LocationResolver, locationResolver)


        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    def cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "once I inserted some documents in database I can read them from the pipe" () {
        given:
        insert(100,  "a", "contentType", "type1", time, "data")
        insert(101, "b", "contentType", "type1", time, null)

        when:
        def request = RestAssured.get("/pipe/100")

        then:
        request
            .then()
            // this is bit fragile on purpose, it will fail on small changes to format of Json
                .content(equalTo("""
            [
                {"type":"type1","key":"a","contentType":"contentType","offset":"100","created":"2018-12-20T15:13:01Z","data":"data"},
                {"type":"type1","key":"b","contentType":"contentType","offset":"101","created":"2018-12-20T15:13:01Z"}
            ]
            """.replaceAll("\\s", "")))
    }

    def "state endpoint returns true and the correct offset"() {
        given: "A data store with offset 100"
        insert(100,  "a", "contentType", "type1", time, "data")

        when: "we call to get state"
        def request = RestAssured.get("/pipe/state?type=type1")

        then: "response is correct"
        def response = """{"upToDate":true,"localOffset":"100"}"""
        request
                .then()
                .statusCode(200)
                .body(equalTo(response))
    }

    void insert(Long msg_offset, String msg_key, String content_type, String type, LocalDateTime created, String data) {

        sql.execute(
            "INSERT INTO EVENTS(msg_offset, msg_key, content_type, type, created_utc, data, event_size) VALUES(?,?,?,?,?,?,?);",
            msg_offset, msg_key, content_type, type, created, data, 0
        )
    }

    void insert(Message message) {
        insert(
            message.getOffset(),
            message.getKey(),
            message.getContentType(),
            message.getType(),
            message.getCreated().toLocalDateTime(),
            message.getData()
        )
    }
}