package com.tesco.aqueduct.pipe.http

import com.tesco.aqueduct.pipe.api.*
import io.micronaut.context.annotation.Property
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import io.restassured.RestAssured
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.inject.Inject
import javax.inject.Named
import java.time.ZonedDateTime

import static java.util.OptionalLong.of

// It is not possible to have two integration tests within same @MicronautTest relying on rate limiter config
// hence having a separate test for cluster change rate limiter functionality because PipeReadControllerIntegrationSpec
// has bootstrap test relying on rate limiter
@Newify(Message)
@MicronautTest
@Property(name="pipe.http.server.read.response-size-limit-in-bytes", value="200")
@Property(name="micronaut.security.enabled", value="false")
@Property(name="compression.threshold-in-bytes", value = "1024")
@Property(name="rate-limiter.capacity", value = "1")
class PipeReadControllerClusterChangeRateLimiterIntegrationSpec extends Specification {

    @Inject @Named("local")
    Reader reader

    @Inject
    LocationService locationResolver

    @Inject
    EmbeddedServer server

    static String type = "type1"

    void setup() {
        RestAssured.port = server.port
        locationResolver.getClusterUuids(_) >> ["cluster1"]
    }

    void cleanup() {
        resetRateLimiter()
    }

    void "0ms retry after is issued when data is recent and within rate limiter"() {
        given: "storage with recent message timestamp and routing id different from cluster id"
        reader.read(*_) >> new MessageResults([
            Message(type, "a", "ct", 100, ZonedDateTime.now().minusHours(1), null, 0, 10, 12)
        ], 100L, of(5), PipeState.UP_TO_DATE)

        when: "concurrent calls request messages"
        def retryAfterHeaders = []
        3.times{ i ->
            new Thread( {
                retryAfterHeaders << RestAssured.given().get("/pipe/0?location='someLocation'").header(HttpHeaders.RETRY_AFTER_MS)
            }).run()
        }

        then: "one call is rate limited"
        PollingConditions conditions = new PollingConditions(timeout: 2)
        conditions.eventually {
            assert retryAfterHeaders.get(0) == "0"
            assert retryAfterHeaders.get(2) == "100"
        }
    }

    void "retry after greater than 0 if not part of cluster change"() {
        given: "a message with same cluster id and routing id"
        reader.read(*_) >> new MessageResults([
            Message(type, "a", "ct", 100, ZonedDateTime.now().minusHours(1), null, 0, 10, 10)
        ], 100L, of(5), PipeState.UP_TO_DATE)

        when: "request is made"
        def retryAfterHeaders = []
        retryAfterHeaders << RestAssured.given().get("/pipe/0?location='someLocation'").header(HttpHeaders.RETRY_AFTER_MS)

        then: "no exception is thrown"
        noExceptionThrown()

        and: "retry after is greater than 0"
        assert retryAfterHeaders.get(0) == "100"
    }

    void "no exception is thrown when cluster id and routing id are not initialised"() {
        given: "a message with no cluster id and routing id"
        reader.read(*_) >> new MessageResults([
            Message(type, "a", "ct", 100, ZonedDateTime.now().minusHours(1), null)
        ], 100L, of(5), PipeState.UP_TO_DATE)

        when: "request is made"
        def retryAfterHeaders = []
        retryAfterHeaders << RestAssured.given().get("/pipe/0?location='someLocation'").header(HttpHeaders.RETRY_AFTER_MS)

        then: "no exception is thrown"
        noExceptionThrown()

        and: "retry after is greater than 0"
        assert retryAfterHeaders.get(0) == "100"
    }

    @MockBean(Reader)
    @Named("local")
    Reader reader() {
        Mock(Reader)
    }

    @MockBean(LocationService)
    LocationService locationResolver() {
        Mock(LocationService)
    }

    void resetRateLimiter() {
        sleep(1000)
    }
}