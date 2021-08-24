package com.tesco.aqueduct.pipe.http

import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static io.restassured.RestAssured.when
import static org.hamcrest.Matchers.equalTo

class PipeStatusControllerIntegrationSpec extends Specification {

    @Shared @AutoCleanup("stop") ApplicationContext context
    @Shared @AutoCleanup("stop") EmbeddedServer server

    void setupSpec() {
        context = ApplicationContext
            .build()
            .mainClass(PipeStatusController)
            .properties(
                parseYamlConfig(
                """
                    compression.threshold-in-bytes: 1024
                """
                )
            )
            .build()

        context.start()

        server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanupSpec() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def "pipe status contains version in it"() {
        expect:
        when()
            .get("/pipe/_status")
            .then()
            .statusCode(HttpStatus.OK.code)
            .body("version", equalTo(Version.getImplementationVersion()))
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
