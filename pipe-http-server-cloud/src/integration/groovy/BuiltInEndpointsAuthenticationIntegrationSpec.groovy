import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.tesco.aqueduct.pipe.api.CentralStorage
import com.tesco.aqueduct.pipe.api.Reader
import groovy.json.JsonOutput
import io.micronaut.context.ApplicationContext
import io.micronaut.context.env.yaml.YamlPropertySourceLoader
import io.micronaut.http.HttpStatus
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.restassured.RestAssured
import org.junit.Rule
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource

import static com.github.tomakehurst.wiremock.client.WireMock.*

class BuiltInEndpointsAuthenticationIntegrationSpec extends Specification {

    static final int CACHE_EXPIRY_SECONDS = 1
    static final String VALIDATE_TOKEN_BASE_PATH = '/some/access-token/validate/path'
    private final static String LOCATION_CLUSTER_PATH_FILTER_PATTERN = "/**/some/get/*/clusters/path/**"

    static final String clientId = UUID.randomUUID().toString()
    static final String secret = UUID.randomUUID().toString()
    static final String clientIdAndSecret = "${clientId}:${secret}"

    static final String userUIDA = UUID.randomUUID()
    static final String clientUserUIDA = "trn:tesco:uid:uuid:${userUIDA}"

    static final String validateTokenPath = "${VALIDATE_TOKEN_BASE_PATH}?client_id={clientIdAndSecret}"

    @Shared @AutoCleanup ApplicationContext context

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    def setup() {
        context = ApplicationContext
            .builder()
            .mainClass(EmbeddedServer)
            .properties(
                parseYamlConfig(
                    """
                micronaut.security.enabled: true
                micronaut.security.token.jwt.enabled: true
                micronaut.security.token.jwt.bearer.enabled: true
                micronaut.caches.identity-cache.expire-after-write: ${CACHE_EXPIRY_SECONDS}s
                endpoints.all.enabled: true
                endpoints.all.path: /endpoints
                endpoints.all.sensitive: true
                compression.threshold-in-bytes: 1024
                location.clusters.get.path.filter.pattern: $LOCATION_CLUSTER_PATH_FILTER_PATTERN
                authentication:
                  identity:
                    attempts: 3
                    delay: 500ms
                    url: ${wireMockRule.baseUrl()}
                    validate.token.path: $validateTokenPath
                    client:
                        id: $clientId
                        secret: $secret
                    users:
                      userA:
                        clientId: $clientUserUIDA
                        roles:
                          - PIPE_READ
                """
                )
            )
        .build()
        .registerSingleton(Reader, Mock(CentralStorage), Qualifiers.byName("local"))
        .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("pipe"))
        .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("registry"))
        .registerSingleton(DataSource, Mock(DataSource), Qualifiers.byName("compaction"))

        context.start()

        def server = context.getBean(EmbeddedServer)
        server.start()

        RestAssured.port = server.port
    }

    void cleanup() {
        RestAssured.port = RestAssured.DEFAULT_PORT
    }

    def 'Built in logger endpoint access is under authentication'() {
        given: 'A valid identity token'
        def identityToken = UUID.randomUUID().toString()
        acceptIdentityTokenValidationRequest(clientIdAndSecret, identityToken, clientUserUIDA)

        and: 'logger level can be fetched with auth and is info to begin with'
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/endpoints/loggers/com.tesco.aqueduct.pipe")
            .then()
            .statusCode(HttpStatus.OK.code)
            .extract()
            .path("effectiveLevel") == "INFO"

        when: "logger level is changed to DEBUG mode with authentication"
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .header("Content-Type", "application/json")
            .body('{ "configuredLevel": "DEBUG" }')
            .post("/endpoints/loggers/com.tesco.aqueduct.pipe")
            .then()
            .statusCode(HttpStatus.OK.code)

        then: "verify that it has been changed to DEBUG for the given package"
        RestAssured.given()
            .header("Authorization", "Bearer $identityToken")
            .get("/endpoints/loggers/com.tesco.aqueduct.pipe")
            .then()
            .statusCode(HttpStatus.OK.code)
            .extract()
            .path("effectiveLevel") == "DEBUG"
    }

    def 'built in endpoints cannot be accessed with invalid access token'() {
        given: "invalid token and Identity rejecting it"
        def someInvalidToken = "someInvalidToken"
        rejectIdentityTokenValidationRequest(clientIdAndSecret, someInvalidToken)

        expect: 'logger endpoint cannot be accessed with invalid token'
        RestAssured.given()
            .header("Authorization", "Bearer $someInvalidToken" )
            .get("/endpoints/loggers/com.tesco.aqueduct.pipe")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def 'built in endpoints cannot be accessed without access token'() {
        expect: 'logger endpoint cannot be accessed without any token'
        RestAssured.given()
            .get("/endpoints/loggers/com.tesco.aqueduct.pipe")
            .then()
            .statusCode(HttpStatus.UNAUTHORIZED.code)
    }

    def acceptIdentityTokenValidationRequest(String clientIdAndSecret, String identityToken, String clientUserUID) {
        def json = JsonOutput.toJson([access_token: identityToken])

        stubFor(
            post(urlPathEqualTo(VALIDATE_TOKEN_BASE_PATH))
                .withQueryParam("client_id", equalTo(clientIdAndSecret))
                .withRequestBody(equalToJson(json))
                .willReturn(aResponse()
                    .withHeader("Content-Type", "application/json;charset=UTF-8")
                    .withStatus(200)
                    .withBody("""
                        {
                          "UserId": "${clientUserUID}",
                          "Status": "VALID",
                          "Claims": [
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/clientid",
                              "value": "trn:tesco:cid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/scope",
                              "value": "oob"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/userkey",
                              "value": "trn:tesco:uid:uuid:${UUID.randomUUID()}"
                            },
                            {
                              "claimType": "http://schemas.tesco.com/ws/2011/12/identity/claims/confidencelevel",
                              "value": "12"
                            },
                            {
                              "claimType": "http://schemas.microsoft.com/ws/2008/06/identity/claims/expiration",
                              "value": "1548413702"
                            }
                          ]
                        }
                    """
                    )
            )
        )
    }


    def rejectIdentityTokenValidationRequest(String clientIdAndSecret, String identityToken) {
        def json = JsonOutput.toJson([access_token: identityToken])

        stubFor(
            post(urlPathEqualTo(VALIDATE_TOKEN_BASE_PATH))
                .withQueryParam("client_id", equalTo(clientIdAndSecret))
                .withRequestBody(equalToJson(json))
                .willReturn(aResponse()
                    .withHeader("Content-Type", "application/json;charset=UTF-8")
                    .withStatus(200)
                    .withBody("""
                        {
                          "Status": "INVALID"
                        }
                    """)
                )
        )
    }

    Map<String, Object> parseYamlConfig(String str) {
        def loader = new YamlPropertySourceLoader()
        loader.read("config", str.bytes)
    }
}
