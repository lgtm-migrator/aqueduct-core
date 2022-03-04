package com.tesco.aqueduct.registry.client

import com.stehno.ersatz.ErsatzServer
import com.tesco.aqueduct.pipe.api.IdentityToken
import com.tesco.aqueduct.pipe.api.TokenProvider
import com.tesco.aqueduct.registry.model.Bootstrapable
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.Resetable
import io.micronaut.context.ApplicationContext
import io.micronaut.http.client.netty.DefaultHttpClient
import io.micronaut.inject.qualifiers.Qualifiers
import io.reactivex.Single
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZonedDateTime

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class RegistryToServiceListIntegrationSpec extends Specification {
    @Shared
    @AutoCleanup
    ErsatzServer cloudServer = new ErsatzServer()

    @Shared
    @AutoCleanup
    ErsatzServer serviceServer = new ErsatzServer()

    TokenProvider tokenProvider = Mock(TokenProvider) {
        retrieveIdentityToken() >> Single.just(Mock(IdentityToken))
    }

    SummarySupplier summarySupplier = Mock(SummarySupplier) {
        getSelfNode() >> Node.builder()
            .group("1234")
            .localUrl(new URL("http://localhost"))
            .offset(0)
            .status(INITIALISING)
            .lastSeen(ZonedDateTime.now())
            .build()
    }

    ApplicationContext context
    SelfRegistrationTask selfRegistrationTask
    PipeLoadBalancerHealthCheckTask pipeLoadBalancerHealthCheckTask

    def setupSpec() {
        cloudServer.start()
        serviceServer.start()
    }

    void setup() {
        cloudServer.expectations {
            GET("/pipe/_status") {
                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(200)
                }
            }
        }

        context = ApplicationContext
            .builder()
            .properties(
                "registry.http.client.url": cloudServer.getHttpUrl() + "/v2",
                "registry.http.client.delay": "500ms",
                "registry.http.client.attempts": "1",
                "registry.http.client.reset": "1s",
                "registry.http.interval": "15m",
                "pipe.http.client.healthcheck.interval": "15m",
                "persistence.compact.deletions.threshold": "30d"
            )
            .build()
            .registerSingleton(tokenProvider)
            .registerSingleton(summarySupplier)
            .registerSingleton(new ServiceList(
                new DefaultHttpClient(),
                new PipeServiceInstance(new DefaultHttpClient(), new URL(cloudServer.getHttpUrl())),
                File.createTempFile("provider", "properties")
            ))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("provider"))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("pipe"))
            .registerSingleton(Bootstrapable.class, Mock(Bootstrapable), Qualifiers.byName("controller"))
            .registerSingleton(Resetable.class, Mock(Resetable), Qualifiers.byName("corruptionManager"))
            .start()

        selfRegistrationTask = context.getBean(SelfRegistrationTask)
        pipeLoadBalancerHealthCheckTask = context.getBean(PipeLoadBalancerHealthCheckTask)
    }

    def "given a new service endpoint by registry, the till starts to follow that"() {
        given: "registry server which returns the new service URL"
        def serviceURL = serviceServer.getHttpUrl()
        cloudServer.expectations {
            POST("/v2/registry") {
                header("Accept-Encoding", "gzip, deflate")

                responder {
                    contentType("application/json")
                    body("""{"requestedToFollow" : [ "$serviceURL" ], "bootstrapType" : "NONE"}""")
                }
            }
        }
        and: "service server is healthy"
        serviceServer.expectations {
            GET("/pipe/_status") {
                called(1)

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(200)
                }
            }
        }

        when: "SelfRegistrationTask calls registry server"
        selfRegistrationTask.register()
        and: "service health check is called"
        pipeLoadBalancerHealthCheckTask.checkState()

        then: "the new service URL is called"
        serviceServer.verify()
    }
}
