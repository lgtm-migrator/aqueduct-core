package com.tesco.aqueduct.registry.client

import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.micronaut.http.client.netty.DefaultHttpClient
import org.junit.Rule
import spock.lang.Specification
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*

class PipeServiceInstanceIntegrationSpec extends Specification {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    def "sets flag isUp to true when a valid http response is returned"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withBody('{"status": "ok","version": "0.1.377"}')
                )
        )

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        pipeServiceInstance.isUp()
    }

    @Unroll
    def "sets flag isUp to false when the service is down with status as #status"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(status)
                        .withBody(body)
                )
        )

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        !pipeServiceInstance.isUp()

        and:
        verify(exactly(3), getRequestedFor(urlEqualTo("/pipe/_status")))

        where:
        status | body
        500    | '{}'
        500    | ''
        500    | 'error body'
        400    | '{}'
        400    | ''
        400    | 'error body'
    }

    @Unroll
    def "sets flag isUp to false when the service is down with 200 status"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(status)
                        .withBody(body)
                )
        )

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        !pipeServiceInstance.isUp()

        and:
        verify(exactly(3), getRequestedFor(urlEqualTo("/pipe/_status")))

        where:
        status | body
        200    | '{}'
        200    | '{"status": "ok","version": "0.0.0"}'
        200    | '{"status": "ok"}'
    }

    @Unroll
    def "checkState retries twice if request fails on first attempt"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(responseFirstCall).withBody(bodyFirst))
                .willSetStateTo("second_call"))

        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs("second_call")
                .willReturn(aResponse().withStatus(responseSecondCall).withBody(bodySecond))
                .willSetStateTo("third_call"))

        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs("third_call")
                .willReturn(aResponse().withStatus(responseThirdCall).withBody(bodyThird)))

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        pipeServiceInstance.isUp()

        and:
        verify(exactly(mockInvocationCount), getRequestedFor(urlEqualTo("/pipe/_status")))

        where:
        responseFirstCall | responseSecondCall | responseThirdCall | mockInvocationCount | bodyFirst                               | bodySecond                              | bodyThird
        200               | 200                | 200               | 1                   | '{"status": "ok","version": "0.1.377"}' | '{"status": "ok","version": "0.1.377"}' | '{"status": "ok","version": "0.1.377"}'
        500               | 200                | 200               | 2                   | '{}'                                    | '{"status": "ok","version": "0.1.377"}' | '{"status": "ok","version": "0.1.377"}'
        500               | 500                | 200               | 3                   | '{}'                                    | '{}'                                    | '{"status": "ok","version": "0.1.377"}'
    }
}
