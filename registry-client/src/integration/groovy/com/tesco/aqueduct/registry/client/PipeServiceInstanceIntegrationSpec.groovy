package com.tesco.aqueduct.registry.client

import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.stubbing.Scenario
import io.micronaut.http.client.DefaultHttpClientConfiguration
import org.junit.Rule
import spock.lang.Specification
import spock.lang.Unroll

import static com.github.tomakehurst.wiremock.client.WireMock.*

class PipeServiceInstanceIntegrationSpec extends Specification {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089)

    def "sets flag isUp to true when a valid http response is returned"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(200)
                    .withBody("a response")
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
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(
            get(urlEqualTo("/pipe/_status"))
                .willReturn(aResponse()
                    .withStatus(status)
                    .withBody("a response")
                )
        )

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        !pipeServiceInstance.isUp()

        and:
        verify(exactly(3), getRequestedFor(urlEqualTo("/pipe/_status")))

        where:
        status | _
        500    | _
        400    | _
    }

    @Unroll
    def "checkState retries twice if request fails on first attempt"() {
        given:
        def pipeServiceInstance = new PipeServiceInstance(new DefaultHttpClientConfiguration(), new URL(wireMockRule.baseUrl()))

        and:
        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withStatus(responseFirstCall).withBody("a response"))
                .willSetStateTo("second_call"))

        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs("second_call")
                .willReturn(aResponse().withStatus(responseSecondCall).withBody("a response"))
                .willSetStateTo("third_call"))

        stubFor(get(urlEqualTo("/pipe/_status")).inScenario("healthcheck")
                .whenScenarioStateIs("third_call")
                .willReturn(aResponse().withStatus(responseThirdCall).withBody("a response")))

        when:
        pipeServiceInstance.updateState().blockingGet()

        then:
        pipeServiceInstance.isUp()

        and:
        verify(exactly(mockInvocationCount), getRequestedFor(urlEqualTo("/pipe/_status")))

        where:
        responseFirstCall | responseSecondCall | responseThirdCall | mockInvocationCount
        200               | 200                | 200               | 1
        500               | 200                | 200               | 2
        500               | 500                | 200               | 3
    }
}
