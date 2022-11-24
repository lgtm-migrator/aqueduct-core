package com.tesco.aqueduct.registry.client

import io.micronaut.http.client.netty.DefaultHttpClient
import spock.lang.Specification
import spock.lang.Unroll

@Newify(URL)
class PipeServiceInstanceSpec extends Specification {

    @Unroll
    def "For path base url #baseUrl resolved path should be #expectedPath"() {
        given: "A url with a base path"
        def url = URL(baseUrl)
        def uri = new URI("/pipe/0")
        def serviceInstance = new PipeServiceInstance(new DefaultHttpClient(), url)

        when: "resolving a relative uri"
        def response = serviceInstance.resolve(uri)

        then: "the path is returned with the base path included"
        response.path == expectedPath

        where:
        baseUrl               | expectedPath
        "http://foo.bar/bar"  | "/bar/pipe/0"
        "http://foo.bar/bar/" | "/bar/pipe/0"
        "http://foo.bar/"     | "/pipe/0"
        "http://foo.bar"      | "/pipe/0"
    }

    def "RxClient errors are not rethrown"() {
        given: "client throwing errors"
        def serviceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL("http://not.a.url"))

        when: "we check the state"
        serviceInstance.updateState().blockingAwait()

        then:
        noExceptionThrown()

        and:
        !serviceInstance.isUp()
    }

    def "error handled when uri is not valid"() {
        given: "pipe service instance with invalid uri"
        def serviceInstance = new PipeServiceInstance(new DefaultHttpClient(), new URL("http://"))

        when: "we check the state"
        serviceInstance.updateState().blockingAwait()

        then:
        noExceptionThrown()

        and:
        !serviceInstance.isUp()
    }
}
