package com.tesco.aqueduct.registry.client


import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.netty.DefaultHttpClient
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import static io.reactivex.Single.fromPublisher

@Newify(URL)
class PipeLoadBalancerSpec extends Specification {

    final static URL URL_1 = URL("http://a1")
    final static URL URL_2 = URL("http://a2")
    final static URL URL_3 = URL("http://a3")

    ServiceList serviceList
    PipeLoadBalancer loadBalancer

    @Rule
    public TemporaryFolder folder = new TemporaryFolder()

    def setup() {
        def config = new DefaultHttpClientConfiguration()
        serviceList = new ServiceList(new DefaultHttpClient(), new PipeServiceInstance(new DefaultHttpClient(), URL_1), folder.newFile()) {
            @Override
            public void updateState() {}
        }
        loadBalancer = new PipeLoadBalancer(serviceList)
    }

    def "Return cloud_url service until the registry has been updated"() {
        when: "that there is no service available"
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then:
        serviceInstance.URI == URL_1.toURI()

        when: "the service list is updated with a new url"
        serviceList.update([URL_2])

        and: "I select the service instance"
        serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then:
        serviceInstance.URI == URL_2.toURI()
    }

    def "After updating the registry the first url in the hitlist is selected"() {
        given: "that the registry is updated"
        def urls = [URL_1, URL_2]
        serviceList.update(urls)

        when: "I select the service instance"
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then: "the service points to the first url in the registry"
        serviceInstance.URI == URL_1.toURI()
    }

    def "Returns a list of URLs"() {
        def urls = [URL_1, URL_3, URL_3]

        given: "An updated list of urls"
        serviceList.update(urls)

        when: "we call get following"
        def actualUrls = loadBalancer.getFollowing()

        then: "we are returned with a list of following URLS"
        urls == actualUrls
    }

    def "Recording an error marks the first URL as down"() {
        def urls = [URL_1, URL_2, URL_3]
        def expectedUrls = [URL_2, URL_3]

        given: "An updated list of urls"
        serviceList.update(urls)

        and: "We record an error"
        serviceList.stream().findFirst().ifPresent({ c -> c.isUp(false) })

        when: "we call get"
        def actualUrls = loadBalancer.getFollowing()

        then: "we are returned with a list of following URLS that are up"
        expectedUrls == actualUrls
    }

    def "Updating the list of urls does not change UP status"() {
        given: "2 service urls and one marked as down"
        serviceList.update([URL_1, URL_2])
        serviceList.stream().findFirst().ifPresent({ c -> c.isUp(false) })

        when: "we update list of urls again"
        serviceList.update([URL_1, URL_2, URL_3])

        then: "the statuses of existing services have not been change"
        loadBalancer.getFollowing() == [URL_2, URL_3] // first service is down hence 2 and 3 is returned
    }

    def "In case of error, pick the next one"() {
        given: "a list of urls"
        serviceList.update([URL_1, URL_2, URL_3])

        when: "we got an error from the client"
        serviceList.stream().findFirst().ifPresent({ c -> c.isUp(false) })

        and: "we select another service"
        def serviceInstance = fromPublisher(loadBalancer.select()).blockingGet()

        then: "we getting next instance"
        serviceInstance.URI.toString() == "http://a2"
    }

    def "returns last updated time"() {
        when: "a list of urls"
        serviceList.update([URL_1, URL_2, URL_3])

        then: "get last updated time"
        loadBalancer.getLastUpdatedTime() != null
    }
}
