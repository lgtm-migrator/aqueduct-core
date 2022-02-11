package com.tesco.aqueduct.registry.client

import com.stehno.ersatz.ErsatzServer
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.netty.DefaultHttpClient
import io.micronaut.http.uri.UriBuilder
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

@Newify(URL)
class ServiceListIntegrationSpec extends Specification {
    final static URL URL_1 = URL("http://a1")

    ServiceList serviceList

    @Rule
    public TemporaryFolder folder = new TemporaryFolder()

    def setup() {
        def config = new DefaultHttpClientConfiguration()
        File existingPropertiesFile = folder.newFile()
        serviceList = new ServiceList(new DefaultHttpClient(), new PipeServiceInstance(new DefaultHttpClient(), URL_1), existingPropertiesFile)
    }

    def "check state respects the base path of all the servers in the list when performing the status check"() {
        given: "a list of paths exist which contains urls with and without a base path"
        def paths = ["/messaging/v1", "", "/"]

        and: "servers that respond a 200 status code for each URL exist"
        def servers = paths.collect { serverWithPipeStatus(it, 200) }

        and: "list is updated with the list of urls from the mock servers"
        serviceList.update(paths.withIndex().collect { element, index -> URL(servers[index].httpUrl + element) })

        when: "checking the status of the urls in the load balancer"
        serviceList.updateState()

        then: "the mock servers are called, showing that the check status method respects the base path of the endpoints"
        servers.each { it.verify() }

        cleanup: "the mock servers are cleaned up"
        servers.each { it.close() }
    }

    def "check state checks the status of all servers on the list"() {
        given: "a list of server urls"
        List<ErsatzServer> servers = [serverWithPipeStatus(200), serverWithPipeStatus(200)]
        servers.each { it.start() }

        serviceList.update(servers.collect{ URL(it.httpUrl) })

        when: "check state is called"
        serviceList.updateState()

        then:"status endpoints are called on each server"
        servers.each { it.verify() }

        cleanup:
        servers.each { it.close() }
    }

    ErsatzServer serverWithPipeStatus(int status) {
        ErsatzServer server = new ErsatzServer()
        server.expectations {
            GET("/pipe/_status") {
                called(1)

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(status)
                }
            }
        }

        return server
    }

    ErsatzServer serverWithPipeStatus(String path, int status) {
        ErsatzServer server = new ErsatzServer()
        server.expectations {
            String urlAsString = UriBuilder.of(URI.create(path)).path("/pipe/_status").build().toString();
            GET(urlAsString) {
                called(1)

                responder {
                    contentType('application/json')
                    body("""[]""")
                    code(status)
                }
            }
        }

        return server
    }
}
