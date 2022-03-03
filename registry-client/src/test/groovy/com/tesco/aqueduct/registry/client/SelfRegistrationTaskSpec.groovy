package com.tesco.aqueduct.registry.client

import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Node
import com.tesco.aqueduct.registry.model.RegistryResponse
import spock.lang.Specification
import spock.lang.Unroll

import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.tesco.aqueduct.registry.model.Status.INITIALISING

class SelfRegistrationTaskSpec extends Specification {
    private static final URL MY_HOST = new URL("http://localhost")

    private static final Node MY_NODE = Node.builder()
        .group("1234")
        .localUrl(MY_HOST)
        .offset(0)
        .status(INITIALISING)
        .following(Collections.emptyList())
        .lastSeen(ZonedDateTime.now())
        .build()

    def upstreamClient = Mock(RegistryClient)
    def services = Mock(ServiceList)
    def bootstrapService = Mock(BootstrapService)

    def 'check registry client polls upstream service'() {
        def startedLatch = new CountDownLatch(1)

        given: "a registry client"
        def registryClient = selfRegistrationTask()

        when: "register() is called"
        registryClient.register()
        def ran = startedLatch.await(2, TimeUnit.SECONDS)

        then: "the client will eventually have a list of endpoints returned from the Registry Service"
        notThrown(Exception)
        ran
        1 * upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], BootstrapType.NONE)
        1 * services.update(_ as List) >> { startedLatch.countDown() }
    }

    def 'check registryHitList defaults to cloud pipe if register call fails'() {
        given: "a registry client"
        def registryClient = selfRegistrationTask()

        and: "when called, upstreamClient will throw an exception"
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> { throw new RuntimeException() }

        when: "register() is called"
        registryClient.register()

        then: "services are NOT updated"
        0 * services.update(_)
    }

    def 'check register does not default to cloud pipe if previously it succeeded'() {
        given: "a registry client"
        def registryClient = selfRegistrationTask()
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> new RegistryResponse(["http://1.2.3.4", "http://5.6.7.8"], BootstrapType.NONE) >> { throw new RuntimeException() }

        when: "register() is called successfully"
        registryClient.register()

        and: "register is called again, and this time is unsuccessful"
        registryClient.register()

        then: "hitlist has only been called once, upstream client is registered to twice"
        1 * services.update(["http://1.2.3.4", "http://5.6.7.8"])
    }

    def 'null response to register call does not result in null hit list update update'() {
        given: "a registry client"
        def registryClient = selfRegistrationTask()

        and: "upstream client will return null"
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> { null }

        when: "register() is called"
        registryClient.register()

        then: "Services are NOT updated"
        0 * services.update(_)
    }

    def 'till bootstraps in pipe and provider if it is stale'() {
        given: 'the node last registered 30+ days ago'
        def staleNode = MY_NODE.toBuilder()
            .lastRegistrationTime(ZonedDateTime.now().minusDays(30))
            .build()

        and: "registry response without bootstrap request"
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> new RegistryResponse([], BootstrapType.NONE)

        and: 'a self registration task'
        def selfRegistrationTask = new SelfRegistrationTask(
            upstreamClient,
            { staleNode },
            services,
            bootstrapService
        )

        when:
        selfRegistrationTask.register()

        then:
        1 * services.update([])

        then:
        1 * bootstrapService.bootstrap(BootstrapType.PIPE_AND_PROVIDER)
    }

    def 'till does not bootstrap if it is not stale'() {
        given: 'the node last registered < 30 days ago'
        def staleNode = MY_NODE.toBuilder()
            .lastRegistrationTime(ZonedDateTime.now().minusDays(29))
            .build()

        and: "registry response without bootstrap request"
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> new RegistryResponse([], BootstrapType.NONE)

        and: 'a self registration task'
        def selfRegistrationTask = new SelfRegistrationTask(
            upstreamClient,
            { staleNode },
            services,
            bootstrapService
        )

        when:
        selfRegistrationTask.register()

        then:
        1 * services.update([])

        then:
        0 * bootstrapService.bootstrap(BootstrapType.PIPE_AND_PROVIDER)
        1 * bootstrapService.bootstrap(BootstrapType.NONE)
    }

    @Unroll
    def 'bootstrap related methods are called in correct combo and order depending on bootstrap type'() {
        given: "a registry client"
        def registryClient = selfRegistrationTask()

        and: "registry response with a bootstrap request"
        upstreamClient.registerAndConsumeBootstrapRequest(_ as Node) >> new RegistryResponse([], bootstrapType)

        when: "register() is called"
        registryClient.register()

        then: "bootstrap service is called with the correct type"
        1 * bootstrapService.bootstrap(bootstrapType)

        where:
        bootstrapType                              | _
        BootstrapType.PROVIDER                     | _
        BootstrapType.PIPE_AND_PROVIDER            | _
        BootstrapType.NONE                         | _
        BootstrapType.PIPE                         | _
        BootstrapType.PIPE_WITH_DELAY              | _
        BootstrapType.PIPE_AND_PROVIDER_WITH_DELAY | _
        BootstrapType.CORRUPTION_RECOVERY          | _
    }

    SelfRegistrationTask selfRegistrationTask() {
        return new SelfRegistrationTask(
            upstreamClient,
            { MY_NODE },
            services,
            bootstrapService
        )
    }
}
