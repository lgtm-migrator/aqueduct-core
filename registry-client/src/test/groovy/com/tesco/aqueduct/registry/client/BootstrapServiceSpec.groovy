package com.tesco.aqueduct.registry.client


import com.tesco.aqueduct.registry.model.BootstrapType
import com.tesco.aqueduct.registry.model.Bootstrapable
import com.tesco.aqueduct.registry.model.Resetable
import spock.lang.Specification
import spock.lang.Unroll

class BootstrapServiceSpec extends Specification {
    def provider = Mock(Bootstrapable)
    def pipe = Mock(Bootstrapable)
    def controller = Mock(Bootstrapable)
    def corruptionManager = Mock(Resetable)
    BootstrapService bootstrapService

    void setup() {
        bootstrapService = new BootstrapService(provider, pipe, controller, corruptionManager, "0.001S", 1)
    }

    @Unroll
    def 'bootstrap related methods are called in correct combo and order depending on bootstrap type'() {
        when:
        bootstrapService.bootstrap(bootstrapType)

        then: "provider is stopped"
        providerStopAndResetCalls * provider.stop()

        then: "provider is reset"
        providerStopAndResetCalls * provider.reset()

        then: "pipe is stopped"
        pipeStopCalls * pipe.stop()

        then: "controller is stopped"
        controllerStopAndStartCalls * controller.stop()

        then: "corruption manager is reset"
        corruptionManagerCalls * corruptionManager.reset()

        then: "pipe is reset"
        pipeResetAndStartCalls * pipe.reset()

        then: "pipe is started"
        pipeResetAndStartCalls * pipe.start()

        then: "controller is started"
        controllerStopAndStartCalls * controller.start()

        then: "provider is started"
        providerStartCalls * provider.start()

        where:
        bootstrapType                              | providerStopAndResetCalls | providerStartCalls | pipeResetAndStartCalls | pipeStopCalls | controllerStopAndStartCalls | corruptionManagerCalls
        BootstrapType.PROVIDER                     | 1                         | 1                  | 0                      | 0             | 0                           | 0
        BootstrapType.PIPE_AND_PROVIDER            | 1                         | 1                  | 1                      | 1             | 1                           | 0
        BootstrapType.NONE                         | 0                         | 0                  | 0                      | 0             | 0                           | 0
        BootstrapType.PIPE                         | 0                         | 0                  | 1                      | 1             | 1                           | 0
        BootstrapType.PIPE_WITH_DELAY              | 0                         | 0                  | 1                      | 1             | 1                           | 0
        BootstrapType.PIPE_AND_PROVIDER_WITH_DELAY | 1                         | 1                  | 1                      | 1             | 1                           | 0
        BootstrapType.CORRUPTION_RECOVERY          | 1                         | 0                  | 0                      | 1             | 0                           | 1
    }
}
