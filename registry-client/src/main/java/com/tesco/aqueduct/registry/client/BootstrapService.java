package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.model.Bootstrapable;
import com.tesco.aqueduct.registry.model.Resetable;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;

import java.time.Duration;

@Singleton
@Requires(property = "registry.http.interval")
public class BootstrapService {

    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(BootstrapService.class));

    private final Bootstrapable provider;
    private final Bootstrapable pipe;
    private final Bootstrapable controller;
    private final Resetable corruptionManager;
    private final long bootstrapDelayMs;

    public BootstrapService(@Named("provider") final Bootstrapable provider,
                            @Named("pipe") final Bootstrapable pipe,
                            @Named("controller") final Bootstrapable controller,
                            @Named("corruptionManager") Resetable corruptionManager,
                            @Property(name = "registry.http.interval") final Duration retryInterval,
                            @Value("${pipe.bootstrap.delay:300000}") final int additionalDelay // 5 minutes extra to allow all nodes to reset
    ) {
        this.provider = provider;
        this.pipe = pipe;
        this.controller = controller;
        this.corruptionManager = corruptionManager;
        this.bootstrapDelayMs = retryInterval.toMillis() + additionalDelay;
    }

    public void bootstrap(BootstrapType bootstrapType) throws Exception {
        LOG.info("bootstrap", "Bootstrapping in " + bootstrapType + " mode");

        switch (bootstrapType) {
            case PROVIDER:
                provider.stop();
                provider.reset();
                provider.start();
                break;
            case PIPE_AND_PROVIDER:
                provider.stop();
                provider.reset();
                pipe.stop();
                controller.stop();
                pipe.reset();
                pipe.start();
                controller.start();
                provider.start();
                break;
            case PIPE:
                pipe.stop();
                controller.stop();
                pipe.reset();
                pipe.start();
                controller.start();
                break;
            case PIPE_WITH_DELAY:
                pipe.stop();
                controller.stop();
                pipe.reset();
                Thread.sleep(bootstrapDelayMs);
                pipe.start();
                controller.start();
                break;
            case PIPE_AND_PROVIDER_WITH_DELAY:
                provider.stop();
                provider.reset();
                pipe.stop();
                controller.stop();
                pipe.reset();
                Thread.sleep(bootstrapDelayMs);
                pipe.start();
                controller.start();
                provider.start();
                break;
            case CORRUPTION_RECOVERY:
                provider.stop();
                provider.reset();
                pipe.stop();
                corruptionManager.reset();
                break;
        }
    }
}
