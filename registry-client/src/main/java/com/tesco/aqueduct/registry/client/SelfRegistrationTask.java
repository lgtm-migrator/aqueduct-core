package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.model.BootstrapType;
import com.tesco.aqueduct.registry.model.Node;
import com.tesco.aqueduct.registry.model.RegistryResponse;
import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;

@Context
@Requires(property = "registry.http.interval")
public class SelfRegistrationTask {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(SelfRegistrationTask.class));

    private final RegistryClient client;
    private final SummarySupplier selfSummary;
    private final ServiceList services;
    private final BootstrapService bootstrapService;

    @Inject
    public SelfRegistrationTask(
        final RegistryClient client,
        final SummarySupplier selfSummary,
        final ServiceList services,
        final BootstrapService bootstrapService
    ) {
        this.client = client;
        this.selfSummary = selfSummary;
        this.services = services;
        this.bootstrapService = bootstrapService;
    }

    @Scheduled(fixedRate = "${registry.http.interval}")
    void register() {
        try {
            final Node node = selfSummary.getSelfNode();
            final RegistryResponse registryResponse = client.registerAndConsumeBootstrapRequest(node);

            if (registryResponse.getRequestedToFollow() == null) {
                LOG.error("SelfRegistrationTask.register", "Register error", "Null response received");
                return;
            }

            services.update(registryResponse.getRequestedToFollow());

            if (isStale(node)) {
                LOG.info("SelfRegistrationTask.register", "Bootstrapping stale till");
                bootstrapService.bootstrap(BootstrapType.PIPE_AND_PROVIDER);
            } else {
                bootstrapService.bootstrap(registryResponse.getBootstrapType());
            }
        } catch (HttpClientResponseException hcre) {
            LOG.error("SelfRegistrationTask.register", "Register error [HttpClientResponseException]: %s", hcre.getMessage());
        } catch (Exception e) {
            LOG.error("SelfRegistrationTask.register", "Register error", e);
        }
    }

    private boolean isStale(Node node) {
        return node.getLastRegistrationTime() != null &&
            node.getLastRegistrationTime().isBefore(ZonedDateTime.now().minusDays(30));
    }
}
