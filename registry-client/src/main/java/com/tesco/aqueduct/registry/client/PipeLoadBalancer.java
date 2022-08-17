package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.discovery.ServiceInstance;
import io.micronaut.http.client.LoadBalancer;
import io.reactivex.Flowable;
import jakarta.inject.Singleton;
import org.slf4j.LoggerFactory;
import org.reactivestreams.Publisher;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class PipeLoadBalancer implements LoadBalancer {
    private final ServiceList services;
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PipeLoadBalancer.class));

    PipeLoadBalancer(final ServiceList services) {
        this.services = services;
    }

    @Override
    public Publisher<ServiceInstance> select(@Nullable final Object discriminator) {
        return services.stream()
            .filter(PipeServiceInstance::isUp)
            .findFirst()
            .map(service-> {
                LOG.debug("PipeLoadBalancer", "Connected Service URL:" + service.getUrl());
                return service;
            })
            .map(ServiceInstance.class::cast)
            .map(Publishers::just)
            .orElse(Flowable.error(new RuntimeException("No accessible service to call.")));
    }

    public List<URL> getFollowing() {
        return services.stream()
            .filter(PipeServiceInstance::isUp)
            .map(PipeServiceInstance::getUrl)
            .collect(Collectors.toList());
    }

    public ZonedDateTime getLastUpdatedTime() {
        return services.getLastUpdatedTime();
    }
}
