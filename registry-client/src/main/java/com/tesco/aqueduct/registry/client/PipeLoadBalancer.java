package com.tesco.aqueduct.registry.client;

import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.discovery.ServiceInstance;
import io.micronaut.http.client.LoadBalancer;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.net.URL;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class PipeLoadBalancer implements LoadBalancer {
    private final ServiceList services;

    PipeLoadBalancer(final ServiceList services) {
        this.services = services;
    }

    @Override
    public Publisher<ServiceInstance> select(@Nullable final Object discriminator) {
        return services.stream()
            .filter(PipeServiceInstance::isUp)
            .findFirst()
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

    public LocalDateTime getLastUpdatedTime() {
        return services.getLastUpdatedTime();
    }
}
