package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.registry.client.PipeLoadBalancer;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.discovery.DiscoveryClient;
import io.micronaut.http.client.LoadBalancer;
import io.micronaut.http.client.loadbalance.DiscoveryClientLoadBalancerFactory;
import jakarta.inject.Singleton;

@Singleton
@Replaces(DiscoveryClientLoadBalancerFactory.class)
public class PipeLoadBalancerFactory extends DiscoveryClientLoadBalancerFactory {

    public static final String SERVICE_ID = "pipe";
    private final PipeLoadBalancer pipeLoadBalancer;

    public PipeLoadBalancerFactory(final PipeLoadBalancer pipeLoadBalancer, final DiscoveryClient discoveryClient) {
        super(discoveryClient);
        this.pipeLoadBalancer = pipeLoadBalancer;
    }

    @Override
    public LoadBalancer create(final String serviceID) {
        if(serviceID.equals(SERVICE_ID)) {
            return pipeLoadBalancer;
        }
        return super.create(serviceID);
    }
}
