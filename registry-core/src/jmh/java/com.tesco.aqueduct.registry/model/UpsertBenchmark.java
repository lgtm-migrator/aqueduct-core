/*package com.tesco.aqueduct.registry.model;

import com.beust.jcommander.internal.Lists;
import org.apache.groovy.util.Maps;
import org.openjdk.jmh.annotations.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.ZonedDateTime;

@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 5, time = 1)
public class UpsertBenchmark {

    @State(Scope.Thread)
    public static class MyState {
        Node node1;
        Node node2;
        Node node3;
        URL cloud;
        NodeGroup nodeGroup;

        public MyState() {
        }

        @Setup(Level.Iteration)
        public void doSetup() {

            try {
                node1 = new Node(
                    "group1",
                    new URL("http://abc1"),
                    0,
                    Status.FOLLOWING,
                    Lists.newArrayList(new URL("http://some.node.url")),
                    Lists.newArrayList(new URL("http://some.node.url")),
                    ZonedDateTime.now(),
                    Maps.of("v","1.0"),
                    Maps.of("GLOBAL_OFFSET","1"),
                    Maps.of("LAST_ACK","1")
                );
                node2 = new Node(
                    "group1",
                    new URL("http://abc2"),
                    0,
                    Status.FOLLOWING,
                    Lists.newArrayList(new URL("http://some.node.url")),
                    Lists.newArrayList(new URL("http://some.node.url")),
                    ZonedDateTime.now(),
                    Maps.of("v","1.0"),
                    Maps.of("GLOBAL_OFFSET","1"),
                    Maps.of("LAST_ACK","1")
                );
                node3 = new Node(
                    "group1",
                    new URL("http://abc3"),
                    0,
                    Status.FOLLOWING,
                    Lists.newArrayList(new URL("http://some.node.url")),
                    Lists.newArrayList(new URL("http://some.node.url")),
                    ZonedDateTime.now(),
                    Maps.of("v","1.0"),
                    Maps.of("GLOBAL_OFFSET","1"),
                    Maps.of("LAST_ACK","1")
                );
                cloud = new URL("http://some.cloud.url");
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }

            nodeGroup = new NodeGroup();
        }

        @TearDown(Level.Iteration)
        public void doTearDown() {
            nodeGroup = new NodeGroup();
        }
    }

    @Benchmark
    public void upsertMethod(MyState state) throws InterruptedException {
        state.nodeGroup.upsert(state.node1, state.cloud);
        state.nodeGroup.upsert(state.node2, state.cloud);
        state.nodeGroup.upsert(state.node3, state.cloud);
    }

}*/