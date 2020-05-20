package com.tesco.aqueduct.registry.model

import com.tesco.aqueduct.pipe.api.PipeState
import spock.lang.Specification

import java.time.ZonedDateTime

import static com.tesco.aqueduct.registry.model.Status.FOLLOWING
import static com.tesco.aqueduct.registry.model.Status.INITIALISING
import static com.tesco.aqueduct.registry.model.Status.OFFLINE
import static com.tesco.aqueduct.registry.model.Status.PENDING

class NodeGroupSpec extends Specification {
    private static final URL CLOUD_URL = new URL("http://some-cloud-url")

    def "one subGroup is created for nodes belonging to same group"() {
        given: "two nodes"
        def node1 = createNode("group", new URL("http://1.1.1.1"))
        def node2 = createNode("group", new URL("http://2.2.2.2"))

        when: "a group with these nodes is created"
        def group = new NodeGroup([node1, node2])

        then: "nodegroup contains a subgroup with two nodes"
        group.subGroups.size() == 1
        group.subGroups.get(0).nodes == [node1, node2]
    }

    def "two subGroups are created for nodes belonging to different versions"() {
        given: "two nodes"
        def node1 = createNode("group", new URL("http://1.1.1.1"), 0, INITIALISING, [], null, ["v":"1.0"])
        def node2 = createNode("group", new URL("http://1.1.1.1"), 0, INITIALISING, [], null, ["v":"1.1"])

        when: "a group with these nodes is created"
        def group = new NodeGroup([node1, node2])

        then: "nodegroup contains a subgroup with two nodes"
        group.subGroups.size() == 2
        group.subGroups.get(0).nodes == [node1]
        group.subGroups.get(1).nodes == [node2]
    }

    def createNode(
            String group,
            URL url,
            long offset=0,
            Status status=INITIALISING,
            List<URL> following=[],
            ZonedDateTime created=null,
            Map<String, String> pipeProperties=["v":"1.0"],
            List<URL> requestedToFollow=[]
    ) {
        return Node.builder()
                .localUrl(url)
                .group(group)
                .status(status)
                .offset(offset)
                .following(following)
                .lastSeen(created)
                .requestedToFollow(requestedToFollow)
                .pipe(pipeProperties)
                .build()
    }

    def "Group has node"() {
        given: "A Group with Nodes"
        def group = new NodeGroup([Mock(Node)])
        when: "checking the group has nodes"
        def result = group.isEmpty()
        then: "the result is false"
        !result
    }

    def "Group does not have nodes"() {
        given: "A Group with no Nodes"
        def group = new NodeGroup([])

        expect: "subgroups are empty"
        group.subGroups.isEmpty()
    }

    def "A node can be removed from a node group given an host"() {
        given: "A node with a host"
        def node = Node.builder()
                .localUrl(new URL("http://node-url"))
                .requestedToFollow([CLOUD_URL])
                .status(OFFLINE)
                .pipe(["v":"1.0"])
                .build()

        and: "A node with a different host"
        def anotherNode = Node.builder()
                .localUrl(new URL("http://another-node-url"))
                .requestedToFollow([CLOUD_URL])
                .status(OFFLINE)
                .pipe(["v":"1.0"])
                .build()

        and: "a Group with these nodes"
        def group = new NodeGroup([node, anotherNode])

        when: "Removing the node from the group using the host"
        def result = group.removeByHost(node.getHost())

        then: "The group no longer contains the removed node"
        result
        group.subGroups.get(0).nodes == [anotherNode]
    }

    def "a node can be added to the group"() {
        given: "an empty node group"
        def group = new NodeGroup([])
        when: "a new node is added"
        def node = Node.builder().pipe(["v":"1.0"]).build()
        group.upsert(node, CLOUD_URL)
        then: "the node group is no longer empty"
        !group.isEmpty()
    }

    def "A node can be updated in the Group"() {
        given: "A node with a local url"
        def node1Url = new URL("http://test_node_1")
        def node1 = Node.builder()
            .localUrl(node1Url)
            .requestedToFollow([CLOUD_URL])
            .status(OFFLINE)
            .pipe(["v":"1.0"])
            .build()

        and: "A node with a different id"
        def node2Url = new URL("http://test_node_2")
        def node2 = Node.builder()
                .localUrl(node2Url)
                .requestedToFollow([CLOUD_URL])
                .status(OFFLINE)
                .pipe(["v":"1.0"])
                .build()

        and: "a Group with these nodes"
        def group = new NodeGroup([node1, node2])

        when: "An updated node is provided to the group"
        Node updatedNode1 = Node.builder()
                .localUrl(node1Url)
                .requestedToFollow([CLOUD_URL])
                .status(FOLLOWING)
                .pipe(["v":"1.0"])
                .build()
        group.upsert(updatedNode1, CLOUD_URL)

        then: "The group contains the updated node"
        group.subGroups.get(0).nodes.get(0).getId() == updatedNode1.getId()
        group.subGroups.get(0).nodes.get(1).getId() == node2.getId()

        when: "Another updated node is provided to the group"
        def updatedNode2 = Node.builder()
                .localUrl(node2Url)
                .requestedToFollow([CLOUD_URL])
                .status(FOLLOWING)
                .pipe(["v":"1.0"])
                .build()
        group.upsert(updatedNode2, CLOUD_URL)

        then: "The group contains the updated nodes"
        group.subGroups.get(0).nodes.get(0).getId() == updatedNode1.getId()
        group.subGroups.get(0).nodes.get(1).getId() == updatedNode2.getId()
    }

    def "Nodes are correctly rebalanced"() {
        given: "a cloud url"
        URL cloudUrl = new URL("http://cloud")
        and: "a nodegroup with unbalanced Nodes"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .pipe(["v":"1.0"])
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .pipe(["v":"1.0"])
            .build()
        URL n3Url = new URL("http://node-3")
        Node n3 = Node.builder()
            .localUrl(n3Url)
            .pipe(["v":"2.0"])
            .build()
        NodeGroup group = new NodeGroup([n1, n2, n3])
        when: "the group is rebalanced"
        group.updateGetFollowing(cloudUrl)
        then: "the result is a balanced group"
        group.subGroups.get(0).nodes.get(0).requestedToFollow == [cloudUrl]
        group.subGroups.get(0).nodes.get(1).requestedToFollow == [n1Url, cloudUrl]
        group.subGroups.get(1).nodes.get(0).requestedToFollow == [cloudUrl]
    }

    def "Nodes are sorted based on status"() {
        given: "a cloud url"
        URL cloudUrl = new URL("http://cloud")

        and: "a nodegroup with balanced, but partially offline nodes"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .requestedToFollow([cloudUrl])
            .status(OFFLINE)
            .pipe(["v":"1.0"])
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .requestedToFollow([n1Url, cloudUrl])
            .status(OFFLINE)
            .pipe(["v":"1.0"])
            .build()
        URL n3Url = new URL("http://node-3")
        Node n3 = Node.builder()
            .localUrl(n3Url)
            .requestedToFollow([n1Url, cloudUrl])
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        URL n4Url = new URL("http://node-4")
        Node n4 = Node.builder()
            .localUrl(n4Url)
            .requestedToFollow([n2Url, n1Url, cloudUrl])
            .status(PENDING)
            .pipe(["v":"1.0"])
            .build()
        URL n5Url = new URL("http://node-5")
        Node n5 = Node.builder()
            .localUrl(n5Url)
            .requestedToFollow([n2Url, n1Url, cloudUrl])
            .status(INITIALISING)
            .pipe(["v":"1.0"])
            .build()
        URL n6Url = new URL("http://node-6")
        Node n6 = Node.builder()
            .localUrl(n6Url)
            .requestedToFollow([n3Url, n1Url, cloudUrl])
            .status(OFFLINE)
            .pipe(["v":"1.0"])
            .build()

        NodeGroup group = new NodeGroup([n1, n2, n3, n4, n5, n6])

        when: "sort based on status is called"
        group.sortOfflineNodes(cloudUrl)

        then: "nodes that are offline are sorted to be leaves"
        group.subGroups.get(0).nodes.stream().map({ n -> n.getLocalUrl() }).collect() == [n3Url, n4Url, n5Url, n1Url, n2Url, n6Url]

        group.subGroups.get(0).nodes.get(0).requestedToFollow == [cloudUrl]
        group.subGroups.get(0).nodes.get(1).requestedToFollow == [n3Url, cloudUrl]
        group.subGroups.get(0).nodes.get(2).requestedToFollow == [n3Url, cloudUrl]
        group.subGroups.get(0).nodes.get(3).requestedToFollow == [n4Url, n3Url, cloudUrl]
        group.subGroups.get(0).nodes.get(4).requestedToFollow == [n4Url, n3Url, cloudUrl]
        group.subGroups.get(0).nodes.get(5).requestedToFollow == [n5Url, n3Url, cloudUrl]
    }

    def "Nodes maintain sort order when none are offline"() {
        given: "a cloud url"
        URL cloudUrl = new URL("http://cloud")

        and: "a nodegroup with balanced but no offline nodes"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .requestedToFollow([cloudUrl])
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .requestedToFollow([n1Url, cloudUrl])
            .status(PENDING)
            .pipe(["v":"1.0"])
            .build()
        URL n3Url = new URL("http://node-3")
        Node n3 = Node.builder()
            .localUrl(n3Url)
            .requestedToFollow([n1Url, cloudUrl])
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        URL n4Url = new URL("http://node-4")
        Node n4 = Node.builder()
            .localUrl(n4Url)
            .requestedToFollow([n2Url, n1Url, cloudUrl])
            .status(PENDING)
            .pipe(["v":"1.0"])
            .build()
        URL n5Url = new URL("http://node-5")
        Node n5 = Node.builder()
            .localUrl(n5Url)
            .requestedToFollow([n2Url, n1Url, cloudUrl])
            .status(INITIALISING)
            .pipe(["v":"1.0"])
            .build()
        URL n6Url = new URL("http://node-6")
        Node n6 = Node.builder()
            .localUrl(n6Url)
            .requestedToFollow([n3Url, n1Url, cloudUrl])
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()

        NodeGroup group = new NodeGroup([n1, n2, n3, n4, n5, n6])

        when: "sort based on status is called"
        group.sortOfflineNodes(cloudUrl)

        then: "the sort order is unchanged"
        group.subGroups.get(0).nodes == [n1, n2, n3, n4, n5, n6]
    }

    def "NodeGroup nodes json format is correct"() {
        given: "a NodeGroup"
        URL n1Url = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(n1Url)
            .pipe(["pipeState": PipeState.UP_TO_DATE.toString(), "v":"1.0"])
            .build()
        URL n2Url = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(n2Url)
            .pipe(["pipeState": PipeState.OUT_OF_DATE.toString(), "v":"1.0"])
            .build()
        NodeGroup group = new NodeGroup([n1, n2])
        when: "the NodeGroup nodes are output as JSON"
        String result = group.nodesToJson()
        then: "the JSON format is correct"
        result ==
            "[" +
                "{" +
                    "\"localUrl\":\"http://node-1\"," +
                    "\"offset\":\"0\"," +
                    "\"pipe\":{\"pipeState\":\"$PipeState.UP_TO_DATE\",\"v\":\"1.0\"}," +
                    "\"id\":\"http://node-1\"" +
                "}," +
                "{" +
                    "\"localUrl\":\"http://node-2\"," +
                    "\"offset\":\"0\"," +
                    "\"pipe\":{\"pipeState\":\"$PipeState.OUT_OF_DATE\",\"v\":\"1.0\"}," +
                    "\"id\":\"http://node-2\"" +
                "}" +
            "]"
    }

    def "Nodes are correctly marked as offline"() {
        given: "A node group"
        Node n1 = Node.builder()
            .localUrl(new URL("http://node-1"))
            .lastSeen(ZonedDateTime.now())
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        Node n2 = Node.builder()
            .localUrl(new URL("http://node-2"))
            .lastSeen(ZonedDateTime.now().minusDays(10))
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        Node n3 = Node.builder()
            .localUrl(new URL("http://node-3"))
            .lastSeen(ZonedDateTime.now().minusDays(3))
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .build()
        NodeGroup group = new NodeGroup([n1, n2, n3])
        when: "requesting nodes be marked offline"
        group.markNodesOfflineIfNotSeenSince(ZonedDateTime.now().minusDays(5))
        then: "Only nodes not seen since the threshold are marked offline"
        group.subGroups.get(0).nodes.get(0).status == FOLLOWING
        group.subGroups.get(0).nodes.get(1).status == OFFLINE
        group.subGroups.get(0).nodes.get(0).status == FOLLOWING
    }

    def "A new subgroup is created if it does not exist"(){
        given: "a node"
        def url1 = new URL("http://node-1")
        Node n1 = Node.builder()
                .localUrl(url1)
                .lastSeen(ZonedDateTime.now())
                .status(FOLLOWING)
                .pipe(["v":"1.0"])
                .build()

        NodeGroup group = new NodeGroup([n1])

        def url2 = new URL("http://node-2")
        Node n2 = Node.builder()
                .localUrl(url2)
                .lastSeen(ZonedDateTime.now())
                .status(FOLLOWING)
                .pipe(["v":"2.0"])
                .build()

        when:
        group.upsert(n2, CLOUD_URL)

        then:
        group.subGroups.size() == 2
        group.subGroups.get(0).subGroupId == "1.0"
        group.subGroups.get(1).subGroupId == "2.0"
        group.subGroups.get(0).nodes.get(0).localUrl == url1
        group.subGroups.get(1).nodes.get(0).localUrl == url2
    }

    def "A node with a version for an already existing subgroup is added to the subgroup"() {
        given: "a node"
        def cloudUrl = CLOUD_URL
        def url1 = new URL("http://node-1")
        Node n1 = Node.builder()
                .localUrl(url1)
                .lastSeen(ZonedDateTime.now())
                .status(FOLLOWING)
                .pipe(["v":"1.0"])
                .requestedToFollow([cloudUrl])
                .build()

        NodeGroup group = new NodeGroup([n1])

        def url2 = new URL("http://node-2")
        Node n2 = Node.builder()
                .localUrl(url2)
                .lastSeen(ZonedDateTime.now())
                .status(FOLLOWING)
                .pipe(["v":"1.0"])
                .build()

        when:
        group.upsert(n2, cloudUrl)

        then:
        group.subGroups.size() == 1
        group.subGroups.get(0).subGroupId == "1.0"
        group.subGroups.get(0).nodes.get(0).localUrl == url1
        group.subGroups.get(0).nodes.get(1).localUrl == url2
    }

    def "A node with new version is added to a new subgroup and removed from the old one"() {
        given: "a node"
        def cloudUrl = CLOUD_URL
        def url1 = new URL("http://node-1")
        Node n1 = Node.builder()
            .localUrl(url1)
            .lastSeen(ZonedDateTime.now())
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .requestedToFollow([cloudUrl])
            .build()

        def url2 = new URL("http://node-2")
        Node n2 = Node.builder()
            .localUrl(url2)
            .lastSeen(ZonedDateTime.now())
            .status(FOLLOWING)
            .pipe(["v":"1.0"])
            .requestedToFollow([cloudUrl])
            .build()

        NodeGroup group = new NodeGroup([n1, n2])

        when: "a new version is deployed to node2"
        Node node2WithNewVersion = Node.builder()
            .localUrl(url2)
            .lastSeen(ZonedDateTime.now())
            .status(FOLLOWING)
            .pipe(["v":"2.0"])
            .build()

        and: "upsert is called"
        group.upsert(node2WithNewVersion, cloudUrl)

        then:
        group.subGroups.size() == 2
        group.subGroups.get(0).subGroupId == "1.0"
        group.subGroups.get(1).subGroupId == "2.0"
        group.subGroups.get(0).nodes.size() == 1
        group.subGroups.get(1).nodes.size() == 1
        group.subGroups.get(0).nodes.get(0).localUrl == url1
        group.subGroups.get(1).nodes.get(0).localUrl == url2
    }
}
