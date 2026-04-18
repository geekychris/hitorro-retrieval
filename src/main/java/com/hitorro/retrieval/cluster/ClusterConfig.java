package com.hitorro.retrieval.cluster;

import java.util.Collections;
import java.util.List;

/**
 * Configuration describing the nodes in a retrieval cluster and their roles.
 */
public class ClusterConfig {

    private final String clusterName;
    private final List<NodeAddress> nodes;

    public ClusterConfig(String clusterName, List<NodeAddress> nodes) {
        this.clusterName = clusterName;
        this.nodes = Collections.unmodifiableList(nodes);
    }

    public String getClusterName() { return clusterName; }

    public List<NodeAddress> getNodes() { return nodes; }

    public List<NodeAddress> getNodesByRole(NodeRole role) {
        return nodes.stream().filter(n -> n.hasRole(role)).toList();
    }

    public NodeAddress getNodeByName(String name) {
        return nodes.stream().filter(n -> n.name().equals(name)).findFirst().orElse(null);
    }

    public int getNodeCount() { return nodes.size(); }
}
