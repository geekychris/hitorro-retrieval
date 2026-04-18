package com.hitorro.retrieval.cluster;

import java.util.Set;

/**
 * Address and role information for a node in the cluster.
 */
public record NodeAddress(String name, String host, int port, Set<NodeRole> roles) {

    public String getBaseUrl() {
        return "http://" + host + ":" + port;
    }

    public boolean hasRole(NodeRole role) {
        return roles != null && roles.contains(role);
    }
}
