package com.hitorro.retrieval.cluster;

/**
 * Plugin interface for discovering cluster topology. Implementations may
 * read from YAML files, DNS SRV records, ZooKeeper, etc.
 */
public interface ServiceRegistry {

    ClusterConfig getClusterConfig();

    void refresh();

    boolean isAvailable();
}
