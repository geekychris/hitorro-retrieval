package com.hitorro.retrieval.cluster;

/**
 * Placeholder ServiceRegistry that resolves nodes via DNS SRV records.
 * Intended for environments where a DNS service routing layer
 * handles node discovery (e.g., Kubernetes, Consul).
 */
public class DnsServiceRegistry implements ServiceRegistry {

    private final String serviceName;
    private final String domain;

    public DnsServiceRegistry(String serviceName, String domain) {
        this.serviceName = serviceName;
        this.domain = domain;
    }

    @Override
    public ClusterConfig getClusterConfig() {
        throw new UnsupportedOperationException(
                "DNS service discovery not yet implemented. " +
                "Use YamlServiceRegistry or implement this for your DNS routing layer. " +
                "Service: " + serviceName + "." + domain);
    }

    @Override
    public void refresh() {
        // no-op
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
