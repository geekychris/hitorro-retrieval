package com.hitorro.retrieval.cluster;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Cluster Config & YAML Registry")
class ClusterConfigTest {

    @Test
    void shouldParseClusterYaml() {
        InputStream is = getClass().getResourceAsStream("/test-cluster.yaml");
        YamlServiceRegistry registry = new YamlServiceRegistry(is);

        assertThat(registry.isAvailable()).isTrue();
        ClusterConfig config = registry.getClusterConfig();
        assertThat(config.getClusterName()).isEqualTo("test-cluster");
        assertThat(config.getNodes()).hasSize(5);
    }

    @Test
    void shouldFilterNodesByRole() {
        InputStream is = getClass().getResourceAsStream("/test-cluster.yaml");
        ClusterConfig config = new YamlServiceRegistry(is).getClusterConfig();

        List<NodeAddress> indexNodes = config.getNodesByRole(NodeRole.INDEX);
        assertThat(indexNodes).hasSize(3); // index-1, index-2, coord-1 (has both COORDINATOR + INDEX)

        List<NodeAddress> kvNodes = config.getNodesByRole(NodeRole.KVSTORE);
        assertThat(kvNodes).hasSize(2);

        List<NodeAddress> coordNodes = config.getNodesByRole(NodeRole.COORDINATOR);
        assertThat(coordNodes).hasSize(1);
    }

    @Test
    void shouldFindNodeByName() {
        InputStream is = getClass().getResourceAsStream("/test-cluster.yaml");
        ClusterConfig config = new YamlServiceRegistry(is).getClusterConfig();

        NodeAddress kv1 = config.getNodeByName("kv-1");
        assertThat(kv1).isNotNull();
        assertThat(kv1.host()).isEqualTo("kv1.hitorro.local");
        assertThat(kv1.port()).isEqualTo(8081);
        assertThat(kv1.hasRole(NodeRole.KVSTORE)).isTrue();
        assertThat(kv1.hasRole(NodeRole.INDEX)).isFalse();
    }

    @Test
    void shouldBuildBaseUrl() {
        InputStream is = getClass().getResourceAsStream("/test-cluster.yaml");
        ClusterConfig config = new YamlServiceRegistry(is).getClusterConfig();

        NodeAddress node = config.getNodeByName("index-1");
        assertThat(node.getBaseUrl()).isEqualTo("http://index1.hitorro.local:8080");
    }

    @Test
    void shouldReturnNullForMissingNode() {
        InputStream is = getClass().getResourceAsStream("/test-cluster.yaml");
        ClusterConfig config = new YamlServiceRegistry(is).getClusterConfig();

        assertThat(config.getNodeByName("nonexistent")).isNull();
    }

    @Test
    void dnsRegistryShouldNotBeAvailable() {
        DnsServiceRegistry dns = new DnsServiceRegistry("hitorro", "local");
        assertThat(dns.isAvailable()).isFalse();
    }
}
