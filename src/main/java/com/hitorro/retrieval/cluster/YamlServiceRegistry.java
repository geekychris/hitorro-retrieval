package com.hitorro.retrieval.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * ServiceRegistry backed by a YAML configuration file.
 *
 * <h3>Expected format</h3>
 * <pre>
 * cluster:
 *   name: hitorro-cluster
 *   nodes:
 *     - name: index-1
 *       host: index1.hitorro.local
 *       port: 8080
 *       roles: [INDEX]
 *     - name: kv-1
 *       host: kv1.hitorro.local
 *       port: 8081
 *       roles: [KVSTORE]
 *     - name: coord-1
 *       host: coord1.hitorro.local
 *       port: 8082
 *       roles: [COORDINATOR, INDEX]
 * </pre>
 */
public class YamlServiceRegistry implements ServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(YamlServiceRegistry.class);
    private final Path configPath;
    private final ObjectMapper yamlMapper;
    private volatile ClusterConfig clusterConfig;

    /** Load from a file path (supports refresh). */
    public YamlServiceRegistry(Path configPath) {
        this.configPath = configPath;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
        refresh();
    }

    /** Load from an InputStream (for testing; refresh reloads from same stream is not possible). */
    public YamlServiceRegistry(InputStream configStream) {
        this.configPath = null;
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
        try {
            this.clusterConfig = parse(configStream);
        } catch (IOException e) {
            log.error("Failed to parse YAML config from stream", e);
            this.clusterConfig = new ClusterConfig("unknown", List.of());
        }
    }

    @Override
    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    @Override
    public void refresh() {
        if (configPath == null) return;
        try (InputStream is = Files.newInputStream(configPath)) {
            this.clusterConfig = parse(is);
            log.info("Cluster config refreshed: {} nodes in '{}'",
                    clusterConfig.getNodeCount(), clusterConfig.getClusterName());
        } catch (IOException e) {
            log.error("Failed to refresh cluster config from {}", configPath, e);
            if (this.clusterConfig == null) {
                this.clusterConfig = new ClusterConfig("unknown", List.of());
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return clusterConfig != null && !clusterConfig.getNodes().isEmpty();
    }

    private ClusterConfig parse(InputStream is) throws IOException {
        JsonNode root = yamlMapper.readTree(is);
        JsonNode clusterNode = root.has("cluster") ? root.get("cluster") : root;

        String name = clusterNode.has("name") ? clusterNode.get("name").asText() : "default";
        List<NodeAddress> nodes = new ArrayList<>();

        if (clusterNode.has("nodes") && clusterNode.get("nodes").isArray()) {
            for (JsonNode nodeJson : clusterNode.get("nodes")) {
                String nodeName = nodeJson.has("name") ? nodeJson.get("name").asText() : "node-" + nodes.size();
                String host = nodeJson.has("host") ? nodeJson.get("host").asText() : "localhost";
                int port = nodeJson.has("port") ? nodeJson.get("port").asInt() : 8080;

                Set<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
                if (nodeJson.has("roles") && nodeJson.get("roles").isArray()) {
                    for (JsonNode roleNode : nodeJson.get("roles")) {
                        try {
                            roles.add(NodeRole.valueOf(roleNode.asText().toUpperCase()));
                        } catch (IllegalArgumentException e) {
                            log.warn("Unknown node role: {}", roleNode.asText());
                        }
                    }
                }
                nodes.add(new NodeAddress(nodeName, host, port, roles));
            }
        }

        return new ClusterConfig(name, nodes);
    }
}
