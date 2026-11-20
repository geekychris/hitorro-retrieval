package com.hitorro.retrieval.cluster;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.core.JacksonException;
import com.hitorro.util.json.YamlMappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import com.hitorro.util.json.JsonValues;

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
    /** Reads YAML as {@code new ObjectMapper(new YAMLFactory())} did on Jackson 2 (scalar typing, defaults, limits). */
    private static final ObjectMapper YAML = YamlMappers.yaml();
    private final ObjectMapper yamlMapper;
    private volatile ClusterConfig clusterConfig;

    /** Load from a file path (supports refresh). */
    public YamlServiceRegistry(Path configPath) {
        this.configPath = configPath;
        this.yamlMapper = YAML;
        refresh();
    }

    /** Load from an InputStream (for testing; refresh reloads from same stream is not possible). */
    public YamlServiceRegistry(InputStream configStream) {
        this.configPath = null;
        this.yamlMapper = YAML;
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
        JsonNode root;
        try {
            root = yamlMapper.readTree(is);
        } catch (JacksonException e) {
            // malformed YAML stays an IOException (logged, and the previous/empty config kept), as in 3.x
            throw new IOException(e.getMessage(), e);
        }
        JsonNode clusterNode = root.has("cluster") ? root.get("cluster") : root;

        String name = clusterNode.has("name") ? JsonValues.asText(clusterNode.get("name")) : "default";
        List<NodeAddress> nodes = new ArrayList<>();

        if (clusterNode.has("nodes") && clusterNode.get("nodes").isArray()) {
            for (JsonNode nodeJson : clusterNode.get("nodes")) {
                String nodeName = nodeJson.has("name") ? JsonValues.asText(nodeJson.get("name")) : "node-" + nodes.size();
                String host = nodeJson.has("host") ? JsonValues.asText(nodeJson.get("host")) : "localhost";
                int port = nodeJson.has("port") ? JsonValues.asInt(nodeJson.get("port")) : 8080;

                Set<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
                if (nodeJson.has("roles") && nodeJson.get("roles").isArray()) {
                    for (JsonNode roleNode : nodeJson.get("roles")) {
                        try {
                            roles.add(NodeRole.valueOf(JsonValues.asText(roleNode).toUpperCase()));
                        } catch (IllegalArgumentException e) {
                            log.warn("Unknown node role: {}", JsonValues.asText(roleNode));
                        }
                    }
                }
                nodes.add(new NodeAddress(nodeName, host, port, roles));
            }
        }

        return new ClusterConfig(name, nodes);
    }
}
