package com.hitorro.retrieval.docstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.kvstore.Result;
import com.hitorro.retrieval.cluster.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * DocumentStore that fetches documents from a remote KVStore node via HTTP.
 */
public class RemoteDocumentStore implements DocumentStore {

    private static final Logger log = LoggerFactory.getLogger(RemoteDocumentStore.class);
    private final NodeAddress node;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public RemoteDocumentStore(NodeAddress node) {
        this(node, HttpClient.newHttpClient());
    }

    public RemoteDocumentStore(NodeAddress node, HttpClient httpClient) {
        this.node = node;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Result<JsonNode> get(String key) {
        try {
            String url = node.getBaseUrl() + "/api/documents/"
                    + java.net.URLEncoder.encode(key, java.nio.charset.StandardCharsets.UTF_8);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .header("Accept", "application/json")
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JsonNode doc = objectMapper.readTree(response.body());
                return Result.success(doc);
            } else if (response.statusCode() == 404) {
                return Result.failure("Document not found: " + key);
            } else {
                return Result.failure("HTTP " + response.statusCode() + " from " + node.name());
            }
        } catch (Exception e) {
            log.debug("Remote document fetch failed for {}: {}", key, e.getMessage());
            return Result.failure(e.getMessage());
        }
    }

    @Override
    public String getName() { return "remote:" + node.name(); }

    @Override
    public boolean isAvailable() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(node.getBaseUrl() + "/api/health"))
                    .method("HEAD", HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
