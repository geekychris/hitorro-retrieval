package com.hitorro.retrieval.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.cluster.NodeAddress;
import com.hitorro.retrieval.merger.SortCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * SearchProvider that delegates to a remote search node via HTTP.
 * Expects the remote node to expose a search API compatible with the retrieval protocol.
 */
public class RemoteSearchProvider implements SearchProvider {

    private static final Logger log = LoggerFactory.getLogger(RemoteSearchProvider.class);
    private final NodeAddress node;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public RemoteSearchProvider(NodeAddress node) {
        this(node, HttpClient.newHttpClient());
    }

    public RemoteSearchProvider(NodeAddress node, HttpClient httpClient) {
        this.node = node;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        return search(indexName, queryString, offset, limit, facetDims, lang, null);
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang,
                               SortCriteria sortCriteria) throws Exception {
        String url = node.getBaseUrl() + "/api/search"
                + "?index=" + encode(indexName)
                + "&q=" + encode(queryString)
                + "&offset=" + offset
                + "&limit=" + limit
                + "&lang=" + encode(lang);
        if (facetDims != null && !facetDims.isEmpty()) {
            url += "&facets=" + encode(String.join(",", facetDims));
        }
        // Compact string form — remote /api/search accepts either the
        // array-of-objects (POST body) or the "field:direction[,…]"
        // querystring form. Older nodes ignore unrecognised params.
        String sortWire = encodeSort(sortCriteria);
        if (sortWire != null) {
            url += "&sort=" + encode(sortWire);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .header("Accept", "application/json")
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Remote search failed: HTTP " + response.statusCode());
        }

        return parseSearchResult(response.body(), queryString, offset, limit);
    }

    private SearchResult parseSearchResult(String json, String query, int offset, int limit) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        List<JVS> documents = new ArrayList<>();
        if (root.has("documents") && root.get("documents").isArray()) {
            for (JsonNode doc : root.get("documents")) {
                documents.add(new JVS(doc));
            }
        }

        long totalHits = root.has("totalHits") ? root.get("totalHits").asLong() : documents.size();
        long searchTimeMs = root.has("searchTimeMs") ? root.get("searchTimeMs").asLong() : 0;

        return SearchResult.builder()
                .documents(documents)
                .totalHits(totalHits)
                .query(query)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(searchTimeMs)
                .build();
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

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s, java.nio.charset.StandardCharsets.UTF_8);
    }

    /** Serialise the sort chain into the {@code field:direction[,…]}
     *  compact form. Returns null for a null chain or a chain that
     *  resolves to plain score-desc (Lucene's default, no need to send). */
    static String encodeSort(SortCriteria criteria) {
        if (criteria == null) return null;
        if (criteria.next() == null
                && criteria.type() == SortCriteria.SortType.SCORE
                && criteria.direction() == SortCriteria.Direction.DESC) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (SortCriteria n = criteria; n != null; n = n.next()) {
            if (n.field() == null || n.field().isBlank()) continue;
            if (sb.length() > 0) sb.append(',');
            sb.append(n.field()).append(':')
              .append(n.direction() == SortCriteria.Direction.DESC ? "desc" : "asc");
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
