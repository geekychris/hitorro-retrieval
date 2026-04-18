package com.hitorro.retrieval.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.cluster.NodeAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

/**
 * SearchProvider that streams results from a remote node via NDJson.
 *
 * <p>The NDJson stream is type-tagged and ordered:
 * <ol>
 *   <li>{@code _type: "meta"} — metadata (totalHits, searchTimeMs)</li>
 *   <li>{@code _type: "doc"} — documents (one per line, streamed lazily)</li>
 *   <li>{@code _type: "facets"} — facet data (after all documents)</li>
 *   <li>{@code _type: "aggregate"} — other aggregates (summaries, etc.)</li>
 * </ol>
 *
 * <p>Documents are yielded lazily as they arrive. Tail objects (facets, aggregates)
 * are collected and available after the document iterator is exhausted.
 */
public class StreamingRemoteSearchProvider implements SearchProvider {

    private static final Logger log = LoggerFactory.getLogger(StreamingRemoteSearchProvider.class);
    private final NodeAddress node;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public StreamingRemoteSearchProvider(NodeAddress node) {
        this(node, HttpClient.newHttpClient());
    }

    public StreamingRemoteSearchProvider(NodeAddress node, HttpClient httpClient) {
        this.node = node;
        this.httpClient = httpClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        StreamingResult streaming = searchStreaming(indexName, queryString, offset, limit, facetDims, lang);
        List<JVS> docs = new ArrayList<>();
        streaming.documents().forEachRemaining(docs::add);
        return SearchResult.builder()
                .documents(docs)
                .totalHits(streaming.totalHits())
                .query(queryString)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(streaming.searchTimeMs())
                .build();
    }

    /**
     * Stream search results from the remote node. Returns metadata + a lazy document
     * iterator. Tail objects (facets, aggregates) are available after document iteration.
     */
    public StreamingResult searchStreaming(String indexName, String queryString, int offset, int limit,
                                          List<String> facetDims, String lang) throws Exception {
        String url = node.getBaseUrl() + "/api/search/stream"
                + "?index=" + encode(indexName)
                + "&q=" + encode(queryString)
                + "&offset=" + offset
                + "&limit=" + limit
                + "&lang=" + encode(lang);
        if (facetDims != null && !facetDims.isEmpty()) {
            url += "&facets=" + encode(String.join(",", facetDims));
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .header("Accept", "application/x-ndjson")
                .build();

        HttpResponse<java.io.InputStream> response = httpClient.send(request,
                HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            response.body().close();
            throw new RuntimeException("Remote streaming search failed: HTTP " + response.statusCode());
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(response.body()));

        // First line: metadata
        String metaLine = reader.readLine();
        long totalHits = 0;
        long searchTimeMs = 0;
        if (metaLine != null) {
            JsonNode meta = objectMapper.readTree(metaLine);
            totalHits = meta.has("totalHits") ? meta.get("totalHits").asLong() : 0;
            searchTimeMs = meta.has("searchTimeMs") ? meta.get("searchTimeMs").asLong() : 0;
        }

        TypedNDJsonIterator iterator = new TypedNDJsonIterator(reader, objectMapper);
        return new StreamingResult(totalHits, searchTimeMs, iterator, iterator::getTailObjects);
    }

    @Override
    public String getName() { return "streaming-remote:" + node.name(); }

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

    /**
     * Result of a streaming search.
     * @param totalHits total matching documents
     * @param searchTimeMs search execution time
     * @param documents lazy document iterator (yields only _type:"doc" objects)
     * @param tailObjects supplier for tail objects (facets, aggregates) — available after documents exhausted
     */
    public record StreamingResult(
            long totalHits, long searchTimeMs,
            Iterator<JVS> documents,
            java.util.function.Supplier<List<JVS>> tailObjects
    ) {}

    /**
     * Type-aware NDJson iterator. Yields documents (type "doc") lazily.
     * Non-document lines (facets, aggregates) are collected into a tail list
     * accessible after the iterator is exhausted.
     */
    static class TypedNDJsonIterator implements Iterator<JVS> {
        private final BufferedReader reader;
        private final ObjectMapper objectMapper;
        private final List<JVS> tailObjects = new ArrayList<>();
        private JVS nextDoc = null;
        private boolean done = false;

        TypedNDJsonIterator(BufferedReader reader, ObjectMapper objectMapper) {
            this.reader = reader;
            this.objectMapper = objectMapper;
            advance();
        }

        private void advance() {
            nextDoc = null;
            while (!done) {
                try {
                    String line = reader.readLine();
                    if (line == null || line.isBlank()) {
                        done = true;
                        try { reader.close(); } catch (Exception ignored) {}
                        return;
                    }

                    JsonNode node = objectMapper.readTree(line);
                    String type = node.has("_type") ? node.get("_type").asText() : "doc";

                    if ("doc".equals(type)) {
                        nextDoc = new JVS(node);
                        return;
                    } else {
                        // Facets, aggregates, etc. — collect for tail
                        tailObjects.add(new JVS(node));
                    }
                } catch (Exception e) {
                    done = true;
                    try { reader.close(); } catch (Exception ignored) {}
                }
            }
        }

        @Override
        public boolean hasNext() { return nextDoc != null; }

        @Override
        public JVS next() {
            if (nextDoc == null) throw new NoSuchElementException();
            JVS result = nextDoc;
            advance();
            return result;
        }

        List<JVS> getTailObjects() { return tailObjects; }
    }
}
