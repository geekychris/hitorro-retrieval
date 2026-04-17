package com.hitorro.retrieval.pipeline.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.JVSMerger;
import com.hitorro.kvstore.Result;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches full documents from the RocksDB KVStore, merging them into
 * the index-only projections returned by {@link IndexRetriever}.
 *
 * <p>Replaces the old ObjectRetriever which used HTTP transport to fetch
 * compressed JVS objects from a Xodus-backed service. The new implementation
 * is in-process -- a simple {@code store.get(key)} call per document.
 *
 * <h3>Query format</h3>
 * <pre>
 * {
 *   "fetch": {
 *     "enabled": true,
 *     "keyField": "id.did"
 *   }
 * }
 * </pre>
 *
 * <p>If {@code keyField} is not specified, the default key extraction tries
 * {@code id.domain/id.did} (matching the pattern used by hitorro-jvs-example-springboot's
 * DocumentStoreService).
 */
public class DocumentRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(DocumentRetriever.class);
    private final TypedKVStore<JsonNode> store;

    public DocumentRetriever(TypedKVStore<JsonNode> store) {
        this.store = store;
    }

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        try {
            if (!query.exists("fetch")) return false;
            if (query.exists("fetch.enabled")) {
                return "true".equals(query.getString("fetch.enabled"));
            }
            return true;
        } catch (Exception e) {
            return query.exists("fetch");
        }
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        if (input == null) {
            context.addError("DocumentRetriever: no input iterator");
            return null;
        }
        return input.map(indexDoc -> enrichFromStore(indexDoc));
    }

    private JVS enrichFromStore(JVS indexDoc) {
        String key = extractKey(indexDoc);
        if (key == null) {
            return indexDoc;
        }
        try {
            Result<JsonNode> result = store.get(key);
            if (result.isSuccess() && result.getValue().isPresent()) {
                // KV store has the full enriched document — use it as primary.
                // Only carry over Lucene-specific metadata (_score, _uid, _index).
                JVS fullDoc = new JVS(result.getValue().get());
                if (indexDoc.exists("_score")) fullDoc.set("_score", indexDoc.get("_score"));
                if (indexDoc.exists("_uid")) fullDoc.set("_uid", indexDoc.get("_uid"));
                if (indexDoc.exists("_index")) fullDoc.set("_index", indexDoc.get("_index"));
                return fullDoc;
            }
        } catch (Exception e) {
            log.debug("Failed to fetch document {}: {}", key, e.getMessage());
        }
        return indexDoc;
    }

    private String extractKey(JVS doc) {
        try {
            // Try domain/did pattern first (standard hitorro key format)
            if (doc.exists("id.domain") && doc.exists("id.did")) {
                return doc.getString("id.domain") + "/" + doc.getString("id.did");
            }
            // Fallback to id.did
            if (doc.exists("id.did")) {
                return doc.getString("id.did");
            }
            // Fallback to id
            if (doc.exists("id")) {
                return doc.getString("id");
            }
        } catch (Exception ignored) {}
        return null;
    }
}
