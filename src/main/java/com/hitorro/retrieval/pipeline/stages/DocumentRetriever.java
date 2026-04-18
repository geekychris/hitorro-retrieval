package com.hitorro.retrieval.pipeline.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.kvstore.Result;
import com.hitorro.kvstore.TypedKVStore;
import com.hitorro.retrieval.context.ContextAttributes;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.docstore.DocumentStore;
import com.hitorro.retrieval.docstore.LocalKVDocumentStore;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches full documents from a {@link DocumentStore} (local RocksDB or remote HTTP).
 * The store document replaces the index projection, carrying over only Lucene metadata.
 */
public class DocumentRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(DocumentRetriever.class);
    private final DocumentStore store;

    public DocumentRetriever(DocumentStore store) {
        this.store = store;
    }

    /** Backward-compatible constructor wrapping TypedKVStore. */
    public DocumentRetriever(TypedKVStore<JsonNode> kvStore) {
        this(new LocalKVDocumentStore(kvStore));
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
        context.setAttribute(ContextAttributes.DOCUMENT_STORE_TYPE, store.getName());
        return input.map(indexDoc -> fetchFromStore(indexDoc));
    }

    private JVS fetchFromStore(JVS indexDoc) {
        String key = extractKey(indexDoc);
        if (key == null) return indexDoc;

        try {
            Result<JsonNode> result = store.get(key);
            if (result.isSuccess() && result.getValue().isPresent()) {
                // KV store has the full enriched document — use it as primary.
                // Only carry over Lucene-specific metadata.
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
            if (doc.exists("id.domain") && doc.exists("id.did")) {
                return doc.getString("id.domain") + "/" + doc.getString("id.did");
            }
            if (doc.exists("id.did")) return doc.getString("id.did");
            if (doc.exists("id")) return doc.getString("id");
        } catch (Exception ignored) {}
        return null;
    }
}
