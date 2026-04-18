package com.hitorro.retrieval.docstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.kvstore.Result;

/**
 * Abstraction over a document store used by the retrieval pipeline
 * to fetch full documents by key. Implementations may be local (RocksDB)
 * or remote (HTTP to a KVStore node).
 */
public interface DocumentStore {

    Result<JsonNode> get(String key);

    String getName();

    boolean isAvailable();
}
