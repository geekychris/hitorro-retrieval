package com.hitorro.retrieval.docstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.kvstore.Result;
import com.hitorro.kvstore.TypedKVStore;

/**
 * DocumentStore backed by a local hitorro-kvstore {@link TypedKVStore}.
 */
public class LocalKVDocumentStore implements DocumentStore {

    private final TypedKVStore<JsonNode> store;

    public LocalKVDocumentStore(TypedKVStore<JsonNode> store) {
        this.store = store;
    }

    @Override
    public Result<JsonNode> get(String key) {
        return store.get(key);
    }

    @Override
    public String getName() { return "local-kv"; }

    @Override
    public boolean isAvailable() { return store.isOpen(); }
}
