package com.hitorro.retrieval;

import com.hitorro.jsontypesystem.Type;

/**
 * Configuration for a retrieval target (index + type + language).
 *
 * <p>Simplified replacement for the old CollectionConfig which loaded
 * from JSON files and managed query modulators and intent maps.
 */
public class RetrievalConfig {

    private final String indexName;
    private final Type type;
    private final String defaultLang;

    public RetrievalConfig(String indexName, Type type, String defaultLang) {
        this.indexName = indexName;
        this.type = type;
        this.defaultLang = defaultLang != null ? defaultLang : "en";
    }

    public RetrievalConfig(String indexName, Type type) {
        this(indexName, type, "en");
    }

    public RetrievalConfig(String indexName) {
        this(indexName, null, "en");
    }

    public String getIndexName() { return indexName; }

    public Type getType() { return type; }

    public String getDefaultLang() { return defaultLang; }
}
