package com.hitorro.retrieval.search;

import com.hitorro.index.IndexManager;
import com.hitorro.index.config.IndexConfig;
import com.hitorro.index.search.SearchResult;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.merger.RRFMerger;
import com.hitorro.retrieval.merger.ScoreMerger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Search Providers")
class SearchProviderTest {

    private IndexManager indexManager;

    @BeforeEach
    void setUp() throws Exception {
        indexManager = new IndexManager("en");
        IndexConfig config = IndexConfig.inMemory().build();
        indexManager.createIndex("test", config, null);

        for (int i = 1; i <= 5; i++) {
            JVS doc = JVS.read("{\"id\":{\"domain\":\"test\",\"did\":\"doc-" + i + "\"},\"title\":\"Document " + i + "\"}");
            indexManager.indexDocument("test", doc);
        }
        indexManager.commit("test");
    }

    @AfterEach
    void tearDown() throws Exception {
        indexManager.close();
    }

    @Test
    @DisplayName("LuceneSearchProvider delegates to IndexManager")
    void luceneProviderDelegates() throws Exception {
        LuceneSearchProvider provider = new LuceneSearchProvider(indexManager);
        assertThat(provider.getName()).isEqualTo("lucene");
        assertThat(provider.isAvailable()).isTrue();

        SearchResult result = provider.search("test", "*:*", 0, 10, null, "en");
        assertThat(result.getTotalHits()).isEqualTo(5);
        assertThat(result.getDocuments()).hasSize(5);
    }

    @Test
    @DisplayName("CompositeSearchProvider merges from two providers")
    void compositeProviderMerges() throws Exception {
        // Create a second index
        IndexConfig config2 = IndexConfig.inMemory().build();
        indexManager.createIndex("test2", config2, null);
        for (int i = 6; i <= 8; i++) {
            JVS doc = JVS.read("{\"id\":{\"domain\":\"test2\",\"did\":\"doc-" + i + "\"},\"title\":\"Other " + i + "\"}");
            indexManager.indexDocument("test2", doc);
        }
        indexManager.commit("test2");

        // Two providers searching different indexes
        SearchProvider p1 = new SearchProvider() {
            public SearchResult search(String idx, String q, int o, int l, List<String> f, String lang) throws Exception {
                return indexManager.search("test", q, o, l, f, lang);
            }
            public String getName() { return "p1"; }
            public boolean isAvailable() { return true; }
        };
        SearchProvider p2 = new SearchProvider() {
            public SearchResult search(String idx, String q, int o, int l, List<String> f, String lang) throws Exception {
                return indexManager.search("test2", q, o, l, f, lang);
            }
            public String getName() { return "p2"; }
            public boolean isAvailable() { return true; }
        };

        CompositeSearchProvider composite = new CompositeSearchProvider(List.of(p1, p2), new ScoreMerger());
        assertThat(composite.getName()).contains("p1").contains("p2");

        SearchResult result = composite.search("ignored", "*:*", 0, 20, null, "en");
        assertThat(result.getTotalHits()).isEqualTo(8);
        assertThat(result.getDocuments()).hasSize(8);
    }

    @Test
    @DisplayName("CompositeSearchProvider skips unavailable providers")
    void compositeSkipsUnavailable() throws Exception {
        SearchProvider available = new LuceneSearchProvider(indexManager);
        SearchProvider unavailable = new SearchProvider() {
            public SearchResult search(String idx, String q, int o, int l, List<String> f, String lang) { throw new RuntimeException("offline"); }
            public String getName() { return "offline"; }
            public boolean isAvailable() { return false; }
        };

        CompositeSearchProvider composite = new CompositeSearchProvider(List.of(available, unavailable));
        SearchResult result = composite.search("test", "*:*", 0, 10, null, "en");
        assertThat(result.getTotalHits()).isEqualTo(5);
    }
}
