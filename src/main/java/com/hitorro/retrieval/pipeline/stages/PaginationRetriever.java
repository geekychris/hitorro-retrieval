package com.hitorro.retrieval.pipeline.stages;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;

/**
 * Applies client-side pagination (skip + take) over the document iterator.
 *
 * <p>For most queries, offset/limit is handled directly by {@link IndexRetriever}
 * at the Lucene level. This stage is for additional pagination on top of
 * post-processed results (e.g., after fixup changes the result set).
 *
 * <h3>Query format</h3>
 * <pre>
 * {
 *   "page": {
 *     "rows": 10,
 *     "page": 2
 *   }
 * }
 * </pre>
 */
public class PaginationRetriever implements Retriever {

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return query.exists("page.rows");
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        if (input == null) {
            context.addError("PaginationRetriever: no input iterator");
            return null;
        }

        int rows = getInt(query, "page.rows", 10);
        int page = getInt(query, "page.page", 0);
        int skip = rows * page;

        return input.skipNTakeM(skip, rows, true);
    }

    private int getInt(JVS query, String path, int defaultValue) {
        try {
            if (query.exists(path)) {
                return (int) query.getLong(path);
            }
        } catch (Exception ignored) {}
        return defaultValue;
    }
}
