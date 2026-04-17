package com.hitorro.retrieval.pipeline;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;

/**
 * A single stage in the retrieval pipeline.
 *
 * <p>Each retriever decides whether it participates based on the query,
 * then wraps or replaces the incoming document iterator with its own behavior.
 * Stages are chained in order: the output iterator of one stage becomes the
 * input iterator of the next.
 */
public interface Retriever {

    /**
     * Decides whether this retriever should participate in the current pipeline
     * execution. Called before {@link #retrieve} -- if this returns false, the
     * stage is skipped entirely.
     *
     * @param query   the retrieval query (JVS document with search parameters)
     * @param context the shared retrieval context
     * @return true if this stage should execute
     */
    boolean participate(JVS query, RetrievalContext context);

    /**
     * Executes this pipeline stage.
     *
     * @param input   the document iterator from the previous stage (null for the first stage)
     * @param query   the retrieval query
     * @param context the shared retrieval context (stages may read/write state here)
     * @return the document iterator to pass to the next stage
     */
    AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context);
}
