package com.hitorro.retrieval.pipeline;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;

import java.util.List;

/**
 * An ordered chain of {@link Retriever} stages.
 *
 * <p>Execution filters to participating stages, then chains each stage's
 * {@code retrieve()} call so the output iterator of one stage becomes the
 * input of the next.
 *
 * <pre>
 * IndexRetriever → DocumentRetriever → FixupRetriever → PaginationRetriever → FacetRetriever
 *   (search)        (fetch full docs)   (field fixup)    (skip/take)           (register facets)
 * </pre>
 */
public class RetrievalPipeline {

    private final List<Retriever> stages;

    RetrievalPipeline(List<Retriever> stages) {
        this.stages = List.copyOf(stages);
    }

    /**
     * Executes the pipeline against the given query and context.
     *
     * @param query   the retrieval query (JVS with search parameters)
     * @param context the shared mutable context
     * @return the final document iterator after all participating stages have run
     */
    public AbstractIterator<JVS> execute(JVS query, RetrievalContext context) {
        AbstractIterator<JVS> iter = null;

        for (Retriever stage : stages) {
            if (stage.participate(query, context)) {
                iter = stage.retrieve(iter, query, context);
            }
        }

        if (iter == null) {
            context.addError("No pipeline stages participated");
            return new CollectionIterator<>(List.of());
        }
        return iter;
    }

    /** Returns the number of registered stages (including non-participating ones). */
    public int getStageCount() {
        return stages.size();
    }
}
