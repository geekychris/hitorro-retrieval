package com.hitorro.retrieval.pipeline.stages;

import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.datamapper.AIOperations;
import com.hitorro.retrieval.aggregate.SummarizationAggregate;
import com.hitorro.retrieval.context.ContextAttributes;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.iterator.CollectionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Pipeline stage that uses an {@link AIOperations} implementation to generate
 * a natural-language summary of the search result set.
 *
 * <p>Materializes documents from the input iterator (up to maxDocuments),
 * builds a text representation, calls AI summarization, and registers
 * a {@link SummarizationAggregate}. Documents pass through unchanged.
 *
 * <h3>Query format</h3>
 * <pre>{"summarize": {"enabled": true, "maxDocs": 10, "maxWords": 200}}</pre>
 */
public class SummarizationRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(SummarizationRetriever.class);
    private final AIOperations aiOperations;
    private final int maxDocuments;
    private final int maxSummaryWords;

    public SummarizationRetriever(AIOperations aiOperations) {
        this(aiOperations, 10, 200);
    }

    public SummarizationRetriever(AIOperations aiOperations, int maxDocuments, int maxSummaryWords) {
        this.aiOperations = aiOperations;
        this.maxDocuments = maxDocuments;
        this.maxSummaryWords = maxSummaryWords;
    }

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return query.exists("summarize") && aiOperations != null && aiOperations.isAvailable();
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        if (input == null) return null;

        // Read config overrides from query
        int maxDocs = maxDocuments;
        int maxWords = maxSummaryWords;
        try {
            if (query.exists("summarize.maxDocs")) maxDocs = (int) query.getLong("summarize.maxDocs");
            if (query.exists("summarize.maxWords")) maxWords = (int) query.getLong("summarize.maxWords");
        } catch (Exception ignored) {}

        // Materialize documents
        List<JVS> docs = new ArrayList<>();
        while (input.hasNext()) {
            docs.add(input.next());
        }

        // Build text for summarization from up to maxDocs documents
        int docsToSummarize = Math.min(maxDocs, docs.size());
        StringBuilder textBuilder = new StringBuilder();
        for (int i = 0; i < docsToSummarize; i++) {
            JVS doc = docs.get(i);
            textBuilder.append("Document ").append(i + 1).append(": ");
            textBuilder.append(extractDocumentText(doc));
            textBuilder.append("\n\n");
        }

        // Call AI summarization
        long start = System.currentTimeMillis();
        try {
            String summary = aiOperations.summarize(textBuilder.toString(), maxWords);
            long duration = System.currentTimeMillis() - start;

            context.setAttribute(ContextAttributes.AI_SUMMARY, summary);
            context.addAggregate(new SummarizationAggregate(summary, docsToSummarize, duration));
        } catch (Exception e) {
            log.warn("AI summarization failed: {}", e.getMessage());
            context.addError("Summarization failed: " + e.getMessage());
        }

        // Pass all documents through unchanged
        return new CollectionIterator<>(docs);
    }

    private String extractDocumentText(JVS doc) {
        StringBuilder sb = new StringBuilder();
        // Try title
        try {
            if (doc.exists("title.mls[0].text")) {
                sb.append(doc.getString("title.mls[0].text"));
            } else if (doc.exists("title")) {
                sb.append(doc.getString("title"));
            }
        } catch (Exception ignored) {}

        // Try body/content snippet
        try {
            String bodyPath = null;
            if (doc.exists("body.mls[0].text")) bodyPath = "body.mls[0].text";
            else if (doc.exists("content.mls[0].text")) bodyPath = "content.mls[0].text";
            else if (doc.exists("description.mls[0].text")) bodyPath = "description.mls[0].text";

            if (bodyPath != null) {
                String text = doc.getString(bodyPath);
                if (text.length() > 500) text = text.substring(0, 500) + "...";
                sb.append(" - ").append(text);
            }
        } catch (Exception ignored) {}

        return sb.toString();
    }
}
