package com.hitorro.retrieval.pipeline.stages;

import com.fasterxml.jackson.databind.JsonNode;
import com.hitorro.jsontypesystem.BaseT;
import com.hitorro.jsontypesystem.Group;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.jsontypesystem.executors.EnrichExecutionBuilderMapper;
import com.hitorro.jsontypesystem.executors.ExecutionBuilder;
import com.hitorro.jsontypesystem.executors.ProjectionContext;
import com.hitorro.retrieval.context.RetrievalContext;
import com.hitorro.retrieval.pipeline.Retriever;
import com.hitorro.util.core.ArrayUtil;
import com.hitorro.util.core.ListUtil;
import com.hitorro.util.core.events.cache.HashCache;
import com.hitorro.util.core.iterator.AbstractIterator;
import com.hitorro.util.core.map.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Applies field-level enrichment to documents using the type system's
 * execution builder pattern with tag-based filtering.
 *
 * <p>Replicates the enrichment logic from the old FixupRetriever and
 * JVS2JVSEnrichMapper, using the same tag predicate system.
 *
 * <h3>Query format</h3>
 * <pre>
 * {
 *   "fixup": {
 *     "tags": ["basic", "segmented", "ner"]
 *   }
 * }
 * </pre>
 */
public class FixupRetriever implements Retriever {

    private static final Logger log = LoggerFactory.getLogger(FixupRetriever.class);

    @Override
    public boolean participate(JVS query, RetrievalContext context) {
        return query.exists("fixup.tags");
    }

    @Override
    public AbstractIterator<JVS> retrieve(AbstractIterator<JVS> input, JVS query, RetrievalContext context) {
        if (input == null) {
            context.addError("FixupRetriever: no input iterator");
            return null;
        }

        String[] tags = extractTags(query);
        EnrichExecutionBuilderMapper mapper = new EnrichExecutionBuilderMapper();
        mapper.setPredicate(mapper.getPredicate()
                .and(new GroupTagPredicate(tags).or(new GroupTagNullPredicate())));
        String cacheKey = "retrieval-fixup:" + String.join(",", tags);
        HashCache<Type, ExecutionBuilder> cache = Type.getExecBuilderCache(cacheKey, mapper);

        return input.map(doc -> enrich(doc, cache));
    }

    private JVS enrich(JVS doc, HashCache<Type, ExecutionBuilder> cache) {
        Type type = doc.getType();
        if (type == null) return doc;

        try {
            ProjectionContext pc = new ProjectionContext();
            pc.source = doc;
            pc.target = new JVS();
            ExecutionBuilder builder = cache.get(type);
            if (builder != null && builder.getCurrentNode() != null) {
                builder.getCurrentNode().project(pc);
            }
        } catch (Exception e) {
            log.debug("Enrichment failed for type {}: {}", type.getName(), e.getMessage());
        }
        return doc;
    }

    private String[] extractTags(JVS query) {
        try {
            List<String> tags = query.getStringList("fixup.tags");
            if (tags != null && !tags.isEmpty()) {
                return tags.toArray(new String[0]);
            }
        } catch (Exception ignored) {}
        return new String[]{"basic"};
    }

    /** Matches Groups whose tags overlap with the requested tag set. */
    static class GroupTagPredicate implements Predicate<BaseT> {
        private final Set<String> tags;

        GroupTagPredicate(String[] tagsIn) {
            tags = MapUtil.getSet(tagsIn);
        }

        @Override
        public boolean test(BaseT b) {
            if (b instanceof Group group) {
                for (String s : group.getTags()) {
                    if (tags.contains(s)) return true;
                }
            }
            return false;
        }
    }

    /** Matches Groups with null or empty tags (always-execute groups). */
    static class GroupTagNullPredicate implements Predicate<BaseT> {
        @Override
        public boolean test(BaseT b) {
            if (b instanceof Group group) {
                return ListUtil.nullOrEmpty(group.getTags());
            }
            return false;
        }
    }
}
