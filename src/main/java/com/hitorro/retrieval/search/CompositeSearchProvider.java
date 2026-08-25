/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.search;

import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.aggregate.MergePolicy;
import com.hitorro.retrieval.merger.ResultMerger;
import com.hitorro.retrieval.merger.SelectTreeMerger;
import com.hitorro.retrieval.merger.SortCriteria;
import com.hitorro.retrieval.stream.IteratorSearchStream;
import com.hitorro.retrieval.stream.SearchStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * SearchProvider that fans out a query to multiple providers concurrently
 * and merges via a streaming k-way {@link SelectTreeMerger}.
 *
 * <p>Design changes vs. the pre-2026 implementation:</p>
 * <ul>
 *   <li><b>Concurrent fan-out</b> — providers query in parallel over a
 *       bounded {@link ExecutorService} (16 workers by default). Per-branch
 *       timeout drops slow / failed sources with a WARN log; the remaining
 *       branches still produce a valid (degraded) result.</li>
 *   <li><b>Streaming k-way merge</b> — hands per-provider {@link SearchResult}s
 *       to {@link IteratorSearchStream}s, feeds them to {@link SelectTreeMerger}.
 *       The merger's cursor-park semantics combine per-source aggregates
 *       (summary, facets, ai_summary) via
 *       {@link com.hitorro.retrieval.aggregate.RetrievalAggregate#mergeWith}
 *       with the caller-selected {@link MergePolicy}.</li>
 *   <li><b>Composite sort keys</b> — accepts a {@link SortCriteria} chain
 *       for the merger comparator. Backward-compat: null → byScore().</li>
 *   <li><b>Provenance</b> — every merged doc carries {@code _sourceIndex}
 *       so consumers can attribute a hit without a lookup.</li>
 * </ul>
 *
 * <p>Backward-compat: the {@link ResultMerger}-taking constructor stays
 * (the merger arg is ignored — SelectTreeMerger takes over). Legacy
 * mergers (ScoreMerger / RRFMerger / FieldSortMerger) remain on disk
 * for as-a-{@link ResultMerger}-callback uses; the flat-merger code
 * path inside this class is retired in favour of SelectTreeMerger,
 * which produces identical output for single-key sorts.</p>
 */
public class CompositeSearchProvider implements SearchProvider {

    private static final Logger log = LoggerFactory.getLogger(CompositeSearchProvider.class);

    /** Shared executor across all CompositeSearchProvider instances —
     *  one bounded pool for every federated search in the JVM. Override
     *  in tests / Spring configs via the injecting constructor. */
    private static final ExecutorService DEFAULT_EXECUTOR = Executors.newFixedThreadPool(
            Integer.parseInt(System.getProperty("hitorro.retrieval.fanout.pool-size", "16")),
            named("hitorro-retrieval-fanout"));

    /** Default per-branch timeout in milliseconds. Slow / failed sources
     *  drop from the merge; remaining sources still produce a result. */
    private static final long DEFAULT_PER_BRANCH_TIMEOUT_MS = Long.parseLong(
            System.getProperty("hitorro.retrieval.fanout.per-branch-timeout-ms", "10000"));

    private final List<SearchProvider> providers;
    private final SortCriteria sortCriteria;
    private final Map<String, MergePolicy> mergePolicies;
    private final ExecutorService executor;
    private final long perBranchTimeoutMs;

    /** Full constructor — usually invoked by
     *  {@code FleetCoordinatorService.searchMultiple} which knows the
     *  caller-requested sort chain + per-aggregate policy map. */
    public CompositeSearchProvider(List<SearchProvider> providers,
                                   SortCriteria sortCriteria,
                                   Map<String, MergePolicy> mergePolicies,
                                   ExecutorService executor,
                                   long perBranchTimeoutMs) {
        this.providers = List.copyOf(providers);
        this.sortCriteria = sortCriteria != null ? sortCriteria : SortCriteria.byScore();
        this.mergePolicies = mergePolicies != null ? mergePolicies : Map.of();
        this.executor = executor != null ? executor : DEFAULT_EXECUTOR;
        this.perBranchTimeoutMs = perBranchTimeoutMs > 0 ? perBranchTimeoutMs : DEFAULT_PER_BRANCH_TIMEOUT_MS;
    }

    /** Convenience — defaults sort to byScore, empty policy map, shared
     *  executor + default timeout. */
    public CompositeSearchProvider(List<SearchProvider> providers) {
        this(providers, SortCriteria.byScore(), Map.of(), DEFAULT_EXECUTOR, DEFAULT_PER_BRANCH_TIMEOUT_MS);
    }

    /** Legacy compat — accepts a ResultMerger for callers that haven't
     *  yet migrated. The merger arg is IGNORED (SelectTreeMerger takes
     *  over). Kept only to avoid breaking existing constructors. */
    public CompositeSearchProvider(List<SearchProvider> providers,
                                   @SuppressWarnings("unused") ResultMerger ignoredMerger) {
        this(providers);
    }

    @Override
    public SearchResult search(String indexName, String queryString, int offset, int limit,
                               List<String> facetDims, String lang) throws Exception {
        if (providers.isEmpty()) {
            return SearchResult.builder()
                    .documents(List.of()).totalHits(0)
                    .query(queryString).offset(offset).limit(limit).build();
        }

        // Single-provider shortcut — no fan-out, no merger overhead;
        // stays byte-identical to what the provider itself returns.
        if (providers.size() == 1 && providers.get(0).isAvailable()) {
            return providers.get(0).search(indexName, queryString, offset, limit, facetDims, lang);
        }

        // Concurrent fan-out — each provider gets 0..offset+limit so
        // the merger has enough per-source docs to slice from.
        int perProviderLimit = offset + limit;
        List<CompletableFuture<SourceResult>> futures = new ArrayList<>(providers.size());
        for (SearchProvider p : providers) {
            if (!p.isAvailable()) continue;
            String label = providerLabel(p);
            CompletableFuture<SourceResult> f = CompletableFuture.supplyAsync(() -> {
                try {
                    SearchResult sr = p.search(indexName, queryString, 0, perProviderLimit, facetDims, lang);
                    return new SourceResult(label, sr, null);
                } catch (Exception e) {
                    return new SourceResult(label, null, e);
                }
            }, executor).orTimeout(perBranchTimeoutMs, TimeUnit.MILLISECONDS)
              .exceptionally(t -> new SourceResult(label, null, unwrapCause(t)));
            futures.add(f);
        }

        // Wait for all branches. Timeouts + failures were wrapped into
        // SourceResult with an error field, so .join() here never throws.
        List<SourceResult> completed = futures.stream()
                .map(CompletableFuture::join)
                .toList();

        // Log failures + drop them from the merge.
        List<SearchStream> streams = new ArrayList<>();
        for (SourceResult r : completed) {
            if (r.error != null) {
                log.warn("Source '{}' dropped from federated merge: {}", r.sourceName, r.error.toString());
                continue;
            }
            if (r.result != null) {
                streams.add(new IteratorSearchStream(r.sourceName, r.result, /*extraAggregates*/ null));
            }
        }

        if (streams.isEmpty()) {
            // Every branch failed — surface the first failure.
            Exception first = completed.stream().map(r -> r.error)
                    .filter(java.util.Objects::nonNull).findFirst().orElse(null);
            if (first != null) throw first;
            return SearchResult.builder().documents(List.of()).totalHits(0)
                    .query(queryString).offset(offset).limit(limit).build();
        }

        SelectTreeMerger merger = new SelectTreeMerger(sortCriteria, mergePolicies);
        SearchResult merged = merger.merge(streams, offset, limit);
        // Preserve query echo — the merger doesn't know it.
        return SearchResult.builder()
                .documents(merged.getDocuments())
                .totalHits(merged.getTotalHits())
                .facets(merged.getFacets())
                .query(queryString)
                .offset(offset)
                .limit(limit)
                .searchTimeMs(merged.getSearchTimeMs())
                .sourceAggregates(merged.getSourceAggregates())
                .build();
    }

    @Override
    public String getName() {
        return "composite[" + providers.stream()
                .map(SearchProvider::getName)
                .collect(Collectors.joining(",")) + "]";
    }

    @Override
    public boolean isAvailable() {
        return providers.stream().anyMatch(SearchProvider::isAvailable);
    }

    // ==================== helpers ============================================

    /** Extract a stable label from a SearchProvider — usually
     *  {@code lucene:<indexName>} / {@code read-only-lucene:<indexName>}
     *  / {@code remote:<endpoint>}. The prefix is stripped so byIndex
     *  map keys are just the index name. Falls back to className. */
    private static String providerLabel(SearchProvider p) {
        String n = p.getName();
        if (n == null || n.isBlank()) return p.getClass().getSimpleName();
        int colon = n.indexOf(':');
        return colon >= 0 && colon < n.length() - 1 ? n.substring(colon + 1) : n;
    }

    private static Exception unwrapCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null &&
                (cur instanceof CompletionException
                        || cur instanceof ExecutionException
                        || cur instanceof TimeoutException)) {
            cur = cur.getCause();
        }
        return cur instanceof Exception e ? e : new Exception(cur);
    }

    /** ThreadFactory that names threads for grep-ability in profiler output. */
    private static ThreadFactory named(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** Per-source result of a concurrent branch. Either has a result OR
     *  has an error — never both. */
    private record SourceResult(String sourceName, SearchResult result, Exception error) { }
}
