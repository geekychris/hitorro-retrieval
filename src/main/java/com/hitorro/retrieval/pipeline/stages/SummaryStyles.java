/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.retrieval.pipeline.stages;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Catalogue of built-in prompt templates used by
 * {@link SummarizationRetriever}. A "style" is just a symbolic name that
 * resolves to a prompt string; the retriever hands that string to
 * {@link com.hitorro.jsontypesystem.datamapper.AIOperations#withPrompt}.
 *
 * <h3>Built-in styles</h3>
 * <ul>
 *   <li>{@code BRIEF} — 2-3 sentence factual summary (default).</li>
 *   <li>{@code CONTRIBUTIONS} — per-participant breakdown; who added what.
 *       Right for multi-author threads.</li>
 *   <li>{@code ACTION_ITEMS} — owner / task / deadline checklist.</li>
 *   <li>{@code DECISIONS} — decisions reached + who agreed.</li>
 *   <li>{@code SENTIMENT} — tone across the corpus.</li>
 *   <li>{@code ENTITIES} — real-world entities grouped by type.</li>
 * </ul>
 *
 * <h3>Adding a new style</h3>
 *
 * <p>Three-line change:
 * <ol>
 *   <li>Add a {@code public static final String X = "X";} constant here.</li>
 *   <li>Add its prompt to {@link #BUILT_IN} (the LinkedHashMap literal
 *       below).</li>
 *   <li>Callers pass {@code "X"} in the JVS {@code summarize.style} field
 *       (case-insensitive) or via
 *       {@code POST /api/retrieval/summarize?style=X} on fleet-retrieval.</li>
 * </ol>
 *
 * <h3>Overriding at call-time</h3>
 *
 * <p>Callers can supply an inline {@code summarize.prompt} instead of (or
 * in addition to) a style — the raw prompt wins. Useful for one-off
 * experiments before promoting a template into a named style.</p>
 *
 * <h3>Registry lookup</h3>
 *
 * <p>Downstream code adding styles at runtime (plugins, dynamic
 * templates loaded from disk, ...) can call {@link #register(String, String)}
 * before the first retrieval executes. Static-only for now; the map is
 * a {@code LinkedHashMap} so registration order = enumeration order.</p>
 */
public final class SummaryStyles {

    /** Built-in style names. Case-insensitive at lookup time. */
    public static final String BRIEF          = "BRIEF";
    public static final String CONTRIBUTIONS  = "CONTRIBUTIONS";
    public static final String ACTION_ITEMS   = "ACTION_ITEMS";
    public static final String DECISIONS      = "DECISIONS";
    public static final String SENTIMENT      = "SENTIMENT";
    public static final String ENTITIES       = "ENTITIES";

    /** Map preserves insertion order — first entry is the display default. */
    private static final Map<String, String> BUILT_IN = new LinkedHashMap<>();
    static {
        BUILT_IN.put(BRIEF, """
                Summarize the content below in 2-3 sentences.
                Report the topic, the outcome (if any), and any unresolved question.
                Do not restate any titles or subject lines verbatim.""");

        BUILT_IN.put(CONTRIBUTIONS, """
                The content below has multiple participants who each contributed
                incrementally. For every distinct participant, list what THEY added,
                proposed, asked, or agreed to — one bullet per participant, prefixed
                with their name or identifier. Do NOT produce a whole-corpus summary;
                the goal is to attribute each addition to its author.""");

        BUILT_IN.put(ACTION_ITEMS, """
                Extract action items from the content below. For each item, list:
                - who owns it (participant name or role)
                - what needs to be done
                - by when (if a deadline is mentioned)
                Format as a Markdown bulleted list. If nothing actionable is present,
                say so explicitly.""");

        BUILT_IN.put(DECISIONS, """
                Extract decisions reached in the content below. For each decision:
                - what was decided
                - who agreed (participants)
                - any dissent or open question
                Format as a Markdown bulleted list. If nothing was decided, say so.""");

        BUILT_IN.put(SENTIMENT, """
                Read the content below and describe the tone across it.
                Note whether it is neutral, urgent, cordial, tense, escalating, or
                resolving; call out any participant whose tone differs sharply from
                the rest. Ground each observation in a specific quoted phrase so
                the reader can cross-check. 3-5 sentences.""");

        BUILT_IN.put(ENTITIES, """
                From the content below, list the distinct real-world entities
                referenced (people, organizations, products, places, dates).
                Group by type. One bullet per entity, with a short note on
                the context it appears in.""");
    }

    /** User-registered templates. Overlays {@link #BUILT_IN}. */
    private static final Map<String, String> USER_ADDED = new LinkedHashMap<>();

    private SummaryStyles() {}

    /**
     * Resolve a style name to its prompt template. Case-insensitive.
     * Returns {@code null} if the style is unknown — callers should
     * decide whether to fall back to {@code BRIEF} or return an error.
     */
    public static String promptFor(String style) {
        if (style == null || style.isBlank()) return null;
        String key = style.trim().toUpperCase();
        String user = USER_ADDED.get(key);
        return user != null ? user : BUILT_IN.get(key);
    }

    /** All known style names in registration order (built-ins first). */
    public static java.util.List<String> knownStyles() {
        java.util.List<String> out = new java.util.ArrayList<>(BUILT_IN.keySet());
        for (String k : USER_ADDED.keySet()) if (!out.contains(k)) out.add(k);
        return out;
    }

    /** Register a new style at runtime. Idempotent — later registrations
     *  overwrite earlier ones for the same key. Case is normalized to
     *  upper-case on insert. */
    public static void register(String name, String promptTemplate) {
        if (name == null || name.isBlank() || promptTemplate == null) return;
        USER_ADDED.put(name.trim().toUpperCase(), promptTemplate);
    }

    /** Undo a runtime registration. Built-ins are not removable. */
    public static void unregister(String name) {
        if (name == null) return;
        USER_ADDED.remove(name.trim().toUpperCase());
    }
}
