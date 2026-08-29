# Summarization variants

`hitorro-retrieval` ships an LLM-backed summarize stage on the retrieval
pipeline **and** a direct-text primitive exposed by fleet members
(`fleet-retrieval`'s `POST /api/retrieval/summarize`). Both share one
catalogue of prompt templates so every consumer — mesh jobs, ad-hoc
scripts, analytics dashboards — sees the same style names and prompt
behaviour.

## Where the pieces live

| File | Role |
| --- | --- |
| `com.hitorro.jsontypesystem.datamapper.AIOperations` | Interface every LLM backend implements. New method: `withPrompt(text, promptTemplate, maxWords)`. |
| `com.hitorro.retrieval.pipeline.stages.SummarizationRetriever` | The stage in the retrieval pipeline. Reads `summarize.style` / `summarize.prompt` from the JVS query. |
| `com.hitorro.retrieval.pipeline.stages.SummaryStyles` | The prompt catalogue. Six built-ins; `register()` at runtime for more. |
| `com.hitorro.retrieval.aggregate.SummarizationAggregate` | Carries the result + the style + the prompt actually used. |
| `com.hitorro.fleet.retrieval.api.SummarizeController` | REST surface: `GET /api/retrieval/summary-styles` and `POST /api/retrieval/summarize`. |

## Built-in styles

| id | prompt intent |
| --- | --- |
| `BRIEF` | 2-3 sentence factual summary. |
| `CONTRIBUTIONS` | Per-participant breakdown — who added what. |
| `ACTION_ITEMS` | Owner / task / deadline checklist. |
| `DECISIONS` | Decisions reached + who agreed. |
| `SENTIMENT` | Tone across the corpus, with grounded quotes. |
| `ENTITIES` | People / orgs / places / dates referenced. |

Look up the actual prompt text via `SummaryStyles.promptFor("CONTRIBUTIONS")`
or `GET /api/retrieval/summary-styles`.

## Calling via the retrieval pipeline

Add a `summarize` block to any `/api/retrieval/execute` query:

```json
{
  "indexName": "mail",
  "lang": "en",
  "query": {
    "search":    { "query": "sender_domain:acme.com", "limit": 25, "lang": "en" },
    "fetch":     {},
    "fixup":     { "tags": ["basic"] },
    "summarize": { "style": "CONTRIBUTIONS", "maxWords": 300 }
  }
}
```

The response's `aggregates[]` gets a `{"_aggregate":"ai_summary",
"summary":"…", "style":"CONTRIBUTIONS", "prompt":"…", "documentCount":25,
"durationMs":4210}` entry. `SummarizationAggregate.MergePolicy` controls
how cross-index summaries combine on `/search-multiple`.

## Calling directly with assembled text

When you already have the corpus (an assembled email thread, a notebook
scratchpad, a log tail) skip the retrieval pipeline:

```bash
curl -X POST http://localhost:8095/api/retrieval/summarize \
  -H 'Content-Type: application/json' \
  -d '{
        "text": "From: alice@x  Sent: 2026-08-01 …",
        "style": "ACTION_ITEMS"
      }'
```

Response:

```json
{
  "summary": "* Alice: send the RFC to legal by Friday …",
  "style":   "ACTION_ITEMS",
  "elapsedMs": 2618,
  "charCount": 4210
}
```

Return codes: `200` on success, `400` for missing `text` or unknown
`style`, `502` if the underlying LLM call fails, `503` when Ollama isn't
reachable.

## Custom prompt (one-off)

Skip `style` and pass `prompt` directly:

```json
{
  "text": "…",
  "prompt": "Extract every dollar amount mentioned and who mentioned it.",
  "maxWords": 200
}
```

The response's `style` is reported as `"CUSTOM"`. Great for experiments
before promoting a template into `SummaryStyles`.

## Adding a new style

Three lines:

```java
// In your @PostConstruct or wherever your fleet member wires beans:
SummaryStyles.register("ROOT_CAUSE", """
    Read the incident thread below. Identify the root cause of the outage
    (technical + human), separate contributing factors from the trigger,
    and list any monitoring gaps that let it happen. 4-6 sentences.
""");
```

The style is immediately available via
`GET /api/retrieval/summary-styles`, `POST /api/retrieval/summarize?style=ROOT_CAUSE`,
and as the `summarize.style` field on any retrieval query.

## Config

`application.yml` on any fleet member using this:

```yaml
hitorro:
  fleet:
    retrieval:
      ollama:
        enabled: true                     # false disables all AI paths
        url:     http://localhost:11434   # Ollama /api/generate endpoint
        model:   llama3                   # any model available on Ollama
        timeout-seconds: 120
```

When `enabled=false` or Ollama is unreachable at boot:

* `AIOperations.isAvailable()` returns false
* The `summarize` stage on `/execute` silently no-ops (docs still flow)
* `POST /api/retrieval/summarize` returns `503`

## Consumers today

* `hitorro-fleet-mail-analytics` — Threads and Topics pages call
  `/api/retrieval/summarize` with rendered thread / entity corpora.
* Any mesh job that adds a `summarize` block to its query gets the same
  stage output in the aggregates array.
