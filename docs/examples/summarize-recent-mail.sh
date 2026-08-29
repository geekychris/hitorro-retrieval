#!/usr/bin/env bash
# Demonstrates the two ways to invoke the summarizer against a running
# hitorro-fleet-retrieval on :8095. Requires Ollama on :11434.
set -euo pipefail
BASE="${FLEET_URL:-http://localhost:8095}"

echo "== 1. Enumerate registered styles =="
curl -s "$BASE/api/retrieval/summary-styles" | python3 -m json.tool
echo

echo "== 2. Summarize a corpus of recent messages via the pipeline stage =="
curl -s -X POST "$BASE/api/retrieval/execute" \
  -H 'Content-Type: application/json' \
  -d '{
    "indexName": "mail",
    "lang": "en",
    "query": {
      "search":    { "query": "*:*", "offset": 0, "limit": 25, "lang": "en" },
      "fetch":     {},
      "fixup":     { "tags": ["basic"] },
      "summarize": { "style": "BRIEF", "maxWords": 200 }
    }
  }' | python3 -c "
import json, sys
r = json.load(sys.stdin)
for a in r.get('aggregates', []):
    if a.get('_aggregate') == 'ai_summary':
        print(f\"[style={a['style']} docs={a['documentCount']} ms={a['durationMs']}]\")
        print(a['summary'])
"
echo

echo "== 3. Summarize pre-assembled text (direct primitive) =="
curl -s -X POST "$BASE/api/retrieval/summarize" \
  -H 'Content-Type: application/json' \
  -d '{
    "text": "From: alice@x  Sent: 2026-08-01\nPlease review the RFC by Friday.\n\nFrom: bob@x  Sent: 2026-08-02\nAgreed, I will loop in legal.\n\nFrom: alice@x  Sent: 2026-08-03\nThanks. Are we still targeting Q4?",
    "style": "CONTRIBUTIONS"
  }' | python3 -m json.tool
echo

echo "== 4. Custom prompt, no style =="
curl -s -X POST "$BASE/api/retrieval/summarize" \
  -H 'Content-Type: application/json' \
  -d '{
    "text": "Q3 revenue was $2.4M, up 12% YoY. Q4 forecast $2.8M.",
    "prompt": "Extract every dollar amount and its context. Output as JSON with fields: amount, context."
  }' | python3 -m json.tool
