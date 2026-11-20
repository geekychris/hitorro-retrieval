package com.hitorro.retrieval;
import tools.jackson.databind.*; import tools.jackson.databind.node.*;
import com.hitorro.jsontypesystem.JVS;
import com.hitorro.index.IndexManager; import com.hitorro.index.config.IndexConfig; import com.hitorro.index.search.SearchResult;
import com.hitorro.retrieval.*; import com.hitorro.retrieval.cluster.*; import com.hitorro.retrieval.docstore.*; import com.hitorro.retrieval.merger.*; import com.hitorro.retrieval.search.*;
import com.hitorro.kvstore.Result;
import com.sun.net.httpserver.HttpServer;
import java.io.*; import java.net.*; import java.nio.charset.StandardCharsets; import java.nio.file.*; import java.util.*;
/**
 * Pins what hitorro-retrieval reads and sends to what 3.0.2 did on Jackson 2.18: YAML cluster configs
 * (incl. every plain-scalar form, read with YAML 1.1 typing as Jackson 2 did), sort criteria parsing,
 * the requests to and responses from remote search / streaming search / document nodes, and whole
 * retrievals over remote nodes and over a local in-memory index. {@code jackson2-retrieval.txt} is the
 * output of the same code run on the 3.0.2 sources (generator: ~/hitorro-j3/tools/Jackson2RetrievalGolden.java.txt
 * in the migration workspace). StringNode is mapped back to TextNode before comparing.
 */
class Jackson2FormatGoldenTest {
  static final Map<String, String> out = new LinkedHashMap<>();
  static void p(String k, Object v){ out.put(k, String.valueOf(v).replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r").replaceAll("\"searchTimeMs\":[0-9]+", "\"searchTimeMs\":T")); }
  static boolean jackson(Throwable t){ return t.getClass().getName().startsWith("com.fasterxml.jackson") || t.getClass().getName().startsWith("tools.jackson"); }
  static String desc(Throwable t){ String s = (t instanceof IOException ? "IOE:" : "RTE:") + (jackson(t) ? "jackson" : t.getClass().getSimpleName());
    if (!jackson(t) && t.getCause() != null) s += "<" + (jackson(t.getCause()) ? "jackson" : t.getCause().getClass().getSimpleName()) + ">"; return s; }
  static String tryIt(java.util.concurrent.Callable<Object> c){ try { return String.valueOf(c.call()); } catch(Throwable t){ if (System.getenv("RG_TRACE") != null) t.printStackTrace(); return "!"+desc(t); } }
  static String sig(JsonNode n){ if (n == null) return "null"; if (n.isObject()) { StringBuilder sb = new StringBuilder("{"); Iterator<String> it = n.propertyNames().iterator(); while (it.hasNext()) { String k = it.next(); sb.append(k).append(':').append(sig(n.get(k))).append(it.hasNext() ? "," : ""); } return sb.append('}').toString(); }
    if (n.isArray()) { StringBuilder sb = new StringBuilder("["); for (int i = 0; i < n.size(); i++) sb.append(i > 0 ? "," : "").append(sig(n.get(i))); return sb.append(']').toString(); }
    return n.getClass().getSimpleName(); }
  // JsonNode.toString() fails past 500 levels on Jackson 3 (2.x: 1000), so print trees with the mapper
  static final ObjectMapper W = com.hitorro.util.json.JsonMappers.json();
  static String str(JsonNode n){ if (n == null) return "null"; try { return W.writeValueAsString(n); } catch (Exception e) { throw new RuntimeException(e); } }
  static String node(JsonNode n){ return str(n) + " | " + sig(n); }
  static String jvs(JVS j){ return j == null ? "null" : node(j.getJsonNode()); }
  static String jvsList(Iterable<JVS> l){ List<String> s = new ArrayList<>(); for (JVS j : l) s.add(jvs(j)); return s.toString(); }
  static String sr(SearchResult r){ return "hits=" + r.getTotalHits() + " time=" + r.getSearchTimeMs() + " off=" + r.getOffset() + " lim=" + r.getLimit() + " q=" + r.getQuery() + " docs=" + jvsList(r.getDocuments()); }
  static String cc(ClusterConfig c){ return c.getClusterName() + " " + c.getNodes(); }
  static String deepYaml(int d){ StringBuilder sb = new StringBuilder("cluster:\n  name: deep\n  extra: "); for (int i = 0; i < d; i++) sb.append("{a: "); sb.append(1); for (int i = 0; i < d; i++) sb.append('}'); return sb.append("\n  nodes:\n    - {name: n, host: h, port: 1}\n").toString(); }
  static String deepJson(int d){ StringBuilder sb = new StringBuilder(); for (int i = 0; i < d; i++) sb.append("{\"a\":"); sb.append(1); for (int i = 0; i < d; i++) sb.append('}'); return sb.toString(); }
  static String result(Result<JsonNode> r){ return r.isSuccess() ? "ok " + node(r.getValue().get()) : "fail" + (r.getError().map(e -> e.startsWith("Document not found") || e.startsWith("HTTP ") ? " " + e : " <msg>").orElse(" none")); }

  static final String[] YAMLS = {
    "cluster:\n  name: hitorro-cluster\n  nodes:\n    - name: index-1\n      host: index1.hitorro.local\n      port: 8080\n      roles: [INDEX]\n    - name: kv-1\n      host: kv1\n      port: \"8081\"\n      roles: [kvstore, COORDINATOR, bogus]\n",
    "name: top-level\nnodes:\n  - {host: h1}\n  - {name: 7, host: 8, port: 8.9, roles: INDEX}\n  - {name: n3, port: true, roles: [7, null, ~]}\n  - {name: n4, port: abc}\n  - {name: n5, port: 12345678901, roles: []}\n  - {name: n6, port: null}\n",
    "cluster:\n  nodes: {name: notalist}\n",
    "cluster: 5\n",
    "",
    "# only a comment\n",
    "cluster:\n  name: [a, b]\n  nodes:\n    - name: {x: 1}\n      host: [h]\n",
    "cluster:\n  name: x\n  nodes:\n   - name: a\n  bad indent: [\n",
    "cluster:\n  name: dup\n  name: dup2\n  nodes: []\n",
    "---\ncluster:\n  name: first\n---\ncluster:\n  name: second\n",
    "cluster:\n  name: \"\u00e9\\u2028 \uD83D\uDE00\"\n  nodes:\n    - name: 'q''uote'\n      host: \"0x1F\"\n      port: 0x1F\n",
    "cluster:\n  name: anchors\n  nodes:\n    - &n {name: a, host: h, port: 1, roles: [INDEX]}\n    - *n\n",
    "cluster:\n  name: big\n  nodes:\n    - {name: b, port: 1.5e3}\n    - {name: c, port: -0}\n    - {name: d, port: 2147483648}\n",
    deepYaml(600),
    deepYaml(1100),
  };
  static final String[] SEARCH_BODIES = {
    "{\"documents\":[{\"id\":\"a\",\"_score\":1.5,\"title\":\"A\",\"n\":1.10},{\"id\":\"b\",\"_score\":0.5,\"big\":12345678901234567890}],\"totalHits\":42,\"searchTimeMs\":7}",
    "{\"documents\":[],\"totalHits\":\"9\",\"searchTimeMs\":\"x\"}",
    "{\"documents\":{\"id\":\"notarray\"},\"totalHits\":3.9,\"searchTimeMs\":true}",
    "{\"totalHits\":null,\"searchTimeMs\":{}}",
    "{\"documents\":[1,\"s\",null,[2]],\"totalHits\":12345678901234567890}",
    "[]",
    "\"str\"",
    "",
    "{\"documents\":[{\"id\":\"x\"}]} trailing",
    "{\"documents\":[{\"id\":\"x\",}]}",
    "{\"documents\":[{\"id\":\"x\",\"d\":" + deepJson(600) + "}],\"totalHits\":1}",
    "{\"documents\":[{\"id\":\"x\",\"d\":" + deepJson(1100) + "}],\"totalHits\":1}",
  };
  static final String[] STREAM_BODIES = {
    "{\"_type\":\"meta\",\"totalHits\":3,\"searchTimeMs\":4}\n{\"id\":\"a\",\"_score\":2.0}\n{\"_type\":\"doc\",\"id\":\"b\",\"_score\":1.0}\n{\"_type\":\"facets\",\"facets\":{\"dept\":{\"R\":2}}}\n{\"_type\":\"aggregate\",\"x\":1}\n",
    "{\"totalHits\":\"5\",\"searchTimeMs\":1.9}\n{\"id\":\"a\"}\n\n{\"id\":\"after-blank\"}\n",
    "{\"totalHits\":2}\n{\"id\":\"a\"}\nnot json\n{\"id\":\"c\"}\n",
    "{\"totalHits\":2}\n{\"id\":\"a\",\"_type\":5}\n{\"id\":\"b\",\"_type\":null}\n[1,2]\n\"s\"\n",
    "not json meta\n{\"id\":\"a\"}\n",
    "",
    "{\"totalHits\":1}\n{\"id\":\"a\"} {\"id\":\"b\"}\n{\"id\":\"c\"}\n",
    "{\"totalHits\":1}\n{\"id\":\"deep\",\"d\":" + deepJson(600) + "}\n{\"id\":\"toodeep\",\"d\":" + deepJson(1100) + "}\n{\"id\":\"z\"}\n",
  };
  static final String[] DOC_BODIES = {
    "{\"id\":{\"domain\":\"demo\",\"did\":\"doc-1\"},\"title\":\"full\",\"n\":1.10,\"big\":12345678901234567890,\"s\":\"\u00e9\"}",
    "[1,2]", "\"x\"", "12", "null", "", "{\"a\":1} trailing", "{bad", "{\"d\":" + deepJson(600) + "}", "{\"d\":" + deepJson(1100) + "}",
  };
  static final String[] SORTS = {
    "null", "\"date:desc,_score:desc\"", "\"title:asc\"", "\"_score\"", "\"\"", "\"a:sideways\"", "42", "true", "{\"foo\":1}",
    "[{\"field\":\"date\",\"direction\":\"asc\"},{\"field\":\"_score\"}]", "[{\"field\":7,\"direction\":5},{\"direction\":\"asc\"},{\"field\":null},\"str\",{\"field\":\"x\",\"direction\":\"ASC\"}]", "[]",
  };

  public static Map<String, String> compute(Path dir) throws Exception {
    out.clear();
    ObjectMapper om = com.hitorro.util.json.JsonMappers.json();
    // ---- cluster config (YAML)
    for (int i = 0; i < YAMLS.length; i++) { String y = YAMLS[i]; final int yi = i;
      p("yaml.stream." + i, tryIt(() -> { YamlServiceRegistry r = new YamlServiceRegistry(new ByteArrayInputStream(y.getBytes(StandardCharsets.UTF_8))); return cc(r.getClusterConfig()) + " avail=" + r.isAvailable(); }));
      p("yaml.file." + i, tryIt(() -> { Path f = dir.resolve("c" + yi + ".yaml"); Files.writeString(f, y); YamlServiceRegistry r = new YamlServiceRegistry(f);
        String first = cc(r.getClusterConfig()); Files.writeString(f, YAMLS[0]); r.refresh(); String second = cc(r.getClusterConfig()); Files.writeString(f, "cluster: [\n"); r.refresh(); return first + " -> " + second + " -> " + cc(r.getClusterConfig()); })); }
    // every plain scalar form as name, host, port and role (YAML 1.1 typing, as Jackson 2 read it)
    String[] scal = {"0x1F","0X1f","-0x1F","0o17","017","-017","08","0b101","1_000","0x_1F","+5","+8080","-0","+0.5",".5","1e3","1.5e3","1E3","1.0e+3","685.230_15e+03","190:20:30","1:20","0.","-.inf",".Inf",".NaN","~","null","Null","NULL","","yes","no","on","off","y","n","Y","True","TRUE","false","12345678901234567890","2147483648","1.10","2001-12-14","'0x1F'","\"yes\"","!!int 42","!!str 42","!!float 1","!!bool yes","!!null x","index","INDEX","Index"};
    for (String v : scal) p("yaml.scalar." + v, tryIt(() -> { String y = "cluster:\n  name: " + v + "\n  nodes:\n    - name: " + v + "\n      host: " + v + "\n      port: " + v + "\n      roles: [" + v + "]\n";
      return cc(new YamlServiceRegistry(new ByteArrayInputStream(y.getBytes(StandardCharsets.UTF_8))).getClusterConfig()); }));
    p("yaml.file.missing", tryIt(() -> cc(new YamlServiceRegistry(dir.resolve("nope.yaml")).getClusterConfig())));
    // ---- sort criteria
    for (int i = 0; i < SORTS.length; i++) { String s = SORTS[i]; p("sort." + i, tryIt(() -> { SortCriteria c = SortCriteriaParser.parse(om.readTree(s)); return String.valueOf(c); })); }
    p("sort.javanull", tryIt(() -> SortCriteriaParser.parse(null)));
    p("sort.missing", tryIt(() -> SortCriteriaParser.parse(om.readTree("{}").path("x"))));
    // ---- remote nodes: search, streaming search, documents
    List<String> reqs = new ArrayList<>(); String[] body = {""}; int[] status = {200};
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    srv.createContext("/", ex -> { synchronized (reqs) { reqs.add(ex.getRequestMethod() + " " + ex.getRequestURI() + " accept=" + ex.getRequestHeaders().getFirst("Accept")); }
      byte[] rb = body[0].getBytes(StandardCharsets.UTF_8); if (ex.getRequestMethod().equals("HEAD")) { ex.sendResponseHeaders(status[0], -1); ex.close(); return; }
      ex.sendResponseHeaders(status[0], rb.length == 0 ? -1 : rb.length); if (rb.length > 0) ex.getResponseBody().write(rb); ex.close(); });
    srv.start();
    try {
      NodeAddress node = new NodeAddress("n1", "127.0.0.1", srv.getAddress().getPort(), EnumSet.of(NodeRole.INDEX));
      RemoteSearchProvider rsp = new RemoteSearchProvider(node); StreamingRemoteSearchProvider ssp = new StreamingRemoteSearchProvider(node); RemoteDocumentStore rds = new RemoteDocumentStore(node);
      for (int i = 0; i < SEARCH_BODIES.length; i++) { String b = SEARCH_BODIES[i];
        p("remote.search." + i, tryIt(() -> { reqs.clear(); body[0] = b; return sr(rsp.search("idx", "title:a b", 2, 5, List.of("dept", "x"), "en", SortCriteria.byField("date", SortCriteria.Direction.ASC).then("_score", SortCriteria.Direction.DESC))) + " " + reqs; }));
        p("remote.streamsearch." + i, tryIt(() -> { body[0] = b; return sr(ssp.search("idx", "q", 0, 10, null, "en")); })); }
      p("remote.search.http500", tryIt(() -> { status[0] = 500; body[0] = "{}"; try { return sr(rsp.search("idx", "q", 0, 10, null, "en")); } finally { status[0] = 200; } }));
      p("remote.available", tryIt(() -> rsp.isAvailable() + "," + ssp.isAvailable() + "," + rds.isAvailable()));
      for (int i = 0; i < STREAM_BODIES.length; i++) { String b = STREAM_BODIES[i];
        p("remote.stream." + i, tryIt(() -> { reqs.clear(); body[0] = b; StreamingRemoteSearchProvider.StreamingResult r = ssp.searchStreaming("idx", "q", 0, 10, List.of("dept"), "de", SortCriteria.byField("t", SortCriteria.Direction.DESC));
          List<JVS> docs = new ArrayList<>(); r.documents().forEachRemaining(docs::add); return "hits=" + r.totalHits() + " time=" + r.searchTimeMs() + " docs=" + jvsList(docs) + " tail=" + jvsList(r.tailObjects().get()) + " " + reqs; })); }
      for (int i = 0; i < DOC_BODIES.length; i++) { String b = DOC_BODIES[i]; p("remote.doc." + i, tryIt(() -> { reqs.clear(); body[0] = b; return result(rds.get("demo/doc 1")) + " " + reqs; })); }
      for (int st : new int[]{404, 500}) p("remote.doc.http" + st, tryIt(() -> { status[0] = st; body[0] = ""; try { return result(rds.get("k")); } finally { status[0] = 200; } }));
      // ---- a whole retrieval over remote nodes: composite search, then documents fetched from the remote store
      String[] queries = {
        "{\"search\":{\"query\":\"*:*\",\"offset\":0,\"limit\":10,\"facets\":[\"dept\"],\"sort\":\"_score:desc\"},\"fetch\":{\"enabled\":\"true\"}}",
        "{\"search\":{\"query\":\"a\",\"limit\":\"3\",\"lang\":\"de\",\"sort\":[{\"field\":\"n\",\"direction\":\"asc\"}]},\"page\":{\"page\":1,\"rows\":1}}",
        "{\"search\":{\"query\":\"b\",\"offset\":1.7,\"limit\":true},\"fixup\":{\"tags\":[\"x\"]}}",
        "{\"search\":{}}",
      };
      String[] docBody = {"{\"id\":{\"domain\":\"demo\",\"did\":\"doc-1\"},\"title\":\"full\",\"dept\":\"R\"}"};
      HttpServer srv2 = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      srv2.createContext("/", ex -> { String path = ex.getRequestURI().getPath(); String rb;
        if (path.startsWith("/api/search/stream")) rb = "{\"totalHits\":2,\"searchTimeMs\":1}\n{\"id\":{\"domain\":\"demo\",\"did\":\"doc-1\"},\"_score\":2.5,\"n\":2}\n{\"id\":{\"domain\":\"demo\",\"did\":\"doc-3\"},\"_score\":0.1,\"n\":1}\n{\"_type\":\"facets\",\"dept\":{\"R\":1}}\n";
        else if (path.startsWith("/api/search")) rb = "{\"documents\":[{\"id\":{\"domain\":\"demo\",\"did\":\"doc-2\"},\"_score\":1.5,\"n\":3},{\"id\":\"plain\",\"_score\":1.5,\"n\":\"4\"}],\"totalHits\":5,\"searchTimeMs\":3}";
        else if (path.startsWith("/api/documents/")) rb = path.contains("doc-3") ? "" : docBody[0].replace("doc-1", path.substring(path.lastIndexOf('/') + 1).replace("demo%2F", ""));
        else rb = "{}";
        int code = path.startsWith("/api/documents/") && path.contains("doc-3") ? 404 : 200; byte[] b = rb.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length == 0 ? -1 : b.length); if (b.length > 0) ex.getResponseBody().write(b); ex.close(); });
      srv2.start();
      try {
        NodeAddress n2 = new NodeAddress("n2", "127.0.0.1", srv2.getAddress().getPort(), EnumSet.of(NodeRole.INDEX, NodeRole.KVSTORE));
        SearchProvider composite = new CompositeSearchProvider(List.of(new RemoteSearchProvider(n2), new StreamingRemoteSearchProvider(n2)));
        RetrievalService svc = new RetrievalService(composite, new RemoteDocumentStore(n2));
        for (int i = 0; i < queries.length; i++) { String q = queries[i]; p("retrieve.remote." + i, tryIt(() -> { RetrievalResult r = svc.retrieve(new RetrievalConfig("idx"), new JVS(om.readTree(q)));
          List<String> docs = new ArrayList<>(); for (JVS d : r.getDocumentList()) docs.add(jvs(d)); return "docs=" + docs + " aggs=" + jvsList(r.getAggregates()) + " errors=" + r.getErrors(); })); }
      } finally { srv2.stop(0); }
    } finally { srv.stop(0); }
    // ---- a whole retrieval over a local in-memory index
    IndexManager im = new IndexManager("en");
    im.createIndex("test", IndexConfig.inMemory().build(), null);
    String[] idocs = {
      "{\"id\":{\"domain\":\"demo\",\"did\":\"doc-1\"},\"title\":\"Document one about climate\",\"department\":\"Research\",\"n\":1.10,\"date\":\"2020-01-01\"}",
      "{\"id\":{\"domain\":\"demo\",\"did\":\"doc-2\"},\"title\":\"Document two about climate\",\"department\":\"Research\",\"n\":2,\"tags\":[\"a\",\"b\"]}",
      "{\"id\":{\"domain\":\"demo\",\"did\":\"doc-3\"},\"title\":\"Document three about technology\",\"department\":\"Engineering\",\"n\":12345678901234567890,\"meta\":{\"k\":null}}",
      "{\"id\":{\"domain\":\"demo\",\"did\":\"doc-4\"},\"title\":\"\u00e9 technology \uD83D\uDE00\",\"department\":\"Engineering\",\"flag\":true}",
    };
    for (String d : idocs) im.indexDocument("test", new JVS(om.readTree(d)));
    im.commit("test");
    String[] lq = {
      "{\"search\":{\"query\":\"*:*\",\"offset\":0,\"limit\":10}}",
      "{\"search\":{\"query\":\"title:climate\",\"limit\":20,\"facets\":[\"department\"]}}",
      "{\"search\":{\"query\":\"*:*\",\"sort\":\"n:asc\",\"limit\":\"2\",\"offset\":\"1\"}}",
      "{\"search\":{\"query\":\"*:*\",\"sort\":[{\"field\":\"department\",\"direction\":\"desc\"},{\"field\":\"_score\"}]},\"page\":{\"page\":2,\"rows\":1}}",
      "{\"search\":{\"query\":\"technology\",\"facets\":\"department\",\"lang\":\"en\"}}",
      "{\"search\":{\"query\":\"title:(\"}}",
      "{\"search\":{\"query\":5}}",
    };
    RetrievalService local = new RetrievalService(im);
    for (int i = 0; i < lq.length; i++) { String q = lq[i]; p("retrieve.local." + i, tryIt(() -> { RetrievalResult r = local.retrieve(new RetrievalConfig("test"), new JVS(om.readTree(q)));
      return "docs=" + jvsList(r.getDocumentList()) + " aggs=" + jvsList(r.getAggregates()) + " errors=" + r.getErrors(); })); }
    im.close();
    return out;
  }
  static Map<String, String> load() throws IOException {
    Map<String, String> m = new LinkedHashMap<>();
    try (InputStream in = Jackson2FormatGoldenTest.class.getResourceAsStream("jackson2-retrieval.txt")) {
      for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        if (line.isEmpty()) continue;
        int i = line.indexOf('=');
        m.put(line.substring(0, i), line.substring(i + 1));
      }
    }
    return m;
  }

  @org.junit.jupiter.api.Test
  void matchesJackson2Output(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Map<String, String> expected = load();
    Map<String, String> actual = new LinkedHashMap<>();
    // The one deliberate difference: a malformed remote response was the Jackson exception itself
    // (a JsonProcessingException, so an IOException); it is now an IOException with the Jackson
    // exception as its cause (Jackson 3 exceptions are unchecked).
    compute(dir).forEach((k, v) -> actual.put(k, v.replace("StringNode", "TextNode").replace("!IOE:IOException<jackson>", "!IOE:jackson")));
    org.assertj.core.api.Assertions.assertThat(expected).hasSize(157);
    org.assertj.core.api.Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
  }
}
