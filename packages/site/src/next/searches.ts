// Recorded searches for the Orbit Next demo. Every match is a real line of public gitlab-org code, read on
// Nov 15, 2026 from these commits: gitlab-org/gitlab 6a9dcd6, knowledge-graph main and the
// cursor-base-pagination-draft branch, site-kit main, and orbit-next main. Only the $X.unwrap() search has measured
// numbers, from an early test in the Orbit Next README, on a smaller index than today's. Re-record the lines if the files move.

export type Mode = "text" | "regex" | "structural";

export type Source = "code" | "jira" | "slack" | "incident.io" | "grafana";

export const SOURCES: { source: Source; label: string; icon: string }[] = [
  { source: "code", label: "Code", icon: "code" },
  { source: "jira", label: "Jira", icon: "work-item-issue" },
  { source: "slack", label: "Slack", icon: "comment" },
  { source: "incident.io", label: "incident.io", icon: "status-alert" },
  { source: "grafana", label: "Grafana", icon: "chart" },
];

// One record from a third-party tool, with the GitLab work it links to.
export type SourceRecord = { title: string; meta: string; text?: string; links: string[] };

export const MODES: { mode: Mode; blurb: string; placeholder: string }[] = [
  { mode: "text", blurb: "Exact text, in any file, on any branch.", placeholder: "Search for exact text" },
  { mode: "regex", blurb: "Regular expressions, in any language.", placeholder: "Search with a regular expression" },
  { mode: "structural", blurb: "Patterns over the syntax tree, in 20 languages.", placeholder: "Search with a pattern, such as $X.unwrap()" },
];

// The languages that structural search parses, from the Orbit Next search page.
export const LANGUAGES = ["bash", "c", "cpp", "css", "elixir", "go", "html", "java", "javascript", "kotlin", "lua", "php", "python", "ruby", "rust", "scala", "swift", "tsx", "typescript", "yaml"];

// One line of a file around a match. `match` is the part of a matching line to mark.
export type Line = { n: number; text: string; match?: string };

export type FileHit = { project: string; path: string; branch: string; lines: Line[] };

// The measured passes of one query: files in the index, candidates after the trigram prefilter, files
// scanned, matches, and milliseconds.
export type Funnel = { files: number; candidates: number; scanned: number; matches: number; ms: number };

export type Search = {
  id: string;
  source: Source;
  label: string;
  pattern: string;
  // A shorter form of a long pattern, for the example cards.
  short?: string;
  mode: Mode;
  language?: string;
  branches: "default" | "all";
  summary: string;
  funnel?: Funnel;
  files: FileHit[];
  records?: SourceRecord[];
};

export const SEARCHES: Search[] = [
  {
    id: "unwrap",
    source: "code",
    label: "Every unwrap() in Rust",
    pattern: "$X.unwrap()",
    mode: "structural",
    language: "rust",
    branches: "default",
    summary: "In our internal testing: 100 matches in 111 ms. The pattern matches method calls, not comments or strings.",
    funnel: { files: 1_489_091, candidates: 4_156, scanned: 256, matches: 100, ms: 111 },
    files: [
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "crates/config/src/lib.rs",
        branch: "main",
        lines: [
          { n: 298, text: "pub fn get() -> Arc<AppConfig> {" },
          { n: 299, text: "    CONFIG.read().unwrap().clone()", match: "CONFIG.read().unwrap()" },
          { n: 300, text: "}" },
        ],
      },
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/code-graph-incremental/src/linker.rs",
        branch: "main",
        lines: [
          { n: 533, text: "        if c.has(C::ImplBlock) {" },
          { n: 534, text: "            self.link_reference(c.child(C::DefName).unwrap());", match: "c.child(C::DefName).unwrap()" },
          { n: 535, text: "        }" },
        ],
      },
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "crates/cli/src/main.rs",
        branch: "main",
        lines: [
          { n: 371, text: "        let path = credentials_path()?;" },
          { n: 372, text: "        std::fs::create_dir_all(path.parent().unwrap())?;", match: "path.parent().unwrap()" },
          { n: 373, text: "        let credentials = Credentials { refresh_token: refresh_token.to_string() };" },
        ],
      },
    ],
  },
  {
    id: "flag",
    source: "code",
    label: "One feature flag, Ruby and Vue",
    pattern: "Feature\\.enabled\\?\\(:knowledge_graph\\b|feature_flag\\(:knowledge_graph\\b|glFeatures\\.knowledgeGraph\\b",
    short: "Feature\\.enabled\\?\\(:knowledge_graph|…",
    mode: "regex",
    branches: "default",
    summary: "The same flag, checked in a finder, a policy, the frontend flags, and a Vue component.",
    files: [
      {
        project: "gitlab-org/gitlab",
        path: "ee/app/policies/ee/group_policy.rb",
        branch: "master",
        lines: [
          { n: 946, text: "      condition(:knowledge_graph_enabled) do" },
          { n: 947, text: "        ::Feature.enabled?(:knowledge_graph, @user)", match: "Feature.enabled?(:knowledge_graph" },
          { n: 948, text: "      end" },
        ],
      },
      {
        project: "gitlab-org/gitlab",
        path: "ee/lib/ee/gitlab/gon_helper.rb",
        branch: "master",
        lines: [
          { n: 56, text: "        push_frontend_feature_flag(:orbit_user_preference, current_user, type: :beta)" },
          { n: 57, text: "        push_frontend_feature_flag(:knowledge_graph, current_user)", match: "feature_flag(:knowledge_graph" },
          { n: 58, text: "        push_frontend_feature_flag(:orbit_foundational_agent, current_user)" },
        ],
      },
      {
        project: "gitlab-org/gitlab",
        path: "ee/app/assets/javascripts/ai/duo_agentic_chat/components/orbit_toggle.vue",
        branch: "master",
        lines: [
          { n: 109, text: "        this.glFeatures.orbitUserPreference &&" },
          { n: 110, text: "        this.glFeatures.knowledgeGraph &&", match: "glFeatures.knowledgeGraph" },
          { n: 111, text: "        this.glFeatures.orbitFoundationalAgent" },
        ],
      },
    ],
  },
  {
    id: "react",
    source: "code",
    label: "React imports in TSX",
    pattern: 'import { $$$ } from "react"',
    mode: "structural",
    language: "tsx",
    branches: "default",
    summary: "One pattern finds every named import from React, whatever names it lists.",
    files: [
      {
        project: "gitlab-org/orbit/documentation/site-kit",
        path: "packages/site/src/components/AgentPrompt.tsx",
        branch: "main",
        lines: [
          { n: 1, text: 'import { type FormEvent, type ReactNode, useEffect, useRef, useState } from "react";', match: 'import { type FormEvent, type ReactNode, useEffect, useRef, useState } from "react"' },
          { n: 2, text: 'import { cn } from "./cn";' },
        ],
      },
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "ui/src/components/query-bar.tsx",
        branch: "main",
        lines: [
          { n: 2, text: 'import { Loader2, Search } from "lucide-react";' },
          { n: 3, text: 'import { useEffect, useRef, useState } from "react";', match: 'import { useEffect, useRef, useState } from "react"' },
        ],
      },
      {
        project: "gitlab-org/orbit/documentation/site-kit",
        path: "packages/markdown/src/media-viewer-host.tsx",
        branch: "main",
        lines: [
          { n: 1, text: 'import { useEffect, useState, type ReactNode } from "react";', match: 'import { useEffect, useState, type ReactNode } from "react"' },
          { n: 2, text: 'import { requestIdle } from "./enhancers/idle";' },
        ],
      },
    ],
  },
  {
    id: "branch",
    source: "code",
    label: "Code that is only on a branch",
    pattern: "CursorPaginator",
    mode: "text",
    branches: "all",
    summary: "No matches on main. 14 on one unmerged branch, where the work is still in progress.",
    files: [
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/indexer/src/modules/sdlc/cursor_paginator.rs",
        branch: "cursor-base-pagination-draft",
        lines: [
          { n: 40, text: "#[derive(Debug, Clone)]" },
          { n: 41, text: "pub struct CursorPaginator {", match: "CursorPaginator" },
          { n: 42, text: "    cursor_columns: Vec<String>," },
        ],
      },
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/indexer/src/modules/sdlc/pipeline.rs",
        branch: "cursor-base-pagination-draft",
        lines: [
          { n: 86, text: "        let mut paginator =" },
          { n: 87, text: "            CursorPaginator::new(self.cursor_columns.clone(), self.page_size);", match: "CursorPaginator" },
          { n: 88, text: "        if let Some(starting) = extract_starting_cursor(&params) {" },
        ],
      },
    ],
  },
  {
    id: "security-context",
    source: "code",
    label: "A type name, in Rust and JavaScript",
    pattern: "SecurityContext",
    mode: "text",
    branches: "default",
    summary: "The same name in a Rust server and a JavaScript adapter, in two projects.",
    files: [
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "crates/server/src/auth.rs",
        branch: "main",
        lines: [
          { n: 105, text: "// An empty prefix set fails closed: the query gets no access." },
          { n: 106, text: "pub struct SecurityContext {", match: "SecurityContext" },
          { n: 107, text: "    pub prefixes: Vec<TraversalPath>," },
        ],
      },
      {
        project: "gitlab-org/gitlab",
        path: "ee/app/assets/javascripts/vulnerabilities/components/vulnerability_details_enrichment/adapters/from_haml.js",
        branch: "master",
        lines: [
          { n: 141, text: "" },
          { n: 142, text: "const toAscpSecurityContext = (securityContext) =>", match: "SecurityContext" },
          { n: 143, text: "  securityContext" },
        ],
      },
    ],
  },
  {
    id: "test-fns",
    source: "code",
    label: "Test functions by name",
    pattern: "fn\\s+\\w+_test",
    mode: "regex",
    branches: "default",
    summary: "Functions whose names end in _test, found by their shape.",
    files: [
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/integration-tests-codegraph/src/validator.rs",
        branch: "main",
        lines: [
          { n: 79, text: "" },
          { n: 80, text: "fn run_test(test: &TestCase, client: &DuckDbClient, ontology: &Arc<Ontology>) -> Vec<Failure> {", match: "fn run_test" },
          { n: 81, text: "    if test.debug {" },
        ],
      },
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/code-graph/src/v2/langs/generic/zig.rs",
        branch: "main",
        lines: [
          { n: 121, text: "// can be interacted with normally." },
          { n: 122, text: "fn zig_anonymous_test(", match: "fn zig_anonymous_test" },
          { n: 123, text: "    node: &N<'_>," },
        ],
      },
    ],
  },
  {
    id: "result-fns",
    source: "code",
    label: "Functions that return a Result",
    pattern: "fn $NAME($$$) -> Result<$T, $E>",
    mode: "structural",
    language: "rust",
    branches: "default",
    summary: "One pattern matches every signature that returns a Result, whatever its arguments.",
    files: [
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "crates/server/src/auth.rs",
        branch: "main",
        lines: [
          { n: 110, text: "impl SecurityContext {" },
          { n: 111, text: "    pub fn from_claims(claims: &Claims) -> Result<Self, String> {", match: "fn from_claims(claims: &Claims) -> Result<Self, String>" },
          { n: 112, text: "        let mut paths = Vec::new();" },
        ],
      },
      {
        project: "gitlab-org/orbit/experiments/orbit-next",
        path: "crates/server/src/error.rs",
        branch: "main",
        lines: [
          { n: 18, text: "" },
          { n: 19, text: "    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {", match: "fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection>" },
          { n: 20, text: "        let Json(value) = Json::from_request(req, state).await.map_err(bad_request)?;" },
        ],
      },
    ],
  },
];

// Third-party searches. These sources have not shipped, so the records are sample data from the team's
// third-party demo world: a pricing cache, epic ORB-1, and incident INC-77, on made-up projects.
export const SAMPLE_SEARCHES: Search[] = [
  {
    id: "jira-cache",
    source: "jira",
    label: "Issues about the pricing cache",
    pattern: "pricing cache",
    mode: "text",
    branches: "default",
    summary: "3 issues, each linked to the merge request that builds it.",
    files: [],
    records: [
      { title: "ORB-12 Add a cache to the pricing API", meta: "Story · Done", links: ["checkout-api!42", "checkout-api!43"] },
      { title: "ORB-20 Document the pricing cache", meta: "Task · Done", links: ["checkout-api!45"] },
      { title: "ORB-1 Checkout latency below 500 ms", meta: "Epic · In progress", links: [] },
    ],
  },
  {
    id: "slack-inc",
    source: "slack",
    label: "Messages that cite an incident",
    pattern: "INC-77",
    mode: "text",
    branches: "default",
    summary: "3 messages in #checkout-incidents, from the alert to the fix.",
    files: [],
    records: [
      { title: "#checkout-incidents", meta: "@ada", text: "INC-77 declared: checkout p95 is at 2 s since checkout-api!42 rolled out.", links: ["checkout-api!42"] },
      { title: "#checkout-incidents", meta: "@bob", text: "Rolling back !42 now, see ORB-12.", links: ["checkout-api!42"] },
      { title: "#checkout-incidents", meta: "@bob", text: "Fix is up: !43 (cache stampede).", links: ["checkout-api!43"] },
    ],
  },
  {
    id: "incident-checkout",
    source: "incident.io",
    label: "Incidents on checkout",
    pattern: "checkout",
    mode: "text",
    branches: "default",
    summary: "1 incident, with the updates that name a merge request.",
    files: [],
    records: [
      { title: "INC-77 Checkout p95 latency spike", meta: "Major · Closed", text: "Checkout p95 rose to 2 s after the pricing cache rollout.", links: ["checkout-api!42"] },
      { title: "Update", meta: "@bob", text: "Rolled back !42. Latency is recovering.", links: ["checkout-api!42"] },
      { title: "Update", meta: "@ada", text: "Fix !43 deployed. p95 is back to 420 ms.", links: ["checkout-api!43"] },
    ],
  },
  {
    id: "grafana-deploys",
    source: "grafana",
    label: "Deploys and alerts on a dashboard",
    pattern: "dashboard:checkout",
    mode: "text",
    branches: "default",
    summary: "4 annotations: two deploys, and the alert that fired and cleared between them.",
    files: [],
    records: [
      { title: "Deploy api v1.4.2", meta: "deploy", links: ["checkout-api!42"] },
      { title: "Checkout p95 above 800 ms", meta: "alert · Alerting", links: [] },
      { title: "Deploy api v1.4.3", meta: "deploy", links: ["checkout-api!43"] },
      { title: "Checkout p95 above 800 ms", meta: "alert · Normal", links: [] },
    ],
  },
];

// The size of the Orbit Next prototype's index of open source code, from its search page on Nov 15, 2026.
export const CORPUS = [
  { value: "51,656", label: "projects" },
  { value: "1.38 M", label: "branches" },
  { value: "44.2 M", label: "commits" },
  { value: "45.7 M", label: "files at HEAD" },
  { value: "22.6 M", label: "unique files" },
  { value: "767 GiB", label: "index" },
];

// Warm query times from the Orbit Next repository: text search from its 3,351-project benchmark
// (docs/clickhouse/code-search-benchmark.md), structural search from the early test in its README.
export const LATENCY = [
  { label: "Text search, default branches", ms: 80, note: "benchmark, 3,351 projects" },
  { label: "Structural search", ms: 111, note: "early test" },
  { label: "Text search, every branch and commit", ms: 120, note: "benchmark, 3,351 projects" },
];
