
export type Mode = "text" | "regex" | "structural";

export const CODE_MS = 900;
export const STEP_MS = 800;

export type CrossStep = { tool: string; icon: string; finding: string; name: string; links?: string[]; tone?: "risk" | "ok" };

export const MODES: { mode: Mode; blurb: string; placeholder: string }[] = [
  { mode: "text", blurb: "Exact text, in any file, on any branch.", placeholder: "Search for exact text" },
  { mode: "regex", blurb: "Regular expressions, in any language.", placeholder: "Search with a regular expression" },
  { mode: "structural", blurb: "Patterns over the syntax tree, in 20 languages.", placeholder: "Search with a pattern, such as $X.unwrap()" },
];

export const LANGUAGES = ["bash", "c", "cpp", "css", "elixir", "go", "html", "java", "javascript", "kotlin", "lua", "php", "python", "ruby", "rust", "scala", "swift", "tsx", "typescript", "yaml"];

export type Line = { n: number; text: string; match?: string };

export type FileHit = { project: string; path: string; branch: string; lines: Line[] };

export type Funnel = { files: number; candidates: number; scanned: number; matches: number; ms: number };

export type Search = {
  id: string;
  kind: "code" | "cross";
  label: string;
  ask?: string;
  pattern: string;
  short?: string;
  mode: Mode;
  language?: string;
  branches: "default" | "all";
  summary: string;
  funnel?: Funnel;
  files: FileHit[];
  more?: string[];
  steps?: CrossStep[];
  points?: [number, number][];
  logo?: boolean;
};

export const SEARCHES: Search[] = [
  {
    id: "error",
    kind: "code",
    label: "Find where an error comes from",
    ask: "Where does “There was an error fetching the deploy freezes.” come from?",
    pattern: "There was an error fetching the deploy freezes.",
    mode: "text",
    branches: "default",
    summary: "It comes from deploy_freeze/store/actions.js:80, when loading deploy freezes fails. One test checks it, and the locale files translate it.",
    more: ["gitlab.pot:82946", "de/gitlab.po:81245"],
    files: [
      {
        project: "gitlab-org/gitlab",
        path: "app/assets/javascripts/deploy_freeze/store/actions.js",
        branch: "master",
        lines: [
          { n: 79, text: "      createAlert({" },
          { n: 80, text: "        message: __('There was an error fetching the deploy freezes.'),", match: "There was an error fetching the deploy freezes." },
          { n: 81, text: "      });" },
        ],
      },
      {
        project: "gitlab-org/gitlab",
        path: "spec/frontend/deploy_freeze/store/actions_spec.js",
        branch: "master",
        lines: [
          { n: 199, text: "      expect(createAlert).toHaveBeenCalledWith({" },
          { n: 200, text: "        message: 'There was an error fetching the deploy freezes.',", match: "There was an error fetching the deploy freezes." },
          { n: 201, text: "      });" },
        ],
      },
    ],
  },
  {
    id: "unwrap",
    kind: "code",
    label: "unwrap() in Rust, on all branches",
    pattern: "$X.unwrap()",
    mode: "structural",
    language: "rust",
    branches: "all",
    summary: "Matches on main and on an unmerged branch. Method calls only, not comments or strings.",
    more: ["cursor_paginator.rs:369", "lib.rs:304", "arrow_client.rs:913", "rules.rs:601"],
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
      {
        project: "gitlab-org/orbit/knowledge-graph",
        path: "crates/indexer/src/modules/sdlc/cursor_paginator.rs",
        branch: "cursor-base-pagination-draft",
        lines: [
          { n: 320, text: "" },
          { n: 321, text: "        let values = paginator.advance(&batch).unwrap();", match: "paginator.advance(&batch).unwrap()" },
          { n: 322, text: "        assert_eq!(values.len(), 2);" },
        ],
      },
    ],
  },
  {
    id: "flag",
    kind: "code",
    label: "A feature flag in Ruby and Vue",
    pattern: "Feature\\.enabled\\?\\(:knowledge_graph\\b|feature_flag\\(:knowledge_graph\\b|glFeatures\\.knowledgeGraph\\b",
    short: "Feature\\.enabled\\?\\(:knowledge_graph|…",
    mode: "regex",
    branches: "default",
    summary: "The same flag, checked in a finder, a policy, the frontend flags, and a Vue component.",
    more: ["groups_finder.rb:32", "groups_finder.rb:39", "group_type.rb:692", "orbit_helper.rb:70", "knowledge_graph.rb:17"],
    files: [
      {
        project: "gitlab-org/gitlab",
        path: "ee/app/policies/ee/group_policy.rb",
        branch: "master",
        lines: [
          { n: 946, text: "      condition(:knowledge_graph_enabled) do" },
          { n: 947, text: "        ::Feature.enabled?(:knowledge_graph, @user)", match: "Feature.enabled?(:knowledge_graph" },
          { n: 948, text: "      end" },
          { n: 949, text: "" },
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
    kind: "code",
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
    kind: "code",
    label: "Branches touching CursorPaginator",
    pattern: "CursorPaginator",
    mode: "text",
    branches: "all",
    summary: "No matches on main. 14 on one unmerged branch, where the work is still in progress.",
    more: ["cursor_paginator.rs:47", "cursor_paginator.rs:266", "pipeline.rs:15", "pipeline.rs:287", "pipeline.rs:435"],
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
    kind: "code",
    label: "A type name, in Rust and JavaScript",
    pattern: "SecurityContext",
    mode: "text",
    branches: "default",
    summary: "The same name in a Rust server and a JavaScript adapter, in two projects.",
    more: ["auth.rs:97", "auth.rs:110", "auth.rs:154", "from_haml.js:145", "risk.vue:13", "risk.vue:48"],
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
    kind: "code",
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
    kind: "code",
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

const cross = (id: string, label: string, pattern: string, summary: string, steps: CrossStep[], logo = false): Search => ({ id, kind: "cross", label, pattern, mode: "text", branches: "default", summary, files: [], steps, logo });

export const CROSS_SEARCHES: Search[] = [
  {
    ...cross("incident", "Resolve an incident", "What caused INC-77, and which change fixed it?", "checkout-api!42 caused it. !43 fixed the cache stampede.", [
      { tool: "incident.io", icon: "status-alert", finding: "INC-77: checkout p95 is at 2 s.", name: "INC-77", tone: "risk" },
      { tool: "Grafana", icon: "chart", finding: "The p95 alert fired 1 hour after the v1.4.2 deploy.", name: "p95 alert" },
      { tool: "GitLab", icon: "tanuki", finding: "v1.4.2 shipped the pricing cache.", name: "!42 deployed", links: ["checkout-api!42"] },
      { tool: "Slack", icon: "comment", finding: "The team rolled back !42 and named a cache stampede.", name: "rollback thread" },
      { tool: "Code search", icon: "code", finding: "cache.get in pricing.rb:57 has no timeout.", name: "pricing.rb:57" },
      { tool: "GitLab", icon: "tanuki", finding: "!43 fixed the stampede. p95 is back to 420 ms.", name: "!43 deployed", links: ["checkout-api!43"], tone: "ok" },
    ]),
    points: [
      [0.1, 0.6],
      [0.27, 0.24],
      [0.46, 0.56],
      [0.64, 0.84],
      [0.7, 0.22],
      [0.92, 0.56],
    ],
  },
  cross("alert", "What changed before an alert", "What deployed right before the checkout alert?", "The v1.4.2 deploy, one hour before. It added a cache read with no timeout.", [
    { tool: "Grafana", icon: "chart", finding: "Checkout p95 went above 800 ms at 15:04.", name: "p95 alert" },
    { tool: "GitLab", icon: "tanuki", finding: "v1.4.2 deployed to production at 14:02.", name: "v1.4.2 deploy" },
    { tool: "GitLab", icon: "tanuki", finding: "v1.4.2 shipped checkout-api!42, the pricing cache.", name: "!42", links: ["checkout-api!42"] },
    { tool: "Code search", icon: "code", finding: "!42 added cache.get in pricing.rb:57, with no timeout.", name: "pricing.rb:57" },
  ]),
  cross("epic", "Track a Linear project", "Is Linear project ORB-1 on track?", "On track for the 500 ms goal. ORB-13 has no code yet.", [
    { tool: "Linear", icon: "work-item-issue", finding: "ORB-1, checkout latency below 500 ms, has 3 issues.", name: "ORB-1" },
    { tool: "GitLab", icon: "tanuki", finding: "ORB-12 shipped in !42 and !43.", name: "ORB-12 shipped", links: ["checkout-api!42", "checkout-api!43"] },
    { tool: "GitLab", icon: "tanuki", finding: "ORB-14 is in review in !44. Its pipeline passed.", name: "ORB-14 in review", links: ["checkout-api!44"] },
    { tool: "Linear", icon: "work-item-issue", finding: "ORB-13 has no merge request yet.", name: "ORB-13" },
    { tool: "Grafana", icon: "chart", finding: "Checkout p95 is at 420 ms, under the goal.", name: "p95 420 ms" },
  ]),
  cross("why", "Explain why this code changed", "Explain every decision behind the pricing cache code.", "Three decisions: !42 added the cache for ORB-1's 500 ms goal, !43 added request coalescing after INC-77, and the team set the 30 s TTL in Slack.", [
    { tool: "Code search", icon: "code", finding: "The pricing cache lives in pricing.rb, with PRICE_TTL = 30.seconds.", name: "pricing.rb" },
    { tool: "GitLab", icon: "tanuki", finding: "!42 added the cache. !43 added request coalescing and set the TTL.", name: "!42, !43", links: ["checkout-api!42", "checkout-api!43"] },
    { tool: "Linear", icon: "work-item-issue", finding: "ORB-12, under ORB-1: bring checkout p95 below 500 ms.", name: "ORB-1" },
    { tool: "Slack", icon: "comment", finding: "“Keep the TTL at 30 s. Any longer and flash-sale prices go stale.”", name: "TTL decision" },
  ], true),
];

export const CORPUS = [
  { value: "51,656", label: "projects" },
  { value: "1.38 M", label: "branches" },
  { value: "44.2 M", label: "commits" },
  { value: "45.7 M", label: "files at HEAD" },
  { value: "22.6 M", label: "unique files" },
  { value: "767 GiB", label: "index" },
];

export const LATENCY = [
  { label: "Text search, default branches", ms: 80, note: "benchmark, 3,351 projects" },
  { label: "Structural search", ms: 111, note: "early test" },
  { label: "Text search, every branch and commit", ms: 120, note: "benchmark, 3,351 projects" },
];
