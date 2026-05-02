## [0.43.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.42.0...v0.43.0) (2026-05-02)

### Features

* **ontology:** separate CI edges into dedicated gl_ci_edge table ([97609e5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/97609e5127e71b045ada9a44636dce7dedb793f5)) by Michael Usachenko

### Performance

* **schema:** add ngram skip indexes and auto column statistics ([1af5814](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1af5814b010f8b84a00e376734b445faea26738a)) by Michael Usachenko

### Other

* **indexer:** restructure top-level files into engine/ and config modules ([4256cbd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4256cbd3f58761d4384ea55727556b52b3db0c7d)) by Jean-Gabriel Doyon
* update landing page ([8d45d50](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8d45d502a9f8d3717c8661c0ac5ca1c0af525bfb)) by Phillip Wells
* update mcp tools ([517de22](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/517de2258ca166cf03a762a663335573ede09751)) by Phillip Wells
* update query language fields ([2080468](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2080468434207ac46d24d5fc7d903c21be48d690)) by Phillip Wells
* update troubleshooting ([2cce45d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2cce45d95c0635abefabb46947341ffb9ef7d1bf)) by phillipwells

## [0.42.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.41.1...v0.42.0) (2026-05-01)

### Features

* **content:** add diff virtual field to MergeRequest ([5d20fc1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5d20fc1a5b7de1757bb426491369ea02e2cab97c)) by Jean-Gabriel Doyon
* **server:** add MR diff virtual field resolver and gitlab-client methods ([ad8c284](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ad8c2840b042c0ee494dcf5c5f465f93c5b4c4c0)) by Jean-Gabriel Doyon

### Fixes

* **indexer:** resolve merge_request denormalised fields from siphon sources ([c97b251](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c97b25164d329b3833ef79ac122ddf066f167d4c)) by Jean-Gabriel Doyon

### Performance

* **compiler:** partial-match edge tag rewrite for mixed-filter queries ([02ab415](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/02ab415a0ae22d0e9d902dcbd3b70035d41e693e)) by Michael Usachenko

## [0.41.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.41.0...v0.41.1) (2026-05-01)

### Fixes

* **auth:** allow cross-org traversal paths in security context ([d0a1e59](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d0a1e59016a2956666f102ffec6125c02dc7564e)) by Jean-Gabriel Doyon
* **indexer:** add stream max age to nats for disaster recovery ([0812b49](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0812b496f3399fd881ac04014a0f1763d2e49768)) by Jean-Gabriel Doyon

### Performance

* **ci:** split compiler integration tests out of unit-test job ([f32ea71](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f32ea71936b6a7628e9bc136dc1929099bb895eb)) by Michael Usachenko
* **compiler:** add LEFT SEMI JOIN support and move perf options to derived schema ([39a83e0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/39a83e05411de3627f2b53029e35cf87656d4282)) by Michael Usachenko
* **compiler:** extend edge tag filtering to cascade and hop frontier CTEs ([022879b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/022879bead2fbf82edba7ec77c23f5283b3de6ff)) by Michael Usachenko

## [0.41.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.40.0...v0.41.0) (2026-04-30)

### Features

* **compiler:** materialize multi-referenced CTEs for ClickHouse ([fba4392](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fba4392b236839257d6911698c9fb828a5ca4955)) by Michael Usachenko

### Performance

* **compiler:** deduplicate cascade and hop frontier CTE outputs ([ff0a6a8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ff0a6a81099f663aec01f249f718625bd9b56910)) by Michael Usachenko

### Other

* **schema:** bump SCHEMA_VERSION to 24 ([79db0f6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/79db0f66a94efc20569fbbc51317a364cad9b11a)) by Michael Usachenko

## [0.40.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.39.0...v0.40.0) (2026-04-30)

### Features

* **resilience:** add circuit-breaker crate ([a8c1f9c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a8c1f9c52bd5b12a579f101583e8219e96d9678c)) by Jean-Gabriel Doyon

### Fixes

* **indexer:** drop siphon_namespaces join from namespace dispatcher query ([8f3ba26](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8f3ba2692c2c163b0f68c5b0e061280a49642f64)) by Jean-Gabriel Doyon
* **mise:** switch go-jsonnet from aqua to github backend ([356a696](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/356a696dc30b135bd478fe494de170633904f954)) by Thiago Figueiró

## [0.39.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.38.0...v0.39.0) (2026-04-30)

### Features

* **compiler:** optimize queries using denormalized edge tags ([c2fc219](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c2fc219eed2e762be7d0933ff4afd2af4178e2de)) by Michael Usachenko
* **config:** add analytics enabled + collector_url fields ([fb5ad25](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fb5ad25e0bb15907178af7355ecd4a4743fb887a)) by Bohdan Parkhomchuk
* **indexer:** populate denormalized edge tags at index time ([269bfd6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/269bfd67ecfa5b98c054d441675a92ec9d9383a8)) by Michael Usachenko

### Performance

* **compiler:** cascade from auth-scoped nodes when no node_ids exist ([ae7fa6f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ae7fa6f3fff049d6465d3a7eae515c9b5bad23d0)) by Michael Usachenko

### Other

* add gitleaks secrets detection to pre-commit hook ([ea7d5f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ea7d5f9b9c10979a52c24fcba2c6ff9396df1ad0)) by Dmitry Gruzd
* **compiler:** add skip_dedup query option ([23945e5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/23945e5887bfc8dff832c13f4c74d07cf4cb6d7d)) by Michael Usachenko

## [0.38.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.37.1...v0.38.0) (2026-04-29)

### Features

* **ontology:** add edge property denormalization schema and DDL ([ca32cb9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ca32cb92240c2d638202a58a8a5d680ff559378f)) by Michael Usachenko
* **schema:** add pipeline_id FK and projections on gl_job and gl_stage ([2f08dae](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2f08dae11de87168c1fae8841f24aefcfd8dc81a)) by Michael Usachenko

### Performance

* **compiler:** use id-only sort key for cascade-fed dedup subqueries ([c75bee3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c75bee3d819effa4c0a6804718e23fa16573e239)) by Michael Usachenko
* **schema:** add _deleted to code graph RMT, projections on MR and Note, granularity 1024 ([38f1620](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/38f1620d1d34c37874124fca2351f371cfbb8c47)) by Michael Usachenko

## [0.37.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.37.0...v0.37.1) (2026-04-29)

### Fixes

* **indexer:** use batch pull instead of no-wait fetch in NATS subscription loop ([9f7a936](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9f7a936ab07a6b19d1723578be0d7ce30e24f612)) by Jean-Gabriel Doyon
* **query:** prune filtered path-finding frontiers ([b429a88](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b429a8810d54d51cf3f93438348a124424f12f67)) by michaelangeloio
* **query:** prune wildcard traversal SQL shape ([b97620b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b97620b24afd21a2bc77e70782e753f2285f4436)) by Michael Angelo Rivera

### Performance

* **compiler:** collapse traversal paths via trie subsumption ([4817199](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/48171999631fd3568f346aa7e416839f416cc493)) by Michael Usachenko
* **e2e:** combine license and PAT bootstrap ([4efd4d8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4efd4d868b53bb79d27fc047d561f5d0cfedf6aa)) by Bohdan Parkhomchuk
* **ontology:** add code-edge relationship projections ([80ab470](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/80ab470c83c2451856d30fb9bb0741eedb5905ab)) by michaelangeloio

### Other

* **e2e:** cover role-scoped authz matrix ([a2e3c57](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a2e3c57034a43d430c39d5fb2970e5f1fa3966e1)) by Michael Angelo Rivera

## [0.37.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.36.0...v0.37.0) (2026-04-29)

### Features

* **compiler:** text index query lowering and token search operators ([5e8e234](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5e8e234f6a9299358ccb8209eba2fe9b911c6fb9)) by Michael Usachenko

### Performance

* **clickhouse:** use planner defaults and temporal minmax ([714bda5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/714bda50c1611f0945d12c97d01116f125d8d822)) by michaelangeloio

## [0.36.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.35.0...v0.36.0) (2026-04-28)

### Features

* **server:** classify execution errors with diagnostic hints ([ed15068](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ed15068738b4ed4caa8260f201451134015bd6d9)) by Michael Usachenko

### Fixes

* **indexer:** report SDLC row count metric per batch instead of per pipeline ([63906d9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/63906d9ebbed279e7464a02e121bd00ce84ff8d4)) by Jean-Gabriel Doyon

### Performance

* **compiler:** extend hop frontiers and multi-hop cascade to all query types ([1689c77](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1689c774afa16e1caea67a72029687d606c78f49)) by Michael Usachenko
* **compiler:** inline _nf_* CTE filters into dedup subqueries ([589a4e8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/589a4e8970c981bae86e89a07dd0caf66dd201a1)) by Michael Usachenko
* **indexer:** skip non-parsable files during archive extraction ([5eb0250](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5eb0250c00d04da55b793b9c0117bf15e9f8faea)) by Bohdan Parkhomchuk
* **ontology:** add text indexes on hot string columns (CH 26.2+) ([90a4e9f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/90a4e9fd0fa2fa318dd1d5eae8578f25ea89ea62)) by Michael Usachenko

### Other

* **code-graph:** typed file outcomes replace string-classified errors ([43e3321](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/43e33210bf36b939128d9a2c59b960757937ca40)) by Michael Angelo Rivera
* **deps:** bump testcontainers 0.27.1 -> 0.27.3 and fix advisories ([b3b4cb5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b3b4cb5b381bfaed9adffd0ef04e5f162b103550)) by Jean-Gabriel Doyon

## [0.35.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.34.1...v0.35.0) (2026-04-28)

### Features

* **graph-status:** per-entity authorization for entity counts ([2e4f514](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2e4f51451f2a6f013178f8dfe87e635d4215fcf0)) by Jean-Gabriel Doyon
* **observability:** resources rows and rails-kg merge ([e04fcc1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e04fcc152c231283f446518f275f1ca7bace72ae)) by Michael Angelo Rivera
* **ontology:** allow multiple FK edges per source column ([17c42fc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/17c42fc58e263269ede8f233d82ad87a40ca2bce)) by Michael Angelo Rivera
* **ontology:** close CI graph gaps for runner attribution and parent/child pipelines ([f0b38fd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f0b38fdb92079daecb0f645b767bc027a67f4474)) by Michael Angelo Rivera
* **ontology:** source ASSIGNED and HAS_LABEL edges from standalone Siphon tables ([c695b12](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c695b12a374b2e2c4faab2a5310d67d17fcae5ca)) by Jean-Gabriel Doyon
* **ontology:** source REVIEWER, APPROVED, ASSIGNED edges from standalone Siphon tables ([0205dfe](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0205dfe7a0fa4e589f641e3af2b3eecfce906b7a)) by Jean-Gabriel Doyon
* **query:** add include_debug_sql option to control SQL in responses ([a8779a7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a8779a7cd847b7d05376b4889df320b42d5b6b27)) by Michael Usachenko

### Fixes

* **code-graph:** mask sign bit so node ids are always positive ([950394e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/950394eea6ce3e9eaf2198e71b6138da3afd4f68)) by Michael Angelo Rivera
* **compiler:** bind DateTime literals as typed DateTime64 params ([ffb69ac](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ffb69ac113cd7111c3e11b1a7b70a73680dff23d)) by Michael Angelo Rivera
* **compiler:** reject direction both on aggregation relationships ([34328a6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/34328a6e3f5196e5027d2663f5e9ea5eada2900e)) by Michael Usachenko
* **compiler:** require rel_types on path_finding with filtered endpoints ([e735f64](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e735f646275f99c37188fd556ffd12ea074dd245)) by Michael Usachenko
* **compiler:** surface ontology validation errors to clients ([42a785d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/42a785d08f0db7e7a10bc02f134eb7f1b36bdbad)) by Michael Usachenko
* **compiler:** wrap UNION ALL in subquery when outer LIMIT or ORDER BY present ([7e66950](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7e66950fe844d98b4828c5421da544c533aead1e)) by Michael Angelo Rivera
* **formatter:** dedupe path_finding and neighbors edge versions ([b993573](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b993573339620a7207ed6752eb55a27dd013361e)) by Michael Angelo Rivera
* **indexer:** use correct label for group permit active_permits decrement ([992f448](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/992f44866acdf162f3613271d541bdf28db6f7d0)) by Jean-Gabriel Doyon
* **observability:** add LATENCY_SLOW buckets for long-running indexer metrics ([6f8f47e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6f8f47eb24d044ff7950587075ae8d741c7ad556)) by Jean-Gabriel Doyon
* require group_by for multi-node aggregation queries ([72f16e0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/72f16e0ebca1736b341ac347d111186443d906a7)) by Michael Usachenko

### Performance

* **compiler:** cascade CTEs for multi-hop aggregation relationships ([f1c7b10](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f1c7b10c59f55aaed5309631e6d5accc2900935f)) by Michael Usachenko
* **compiler:** hop frontiers fire for filter-derived CTEs ([9ad461d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9ad461dd5fb6d6a9ef7797db1cbde8abe5f10c5b)) by Michael Usachenko

### Other

* clean up remaining search query_type references ([869573c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/869573c431b35a9e8de719d9c250804c982eee6d)) by Michael Angelo Rivera
* **code-graph:** remove legacy parser and linker ([b0cc291](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b0cc291dd5287e74312fb0ade6e198aa1c0ab580)) by Michael Usachenko
* **queries:** replace search query_type with traversal ([5c6a491](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5c6a49112dd9b8152de1484441d362fbdb421057)) by Jean-Gabriel Doyon

## [0.34.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.34.0...v0.34.1) (2026-04-27)

### Fixes

* **indexer:** provision full subject union on GKG_INDEXER ([4671b25](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4671b25222cb205a3b379efbcb9169c5b14e8bb6)) by Bohdan Parkhomchuk

## [0.34.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.33.2...v0.34.0) (2026-04-27)

### Features

* **compiler:** add hop frontier CTEs for multi-hop traversals ([a46af71](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a46af71af9bacee9a2930b4ff44546e2bc725fb3)) by Michael Usachenko
* **compiler:** allow filters and id_range on path_finding endpoints ([3d0f9f7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3d0f9f735103820e680ab538e0b8fb0316802fc1)) by Michael Usachenko
* **grpc:** add ResponseFormat support to GetGraphStatus ([fa2752c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fa2752c6d809584ecf24904ecb11d97515aa2756)) by Jean-Gabriel Doyon
* **indexer:** selectable modules per indexer process ([4ed457d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4ed457d0986d0521192aee7b3f8ab39cb0507617)) by Bohdan Parkhomchuk
* **indexer:** split benign code-pipeline file skips out of errors_total ([99478ab](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/99478abb8e27d88d8bd18b720b205a0a7a6b39c7)) by Michael Angelo Rivera
* **observability:** story-shaped indexer dashboard with code/SDLC split ([84b60f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/84b60f98eca6f54c5c59697c2ba709b3c59a31a0)) by Michael Angelo Rivera
* **ontology:** register CALLS and EXTENDS, drop custom Ruby pipeline ([5c8872a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5c8872acbd57a02d01a7ebf4847b8a1e8a45be14)) by Michael Angelo Rivera

### Fixes

* **compiler:** close selectivity validation gaps for path_finding and id_range ([a57ae4f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a57ae4f83e9e543e30322801a1d4e503a2a9d305)) by Michael Usachenko
* **compiler:** reject unbounded queries that cause full-table scans ([8dd64ff](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8dd64ff2ac181035e5ca4800821ede9d07280072)) by Michael Usachenko
* **e2e:** fix broken test harness ([24d10bf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/24d10bf13f71337e6ee553d577fdc771a6fa3419)) by Bohdan Parkhomchuk
* **indexer:** classify permanent vs retryable handler errors ([75b4d3f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/75b4d3f46ba366878da1a0181ba21176b526dbad)) by Jean-Gabriel Doyon
* **indexer:** match code indexing lock TTL to NATS ack_wait ([65ad7b7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/65ad7b7e8d62f98f5e0be026185230c98ebc5560)) by Jean-Gabriel Doyon
* **indexer:** pin dead-letter stream max_age to 0 (no expiry) ([854eb2d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/854eb2d080d09f9cbbf7c0b7804e2070cba69c02)) by Jean-Gabriel Doyon
* **indexer:** write checkpoint on 404 ack in code-indexing handler ([8d53891](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8d53891cfa7ce281347fe0342553761c1af87174)) by Michael Angelo Rivera
* **query-engine:** per-entity role scoping for authz ([c24b2b9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c24b2b971e57a2536fceba5fe8953d6deb18995b)) by Michael Angelo Rivera
* **tests:** add selectivity to CALLS/EXTENDS test queries ([6746c24](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6746c247241a6142af359c1134ad4500731c1b4d)) by Michael Usachenko
* **tests:** migrate stale search query_type to traversal ([f4537d9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f4537d9ed3ecf34727913c125f2a546d412bd7a0)) by Bohdan Parkhomchuk

### Performance

* **compiler:** close 2 aggregation-traversal cliffs ([5930c8b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5930c8bd5d87ee2e67fbbbbb13bc5c94cb13f4fd)) by Michael Angelo Rivera
* **compiler:** close 4 query cliffs ([6fdc896](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6fdc89638c03addff01702810c77c49930a228f9)) by Michael Angelo Rivera
* **compiler:** close variable-length traversal cliff ([6f08588](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6f085884f340838e86cb5e217a59d2e67a9fd90c)) by Michael Angelo Rivera
* **compiler:** hoist sort-key filters into aggregation dedup subquery ([96adcd9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/96adcd9373a6bdb5826216c69da6955ecfd5ea69)) by Michael Angelo Rivera
* **ontology:** add source-side aggregate projection to gl_edge ([750c095](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/750c0959cf711682346efa1627ee29d3520dfd56)) by Michael Angelo Rivera

### Other

* **compiler:** make search a special case of traversal (1 node, 0 rels) ([ecd3f9e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ecd3f9e6f6d16240ebfb843e2dc130536b21bd90)) by Michael Usachenko
* **deps:** update rust-analyzer crates to 0.0.329 ([16c0117](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/16c011768b9a99f19d991f3c69d426e101f2c3fc)) by Jean-Gabriel Doyon

## [0.33.2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.33.1...v0.33.2) (2026-04-26)

### Fixes

* **nats-client:** use create_or_update_key_value to migrate KV bucket config ([e2f389c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e2f389c0e982739a2507742fe04608949f664a75)) by Michael Angelo Rivera

## [0.33.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.33.0...v0.33.1) (2026-04-25)

### Fixes

* **ontology:** bound unbounded joins in Group and MergeRequestDiffFile ETL ([b25aa44](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b25aa44f52ac47191f33f788498cdc396f220a0c)) by Jean-Gabriel Doyon

## [0.33.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.32.0...v0.33.0) (2026-04-25)

### Features

* **observability:** jsonnet-based orbit dashboard generator ([baa9f52](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/baa9f52055a3199b985d29e05ac90390ea020023)) by Michael Angelo Rivera
* **ontology:** introduce admin-gated User columns ([800f307](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/800f3070b37731c2dab744d3a353eeeff8562924)) by Michael Angelo Rivera

### Fixes

* **indexer:** clamp out-of-range Date32 values to NULL in SDLC extract ([044e288](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/044e288e3a443dc3261df3f722fe326e2b38c614)) by Michael Angelo Rivera
* **indexer:** classify empty 200-OK archives as indexed-empty ([b64af3d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b64af3d129a65ae9005690619acc14209f1bf81f)) by Michael Angelo Rivera
* **indexer:** dlq subjects for wildcard deliveries ([10e52f4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/10e52f45d56929c42e0ccedde5ba14af8b48bdea)) by Michael Angelo Rivera

### Other

* **schema:** bump schema version to 8 ([a929503](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a92950374ddd212b6ea31657fe10c246710e9aee)) by Michael Angelo Rivera

## [0.32.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.31.1...v0.32.0) (2026-04-25)

### Features

* **code-graph:** initial C# resolution support with comprehensive test coverage ([b32cf72](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b32cf72b20789afc60f07e09368b9465b5918c59)) by Michael Usachenko

### Fixes

* **indexer:** adaptive halving on SDLC datalake retry ([1c88346](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1c8834608268a604c23eff818e996af2730d9404)) by Michael Angelo Rivera
* **indexer:** add per-pipeline batch_size_overrides to prevent Arrow 2 GiB cap ([85ff1df](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/85ff1dfca0ba723660a7e62a3ac33b1a33c39800)) by Jean-Gabriel Doyon
* **indexer:** drive code backfill from coverage, not migrating-version gate ([0d0a33c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0d0a33c9a7839bf7d164bf34a8d1c445b5d2c720)) by Michael Angelo Rivera

### Performance

* arrow IPC compression, dictionary encoding, edge sorting ([2d0f7a5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2d0f7a521fa24e8b0fc131e4e76046896c9594bd)) by Michael Usachenko
* **query:** replace argMax with LIMIT 1 BY for search dedup ([0dc6b41](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0dc6b411f4b718e8e433a33d9fb8c0b4566b78a9)) by Michael Usachenko
* **query:** replace argMax with LIMIT 1 BY in _nf_* CTEs ([97a5434](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/97a5434b16e4c5c9a81b82267c1acf8388984af9)) by Michael Usachenko

## [0.31.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.31.0...v0.31.1) (2026-04-24)

### Fixes

* **indexer:** avoid Arrow 2 GiB column overflow in SDLC transform ([e31758b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e31758b67dbe26b45a06600ae372c92f66b7b72a)) by Jean-Gabriel Doyon

## [0.31.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.30.0...v0.31.0) (2026-04-24)

### Features

* **billing:** emit Snowplow billing events on successful ExecuteQuery ([92d3371](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/92d3371a6e3193f9060a1127fb2b002766645db7)) by Sharmad Nachnolkar
* **code-graph:** streaming pipeline with per-language writer threads ([37b5cb8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/37b5cb83f8a51db9458f651dddc18b4f658d121b)) by Michael Usachenko
* **graph-status:** wire NATS KV indexing progress into GetGraphStatus response ([edfbc64](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/edfbc644927084f02f16e56f1481f1e27975bf38)) by Jean-Gabriel Doyon
* **indexer:** configure code indexing v2 pipeline ([ad32db3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ad32db3351edfca7dc74df6a92893b5c6b6fee25)) by Michael Angelo Rivera
* **indexer:** enable v2 code indexing pipeline in production ([ed64d2d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ed64d2db491b071e7731b51e688fc70b8770a55f)) by Michael Usachenko
* **indexer:** improve code indexing metrics ([2d561f3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2d561f3e0446f79a3f944247dad6f8a56d7feff3)) by michaelangeloio
* **indexing-status:** write per-run indexing progress to NATS KV ([a50bba4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a50bba4bdf45f65c3fccbb20f3a68fec5a145470)) by Jean-Gabriel Doyon
* **observability:** [secure] metrics catalog and orbit-dashboards library ([5ca8dfc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5ca8dfce1112154d80228550f00628e990e228b2)) by Michael Angelo Rivera
* **skills:** add orbit skill for Knowledge Graph queries via glab api ([9ae1052](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9ae1052086450770980bb32539d169c9624b0534)) by Dmitry Gruzd

### Fixes

* **code-graph:** harden custom v2 indexers ([e1d6f83](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e1d6f83305b942cc28911b522c3f7436aab0e94e)) by michaelangeloio
* **code-graph:** v2 pipeline hardening — error propagation, stack guards, tracing ([1ca434a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1ca434a894282b79f5baa4c630f5a4ac2a6e9bfb)) by Michael Usachenko
* **compiler:** prevent cascade InSubquery filters from being folded into aggregates ([560725d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/560725de14cb8bf428258569d315c7e424c6bead)) by Michael Usachenko
* **dev:** replace Python+PyYAML with yq in parse_gdk_value ([a867987](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a8679877f0aec12458bb28134b5309260e30e5a1)) by Dmitry Gruzd
* **grpc:** pair max_connection_age with grace to avoid tonic 0.14.5 panic ([c527af9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c527af965a4dd804e3f9fce6610016a3ed94d2c3)) by Michael Angelo Rivera
* **indexer:** fail code indexing when v2 writes fail ([9407db2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9407db281d66a1eac71da1646bdc3a1435f01272)) by michaelangeloio
* **indexer:** tolerate dangling symlinks in archive extraction ([ffb5d1d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ffb5d1dc633eea7ec774796f4f1be9fb18cce7dd)) by Jean-Gabriel Doyon

### Other

* add ai-review-bot CI component for automated MR reviews ([922dd77](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/922dd7726d3278f8380fdebbb584e6d87056efd1)) by Dmitry Gruzd
* **ai-review:** switch component to Opus 4.7 with xhigh variant ([bcc7e1e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bcc7e1ea9ac80b13e01b1ca360a1f774aa17cc68)) by Dmitry Gruzd
* **nats:** extract nats-client crate from indexer ([3e0d7f0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3e0d7f0d00732f7fad1ec4da36808f9ec7a080e7)) by Jean-Gabriel Doyon

## [0.30.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.29.1...v0.30.0) (2026-04-23)

### Features

* **graph-stats:** add source_type, project coverage and replace count() with uniq(id) for graph stats ([afc2885](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/afc2885169db68983c66b26eecf0b7036120a034)) by Jean-Gabriel Doyon
* **ontology:** activate gl_code_edge table for code-domain edges ([db8f638](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/db8f638a6275093445a4d64cdf6ad83e7eabdf18)) by Michael Usachenko
* **ontology:** add Deployment and Environment nodes ([6260ba1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6260ba133b2c46989dff6b6dee9c61b83382d1af)) by Jean-Gabriel Doyon
* **ontology:** add disabled diff and patch virtual fields for MR resolvers ([491eae8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/491eae80c4eeb30fee513dffd61222f3ca890160)) by Jean-Gabriel Doyon
* **ontology:** expand merge request entities coverage + note st_diff ([9262ccb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9262ccb8e9420daa5aa29e056f0d641069da68e4)) by Jean-Gabriel Doyon

### Fixes

* **ci:** stop triggering code-indexing benchmarks on ontology YAML changes ([13c8df0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/13c8df0c34fcae68c884f1b60d80c2f0b0508f47)) by Jean-Gabriel Doyon
* **compiler:** reject aggregation on globally-scoped entities ([13abdd2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/13abdd2d96ce8582cda2eac5cbec50a0b00bbaa2)) by Michael Angelo Rivera
* **ontology:** expose query dsl schema via cli for use by agents ([ea01604](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ea0160497876e022e38fb83281419db4826e4c5a)) by Michael Usachenko

### Other

* add ADR 009 for evolving GetGraphStats into GetGraphStatus ([b2c6fdb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b2c6fdb65cee3cc375fa45bc3e84d14ed8a33a60)) by Jean-Gabriel Doyon
* **graph-status:** rename GetGraphStats to GetGraphStatus ([345099b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/345099b74344a061cf8631dd421f738c4d114bda)) by Jean-Gabriel Doyon

## [0.29.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.29.0...v0.29.1) (2026-04-22)

### Fixes

* **compiler:** enforce per-entity role on aggregation target nodes ([4a5bd9d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4a5bd9d0df7321a923031f7bc70a65cab9a1f8d5)) by Michael Angelo Rivera
* **indexer:** count code-eligible namespaces in migration completion check ([7b8b28e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7b8b28e8add8ae7f768b8d17990bf0c634c31768)) by Michael Angelo Rivera

### Other

* **deps:** resolve new cargo-deny advisories on main ([cb2851e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cb2851eadbc16a8b17bf866e072c84e97d229424)) by Michael Angelo Rivera
* revert fix-347-aggregation-authz-per-entity-role' ([e3fd32b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e3fd32ba3d7e6540d28c4e6515f523bd59c0cfa2)) by Michael Angelo Rivera

## [0.29.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.28.0...v0.29.0) (2026-04-22)

### Features

* add CODEOWNERS for security and core files ([d3de60d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d3de60d6185f04bfea961e7e18de79d3a9a079fe)) by Michael Usachenko
* **cli:** add `orbit schema` introspection scoped to local DuckDB ([d6609a8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d6609a891fe2787c56b79d4de6c69c5624673e8c)) by Michael Angelo Rivera
* **code-graph:** add JavaScript and TypeScript v2 config ([6be5d60](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6be5d60e278c41d7bd1da4b12c2622cd3af230c8)) by Michael Angelo Rivera
* **code-graph:** add rust-analyzer-backed Rust v2 pipeline ([b24d74d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b24d74d9d7cbf237de815d422471594e4534b048)) by Michael Angelo Rivera
* **code-graph:** getting v2 code indexing ready for production ([3e641d2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3e641d2febbcab3d4415b82cc2e3ae7fb1cf4cca)) by Michael Usachenko
* **code-graph:** integrate js ecosystem indexing into v2 ([899a2d8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/899a2d83124274dbfc92661c341a9c975ce39062)) by Michael Angelo Rivera
* **code-graph:** re-add phi elimination + CLI thread propagation ([9f0743b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9f0743bdd81505c31265d3bcb4838c3e7de63d77)) by Michael Usachenko
* **code-graph:** significantly simplify reference resolution ([6f284db](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6f284db26baf34b5a24da97817f5948dd697c1a6)) by Michael Usachenko
* **code-graph:** ssa resolution improvements ([927e884](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/927e884f6d0d1d95594b8a44a4b433c46350a329)) by Michael Usachenko
* **code-graph:** structured dsl and resolver tracing and Java v1 parity fixes ([965667e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/965667e761a5dce51fd314ec156acdcebdc918de)) by Michael Usachenko
* **code-graph:** v1 coverage complete — Kotlin, Python, Ruby resolution ([28bf7cf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/28bf7cf32fc69121b497af3bb3274a51347be74e)) by Michael Usachenko
* **code-graph:** v1 parity — 62 tests unskipped, tracer cleanup ([de1adbe](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/de1adbe38807149f31197b43fa9b40d0fdef60f9)) by Michael Usachenko
* **code-graph:** v1 parity — Go coverage, deterministic indexes, dead code removal ([0c62809](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0c628099280d74a643518572ccdcc4ecd8ee2d86)) by Michael Usachenko
* **code-graph:** v2 resolution improvements with better fqn coverage ([96f2591](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/96f2591c07e353178ee4b28485950d1cad0e8578)) by Michael Usachenko
* **e2e:** generate siphon CDC config from SSOT ([9eb2c9b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9eb2c9b8b049179cc880c6d36f9b3952702068db)) by Bohdan Parkhomchuk
* **webserver:** readiness gate on schema version ([d0c96fe](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d0c96fe1646f9a8067a9bd90653e71f3f1fa26a2)) by Bohdan Parkhomchuk

### Fixes

* **ci:** enforce monotonic SCHEMA_VERSION increments ([34decce](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/34decce44f6c4601d04e40bcd03a54a541dd8531)) by Michael Usachenko
* **code-graph:** drop dead chain_mode from custom rules constructor ([d13b792](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d13b7927448b6370dab40d7c5445dd55c30ae53b)) by Michael Angelo Rivera
* **code-graph:** include byte range in v2 Definition/Import node IDs ([535b9d7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/535b9d71de53bba8612ec415f1e7267e75a05933)) by Michael Angelo Rivera
* **compiler:** aggregation pk in GROUP BY and DuckDB LIKE escape ([79eeb6d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/79eeb6d89f15defdeb550e1063ae218aa33e02c6)) by Michael Angelo Rivera
* **compiler:** aggregation with relationships emits valid SQL ([35f8fd4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/35f8fd43e1167c05683dbb73fc983d14b443e0e4)) by Michael Angelo Rivera
* **e2e:** pin gitlab devel and skip post-deploy migrations ([a3333be](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a3333be9cdadd2dd6f5bbae6a68bed681939c1cd)) by Bohdan Parkhomchuk
* **fs:** validate_symlinks deletes all bad symlinks, not just the first ([cfeb8be](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cfeb8be00d4eca3f578a33cb0438b81e8957d2be)) by Michael Usachenko
* **indexer:** ack code tasks when project_info 404s instead of nacking ([36af03c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/36af03cdbacb2606170879233e0e58c6383600ba)) by Michael Angelo Rivera
* **indexer:** clarify schema version bump policy for data-value changes ([94b71e4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/94b71e46f2d9114fcc50cb120167101e2b258090)) by Michael Usachenko
* **indexer:** harden archive extraction against symlink traversal attacks ([d4375cf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d4375cf1583c04648c1ee8ba51844a3e3dda96ed)) by Michael Usachenko
* **indexer:** map polymorphic noteable_type to WorkItem ([dc8d754](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dc8d7541dcd58e2344d440343b663ab314566f19)) by Bohdan Parkhomchuk
* **security:** restrict is_admin and is_auditor visibility to admins ([2d37f46](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2d37f46764b8fc67ebe4c4a348de0fed05aec89a)) by Michael Angelo Rivera

### Other

* bump rust to 1.95.0 ([30a9e49](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/30a9e49e14c0d1c99a3b1cdb54b669d06f05b4d0)) by Michael Angelo Rivera
* **code-graph:** collapse sub-crates into v2/legacy split ([4c7809a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4c7809ad6e50e98a6d729fc4d2f58461f6d6bc4f)) by Michael Angelo Rivera
* **code-graph:** harden integration test assertions ([d49662c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d49662c761c80a49ae2c02f076559560152501ca)) by Michael Usachenko
* **code-graph:** port v1 legacy resolution tests to v2 integration fixtures ([5eaf79a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5eaf79aee1990e433b906ee0a8ac32d70582eb00)) by Michael Usachenko
* **e2e:** activate license, cover sdlc edges ([0cbd08c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0cbd08c7f03cfe027518b9def1d84414c25a0cb0)) by Bohdan Parkhomchuk
* **e2e:** bump siphon to 0.0.64-beta with faster snapshot polls ([c8371fb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c8371fbe859622a7653b1232c7ef06c7deaaa7ae)) by Bohdan Parkhomchuk
* **e2e:** code backfill on namespace enablement ([4fd557a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4fd557a262c403142122b5d558555cc14365728e)) by Bohdan Parkhomchuk
* **e2e:** code indexing assertions via orbit api ([1ac9fc9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1ac9fc97574adecaa23d63ac0ec011630cc79d2e)) by Bohdan Parkhomchuk
* **e2e:** run on main and simplify test job ([363532e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/363532efbc31b0c84ec917b1fc51c70420f56cc3)) by Bohdan Parkhomchuk
* fail pipeline when benchmarks fail ([36587e6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/36587e6182d4a759b9093f48f66869c70339603d)) by Michael Angelo Rivera
* move e2e stage after build ([3a0ce49](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3a0ce49162b1553931d4f0fcf3c80b81632311a7)) by Bohdan Parkhomchuk
* revert CODEOWNERS file ([df901fd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df901fda05864116c4c3577f8570f3328323af15)) by Michael Usachenko
* **security:** add aggregation SQL security assertions and multi-path aggregation tests ([2bbb54c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2bbb54c765443369d371c52290995f091bfd994c)) by Michael Usachenko
* **security:** add cross-org isolation tests and fix security docs ([5e59d1c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5e59d1c7b8efc7708d94090d0ab0f2b8252346a3)) by Michael Usachenko
* switch to [secure] rust build-images registry ([73cb4af](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/73cb4afed03ef5681cd6ace12ab8a8fa4ec2febc)) by Bohdan Parkhomchuk

## [0.28.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.27.0...v0.28.0) (2026-04-17)

### Features

* **code-graph:** custom pipeline support with PipelineOutput enum ([11de4ec](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/11de4ecd48e354715c195ce95dcd3073c90d7223)) by Michael Usachenko
* **code-graph:** go and ruby DSL language specs ([4a7f0cc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4a7f0cce9ec8abd7e0b70e2bd73fdb791e5cc0b5)) by Michael Usachenko
* **config:** add deployment environment configuration ([36b366a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/36b366a5d79b8da160cd535b50ea04433e5a4d45)) by Michael Angelo Rivera
* **formatters:** add semver-based output format versioning ([4ad78f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4ad78f942991ed7be0ecea508a9ee2020df208ab)) by Michael Angelo Rivera
* **observability:** add content resolution metrics for Gitaly calls ([28e8e5d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/28e8e5da23fd3f55cc5f805117df2c0038956ed4)) by Michael Usachenko
* **server:** parse session_id from JWT and add to tracing spans ([be80251](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/be80251e4442cd5745f0067f269e9bc7790038cc)) by Ankit Panchal

### Fixes

* **indexer:** backfill code indexing during schema migration ([0970e23](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0970e23a304f201b25a5e2ec29eb30f5c3810d50)) by Bohdan Parkhomchuk
* **indexer:** checkpoint empty repositories as terminal ([55c949e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/55c949e44aa1b279ac82a95bb35bb02be51f6204)) by Michael Angelo Rivera
* **query-engine:** serialize graph IDs as strings ([5b6f389](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5b6f389636fc35189e434ae4a2f0932369516d64)) by Michael Angelo Rivera
* **server:** exit if no active schema version found ([61dbf48](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/61dbf4853ba149ff4053153f66e054b5440e384e)) by Bohdan Parkhomchuk

### Performance

* **code-graph:** centralized linker state, and DuckDB conversion fixes ([0ca3ff4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0ca3ff43c7bfb66de4cd07cb397d11ce7656ebe6)) by Michael Usachenko
* **code-graph:** complete ssa implementation with witness caching, SCC removal, copy propagation ([0d810b4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0d810b4ca59ae9c92bf245ced5555c6610463952)) by Michael Usachenko
* **code-graph:** fused pipeline, invariant-based skips, streaming reads ([a9dd528](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a9dd52830a93104d0362cf7ce035bbbeabf98f39)) by Michael Usachenko

### Other

* **benchmark:** skip benchmark jobs in forks ([05d5f9f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/05d5f9fcd879cba3786ad25faabd3f6409b8a2ec)) by Bohdan Parkhomchuk
* **e2e:** declarative test runner with live logs ([7f03636](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7f03636a343bb82e7536b559b497b33315876a74)) by Bohdan Parkhomchuk
* **e2e:** move kubectl resources to helm charts ([60f3648](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/60f36487645da17e7e2b81fef680c005240fdfaa)) by Bohdan Parkhomchuk
* **indexer:** remove code indexer disk cache ([a45ce75](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a45ce7540e254c8a04db1fb9f5c554de22780fbc)) by Michael Angelo Rivera
* **server:** builder pattern for GrpcServer, cache config, conditional NATS wiring ([546f979](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/546f9799e9441257657b271dba0e04755c3d9e80)) by Michael Usachenko

## [0.27.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.26.1...v0.27.0) (2026-04-16)

### Features

* **ci:** add v2 code-graph benchmarks and extract integration-tests-codegraph crate ([81527c7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/81527c75cf66d22c61fc404b1530e8610eb69b62)) by Michael Usachenko
* **code-graph:** code graph construction pipeline with SSA-based generic resolver ([04cc67e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/04cc67ee92a5f972e97f17ff12aa3d0102d89792)) by Michael Usachenko
* **code-graph:** graph-native operations, batched resolve, DSL ergonomics ([bd0f965](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bd0f965ed45a5aa3aee3762c5e19b7809c5aafa7)) by Michael Usachenko
* **code-graph:** type-flow resolution and ast-driven walking for ssa ([9954c9e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9954c9e942614ffaca5c0ce7c69da48d720298ce)) by Michael Usachenko
* **code-graph:** v2 pipeline performance + zero-fuzz resolution ([570e335](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/570e33548db8d9679f97a26762da4852a4f1024b)) by Michael Usachenko
* **config:** add migration-completion schedule task ([2a33dfa](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2a33dfae82adfc87e43f4a620abecdd4b14a5e63)) by Bohdan Parkhomchuk
* **e2e:** add e2e test harness with Robot Framework ([9b7a102](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9b7a1026488b70cdf22ed267e000b610383ac4ed)) by Bohdan Parkhomchuk
* **ontology:** add tp_count projections and update node_edge_counts ([4f7ff16](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4f7ff162a8e0870cbe0408909e069fd3fbd16182)) by Michael Angelo Rivera
* **ontology:** generate local DuckDB DDL from ontology ([dd848f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dd848f91ffd7e1e0629fb7dbd7f016646c8dae10)) by Michael Usachenko
* **ontology:** wire multi-edge-table routing through compiler and indexer ([bfb2765](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bfb2765c6143c96e6638b216e9cca61b4de68c5e)) by Michael Usachenko
* **perf:** enable mimalloc allocator for gkg-server and orbit CLI ([2960896](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/29608969d46718c81fabb8cee4a00794d60544da)) by Michael Angelo Rivera

### Fixes

* **dev:** support HTTPS and Unix socket GDK setups in native dev script ([e9730f5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e9730f5541a36cf2dc98f211c915c0ca7a267e10)) by Dmitry Gruzd
* **e2e:** simplify siphon setup and add smoke test ([20ca312](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/20ca31277885ce30ba4aab8454c6162ff05b3d93)) by Bohdan Parkhomchuk
* **neighbors:** center node properties not hydrated in neighbors query response ([edefe9f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/edefe9f19a973b9ca17bef1a4d501062c6ff3210)) by Michael Usachenko
* **query-engine:** harden GraphResponse serde and add cross-namespace neighbors test ([e40a810](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e40a8104e795db8826e5fdb11d30f66e038b9d5e)) by Michael Usachenko
* **server:** reject queries from users with no enabled namespaces ([cb7f4f2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cb7f4f2549d0b2d3814ce1a1ebf4bd0066ce8e27)) by Michael Angelo Rivera
* **testkit:** generate graph DDL with SCHEMA_VERSION prefix ([3e71ea2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3e71ea245dacbb99854328acb7e2e229758bab3b)) by Michael Angelo Rivera

### Other

* **config:** fix stale references and complete config documentation ([581d019](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/581d019bbc3b2dbf903c420260ae8368edca9cae)) by Michael Angelo Rivera
* **deps:** remove 21 unused dependencies ([56bbdf9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/56bbdf9567c8f36b6f6a5f12b87f2bd6e177364e)) by Michael Angelo Rivera
* **dev:** remove in-repo helm charts and e2e test harness ([2a7526e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2a7526eb95d5dd7f5515859e402c351196c35871)) by Michael Angelo Rivera
* **dev:** remove Tilt local development path ([8851f1c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8851f1c97bcfda6473817b027f5016ccd8ae3649)) by Michael Angelo Rivera
* establish fuzzing framework ([4253f48](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4253f485f45aab716c8c33b7f84513cb19742803)) by Gus Gray

## [0.26.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.26.0...v0.26.1) (2026-04-13)

### Fixes

* **server:** skip schema table creation in webserver mode ([f813ea1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f813ea18a51023e59ffb01e3075e7947d40487ca)) by Bohdan Parkhomchuk

## [0.26.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.25.0...v0.26.0) (2026-04-13)

### Features

* **benchmark:** add code indexing benchmark CI pipeline ([df0562c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df0562c265554caa0af27290f229e67f63394f27)) by Michael Angelo Rivera
* **benchmark:** per-language scenarios with per-repo timing ([ef92ab4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ef92ab4e52ca4efda647b583ce461de1b3596270)) by Michael Angelo Rivera
* **ci:** add semver tagging for dev images ([5b2c103](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5b2c10333163ee208cd02d28db776677c5a2382b)) by Bohdan Parkhomchuk
* **compiler:** add DDL AST and ClickHouse DDL codegen ([272afec](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/272afecc1fa5592860552142bc4c71b71643494c)) by Michael Usachenko
* **compiler:** extend AST and codegen for schema version statements ([3211e61](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3211e6180342374ae3dcd3a80c80c489df810532)) by Michael Usachenko
* **health-check:** multi-namespace targets with StatefulSet support ([43eac29](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/43eac29a5cf0dd165f05ec4be3631f7fecc71cae)) by Bohdan Parkhomchuk
* **indexer:** add migration completion detection and table cleanup ([f77dee1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f77dee12a685295b0b82266a5b94b0ef88d932ac)) by Dmitry Gruzd
* **indexer:** add schema version tracking with table prefix support ([2285c82](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2285c8243687c3f4cc8b9b2be01fa8e5e5dd2701)) by Dmitry Gruzd
* **indexer:** add table-prefix-aware migration orchestrator ([5f576dc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5f576dcdcfa04cd2e095b3141053093957327b4c)) by Dmitry Gruzd
* **mcp:** add format argument to tool parameter schemas ([e838f4b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e838f4b47ad9e40e82846ab68b64a64a761d07a2)) by Michael Angelo Rivera
* **ontology:** inject schema version table prefix at load time ([8217061](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8217061a4f25d22d9510abd0f77788f23e7f4e78)) by Michael Usachenko
* **ontology:** ontology-driven DDL generation with 1:1 graph.sql match ([1b43a07](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1b43a07176145cd3398d1896600bb51cedcbcfb3)) by Michael Usachenko

### Fixes

* **ci:** fall back to direct registry pulls in security fork ([76427c7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/76427c7a42471dc57b7a3e425a1ac0aef33eea92)) by John Skarbek
* **code-graph:** add stacker guard to Java resolve_expression ([b2273b3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b2273b3893d92b92c94c0cdd5d582385d055dbaa)) by Michael Angelo Rivera
* **compiler:** use correct FINAL placement for ClickHouse 26.2 ([5464620](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/54646202941a23beea9901d1806f2524444eab2b)) by Michael Angelo Rivera
* **helm:** camelCase statefulSets in local values ([79f3f84](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/79f3f84c933e28e5e7c4e4eb95a9e3412d968200)) by Bohdan Parkhomchuk
* **ontology:** rename person-action edges to active-verb form ([83eb66c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/83eb66cdf9a5851a3605f46405dc3270fa3653d2)) by Lyle Kozloff
* **parser:** guard against tree-sitter-python infinite recursion on missing colon ([293b0d2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/293b0d20fd9abd7f7e164234c48190fa990e0712)) by Michael Angelo Rivera
* **parser:** harden Python and Ruby stack handling ([920bea3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/920bea3cbbae6b366a0a343a8547e28a8dee71ec)) by Michael Angelo Rivera

### Other

* add query Orbit docs ([f119bbf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f119bbfe16c9080f31519e2cf86bfc1d6c40a20c)) by Phillip Wells
* **deps:** update rust crate hmac to 0.13.0 ([a8ab7a4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a8ab7a486da32b5a0fe3936aab7699f3a6786eca)) by GitLab Renovate Bot
* **helm:** bump charts, remove dispatcher patch ([4a76cfe](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4a76cfe117c836158a31a95c08796999f9253ea9)) by Bohdan Parkhomchuk

## [0.25.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.24.0...v0.25.0) (2026-04-10)

### Features

* add mise dev task for GDK-connected local development ([44be42c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/44be42c108dc2557f94fa966be2e7f9dc9862076)) by Dmitry Gruzd
* **auth:** accept source_type JWT claim and trace it ([a533e0e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a533e0ee40e8de4e03941f5feb431907bebeaf84)) by Michael Angelo Rivera
* **indexer:** dispatcher health endpoints ([f3e92f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f3e92f98e39a2c1e17afdb4ce914a2bb9cebea69)) by Bohdan Parkhomchuk
* **schema:** add node_edge_counts projection to gl_edge ([ade976c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ade976c860751ee6d2b5cf8192361eb9587ba64a)) by Michael Usachenko

### Other

* **deps:** update rust crate async-nats to 0.47.0 ([81f5b6e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/81f5b6ec0c078fe44c64d5b9f8df83486ab42a70)) by GitLab Renovate Bot
* **deps:** update rust crate indexmap to v2.13.1 ([74ff087](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/74ff08773fefc231bb6a399be95f94cf748e2a92)) by GitLab Renovate Bot
* **deps:** update rust crate uuid to v1.23.0 ([6fbd6b3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6fbd6b32ace69509f408d488bbeb1ad5903bd9e2)) by GitLab Renovate Bot

## [0.24.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.23.0...v0.24.0) (2026-04-10)

### Features

* **indexer:** loop-based dispatcher with cron scheduling ([4ce265f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4ce265fbd4a9185c7c020a9f395cde930da8630a)) by Bohdan Parkhomchuk
* **orbit-cli:** branch, commit SHA, and worktree tracking in local code indexer ([fa718d4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fa718d457d8edaee1419119890ff82636580e33e)) by Michael Usachenko

### Fixes

* make AsRecordBatch generic over context type ([58c4fe9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/58c4fe9eeb984b12289cf70bc3cb931bf0d4571c)) by Michael Usachenko
* **orbit-cli:** discover nested repos when workspace root is a git repo ([0cd62c2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0cd62c2a1016a2ebc14402748fe2e0d5d7ebfe2e)) by Michael Angelo Rivera

### Other

* bump rust to 1.94.0 ([f2c75a6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f2c75a63d1a47512ccb5cb6901ee02771c09daf4)) by michaelusa
* **cli:** code indexing cli integration tests ([692b83b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/692b83b39022d438b82dbf1ce79da4fcdaf9b1c4)) by Michael Usachenko

## [0.23.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.22.0...v0.23.0) (2026-04-09)

### Features

* add GitLab connectivity probe to webserver /ready endpoint ([322c34d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/322c34d3d92251ce78c7658ea1d748e84c6dcc4f)) by Michael Usachenko
* **cli:** local DuckDB query pipeline with proper stage architecture ([bfe7497](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bfe74970d5688570a056a4e8689aba86ffd86217)) by Michael Usachenko
* **cli:** local filesystem content resolver for virtual columns ([074a3db](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/074a3db351e422b790995f4ef531648a04cf2e6c)) by Michael Usachenko
* **cli:** local hydration with DuckDB execution ([7f2ba99](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7f2ba99fc3d2fbeee516a7a496870f4e55354e00)) by Michael Usachenko
* **cli:** persist graph index to DuckDB ([97b9212](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/97b9212798631433fc22af7e7fb1318dcc587638)) by Michael Angelo Rivera
* **code-graph:** add as-record-batch trait for code graph node types ([ede7fe2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ede7fe278306c73aa4a1ee35563091e1c762c890)) by Michael Usachenko
* **ontology:** add local_entities and local_exclude_properties settings ([dd18dae](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dd18daedf265cdc16dec0d719de7b76748e1bc27)) by Michael Usachenko
* **ontology:** add name field to local_db edge table config ([b544b52](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b544b52aa5bb3a00c35d04b4f0a6b5881097f939)) by Michael Usachenko
* **query-engine:** duckDB pipeline with enforce pass and PipelineOutput::from_batches ([d584979](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d584979c258c988cd4b1d2279242be2ad4775795)) by Michael Usachenko
* **server:** query stats from X-ClickHouse-Summary + query pipeline dashboard ([9408e27](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9408e273077b4d867caa5ef0185b9e8212ae641f)) by Michael Usachenko
* **utils:** generic NodeBatch builder for Arrow RecordBatch construction ([ff96956](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ff96956cb60cbe666c296f6792cbcf26f5e3d44e)) by Michael Usachenko

### Fixes

* **ci:** gate canonical-only jobs for security fork support ([7d6ce1b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7d6ce1b21e75b6cdcdaaa9a09d494b6811d39822)) by Michael Angelo Rivera
* **ci:** set git remote URL with Vault token ([7ab6bd4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7ab6bd4ad704f51af886fe5c92195cb2b40f68e1)) by Bohdan Parkhomchuk
* **compiler:** add path tie-breaker to path finding ORDER BY ([22179b0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/22179b064919faa28ee68606b549cf1301da2885)) by Michael Usachenko
* **compiler:** deterministic cursor ordering and path finding entity filter ([62a24a4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/62a24a48ab680281b390561a35dd265248961924)) by Michael Usachenko
* deterministic path finding cursor ordering leading to flaky tests ([6c16c45](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6c16c459bc4b1d18ded309fe65eb844fe54b860d)) by Michael Usachenko
* **indexer:** use batch() instead of fetch() for filtered consumers ([14a2810](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/14a2810a3c32e4ec41149f2ad1baa630975eb024)) by Bohdan Parkhomchuk
* **ontology:** standardize APPROVED_BY edge to User-is-source direction ([164ca52](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/164ca5290ea9aeb19089a87f8f06531d6bdad068)) by Lyle Kozloff
* require GitLab client config at server startup ([36390fa](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/36390faf9181d8f57fb861ebf0275b5861dd8064)) by Michael Usachenko
* **server:** sanitize error messages and stop leaking compiled SQL ([ea35c44](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ea35c44d2970cc023d3ea9eeceb169f0de3d3e15)) by Michael Usachenko
* **tests:** run dedup tests in forked databases for isolation ([91c7925](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/91c79250e54ce59be844490c55fefe94b86769db)) by Michael Usachenko
* **test:** stabilize flaky cursor pagination determinism test ([0f0377d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0f0377df0c8ce297b7508ea246093a24c47870a8)) by Dmitry Gruzd
* virtual column resolution for all query types ([e4fa2e6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e4fa2e6121155d44b0948e7d0b7c8c41529fd5f7)) by Michael Usachenko

### Other

* **adr:** add workhorse query pattern (ADR 008) ([d2e77a0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d2e77a08c8e35ebf530a8577980320af57d7d2f1)) by Michael Angelo Rivera
* **cli:** write lock retry, read-only queries, manifest in DuckDB ([7fca313](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7fca313b592921d9b35e504a29fc76beaf038571)) by Michael Usachenko
* **compiler:** add query_cache_share_between_users setting ([a741cac](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a741cacece56f0a5aa65682e5cbd837885db7e9c)) by Michael Usachenko
* data sources documentation ([05d162c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/05d162c24c470a637ead40e5473e15f2cb51d257)) by Phillip Wells
* **deps:** bump clickhouse crate from 0.14 to 0.15 ([2a0beb4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2a0beb4113f3bb7fe7fba4a5464b6eaceece1450)) by Michael Usachenko
* fix minor typos in design docs ([10d67c0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/10d67c026acb0974920c113ed43b08908baf14cb)) by Bob Singh
* **mise:** add vendir and yq ([5936da8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5936da8323aaf6ac5dcccb1d58670678f72f3b29)) by Bohdan Parkhomchuk
* **testing:** integration tests for GitalyContentService::resolve_batch ([f0eec8d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f0eec8dd357b3e9f275f1ef1ea5f5b7318980266)) by Michael Usachenko

## [0.22.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.21.0...v0.22.0) (2026-04-07)

### Features

* **ontology:** support per-edge destination_table for multiple edge tables ([5638a41](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5638a41c77a3162f5be0ba2eaec36b739ccfffac)) by Michael Usachenko
* **testkit:** add assert_all_edge_types_covered for neighbors edge completeness ([beb423f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/beb423fa621472d7c6db48fd0759ce8e4df3fdcd)) by Andrew Dunn

### Fixes

* **ci:** use Vault for semantic-release auth ([d431f39](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d431f3921d30e94e70197313c083bb928e27547c)) by Bohdan Parkhomchuk
* **compiler:** add _deleted filter for edge table scans ([c094da8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c094da8ec465d231645425f2e72f58c971b07a62)) by Michael Usachenko
* **compiler:** apply argMax dedup to hydration queries ([b73fe58](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b73fe584e583523800658dbdd625631100f22dc3)) by Michael Usachenko
* **compiler:** only push sort-key filters inside LIMIT BY dedup subquery ([8f6cacb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8f6cacb8eca855bbed5261dbc29e2e3d3b7ca360)) by Michael Usachenko
* **docker:** exclude release-only files from build context ([e5ba8c0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e5ba8c0eef5a30f5673bf36c82f5ebb602942bcf)) by Bohdan Parkhomchuk
* **docker:** restore GKG_VERSION in runtime stage ([91fea2b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/91fea2b0e1146be4b005ac0d90cd856844c074dc)) by Bohdan Parkhomchuk
* **indexer:** make consume_pending fetch timeout configurable ([87bc6dd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/87bc6dd484e21b424352ac32dc40adfc31b0470a)) by Bohdan Parkhomchuk
* **indexer:** skip PAX header entries during tar archive extraction ([ce7f6a8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ce7f6a86c1c3148d6397cbd41c0dc1795e412d7f)) by Michael Usachenko
* **perf:** use argMax dedup for _nf_* CTEs instead of LIMIT BY ([7e0582f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7e0582f94e2d2768fcbeaba3df6dc1551a9e4552)) by Michael Usachenko
* **profiler:** query system tables via clusterAllReplicas ([bdb7477](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bdb74772413c7443430da410cd5e3888aac61f5e)) by Michael Usachenko
* **testkit:** fork copies seed data and run_subtests reports failures by name ([5ee4476](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5ee447668b4dc4c990f9e9db861a4ce5a493a513)) by Michael Usachenko

### Other

* add troubleshooting page ([215030f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/215030fb2636a5f378630254a89d65ff533dcf62)) by Phillip Wells
* **dedup:** additional correctness coverage for hydration, edges, and multi-version ([679c64d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/679c64dc433b030fe6cbc69c02c3f3fdffcbad94)) by Michael Usachenko

## [0.21.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.20.0...v0.21.0) (2026-04-03)

### Features

* **config:** generate JSON schema for server config ([24cda9e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/24cda9e91e3e9d8c984b7a2820319e636c6040fe)) by Bohdan Parkhomchuk
* **docker:** optimize build for better layer caching ([e19fc06](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e19fc065e741dc70bbdfeeb352e793221f0d8e0c)) by Bohdan Parkhomchuk
* **ontology:** add IN_PROJECT, CLOSED_BY, APPROVED_BY edges and fix REVIEWER source ([89d4eb1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/89d4eb18e34484fad90b422af7dc9825aec8efcf)) by Michael Angelo Rivera
* **server:** tune tonic gRPC HTTP/2 settings for production ([e30a8d0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e30a8d081998af252feb8afd489f9225be2ac56c)) by Michael Angelo Rivera
* **server:** type-safe ClickHouse settings and log_comment tracing ([0848f5b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0848f5b6d8d7d2b4877ea6c4534cd5ce24549da9)) by Michael Usachenko
* **server:** wire Gitaly content resolution end-to-end ([c08d0b9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c08d0b96654ecdfda701ad08701aa1b3017057cc)) by Michael Usachenko

### Fixes

* **config:** fix handler config deserialization and drop unused env var source ([7c8b6e9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7c8b6e906cf5bb32a5afee64d05602e3393e9459)) by Michael Usachenko
* **ontology:** compute full_path from routes table instead of slug ([2d784f8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2d784f8ede2da4a7a63c5559fa7e493d0dd18d36)) by Michael Angelo Rivera
* **ontology:** standardize person-action edges to person-is-source direction ([ad8cf90](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ad8cf900d51a44be90f1b9fd667ebe9851a07f4a)) by Michael Angelo Rivera
* **tests:** fix integration test failures from edge direction change and stale cache ([deb3bf0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/deb3bf0dad43867527730cd371b25c5b3af569b1)) by Michael Usachenko

### Other

* add MIT license and go.mod to gkgpb Go submodule ([e165a69](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e165a6955adf13266b1aad929e6851de7c5b93ca)) by Michael Angelo Rivera
* add MR issue-link guidance ([4202298](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/420229853a89ca16690646ddc80968ea52b35af6)) by Dmitry Gruzd
* **config:** load query settings from YAML via gkg-server-config crate ([e49e797](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e49e797aa090bb702fbc762426d32746cc3f8b8c)) by Michael Usachenko
* **config:** remove config re-export shims ([c6af4f0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c6af4f082e283cf45f0dd963fdcc396dcacca7b9)) by Michael Usachenko
* **config:** unify all config types in gkg-server-config crate ([5fedf4e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5fedf4ef9d233ba4d5bd11900570c481f5384368)) by Michael Usachenko
* **server:** rename config.rs to tls.rs ([f507fe5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f507fe5e42e4f29b32e0a9ad5185dab0dd6805a2)) by Michael Usachenko
* **server:** replace QueryProfiler with log_comment-based system table queries ([f5f52bf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f5f52bfd664dddf28baf027cc13bb3bd78a0202d)) by Michael Usachenko
* tag gkgpb Go submodule on release ([8758034](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/87580344056dad0153ebf9a33bba03f3bf357741)) by Michael Angelo Rivera
* update design document to reflect current codebase ([4251b16](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4251b1669214d863dc2a148b223b73d4049bd642)) by Dmitry Gruzd

## [0.20.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.19.0...v0.20.0) (2026-04-01)

### Features

* **indexer:** add mTLS support for NATS client ([fcca027](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fcca027c9ca69a5edd90f655ba06a013a0ff1015)) by Bohdan Parkhomchuk

## [0.19.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.18.0...v0.19.0) (2026-04-01)

### Features

* **proto:** publish Go protobuf stubs for workhorse integration ([b328e96](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b328e96b5e9b54fd3ccda4b2a700ffdd2f310173)) by Michael Angelo Rivera
* **utils:** add ColumnValue::coerce<T> for typed extraction ([2cfd81f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2cfd81f8b4b17eca954704bb5faf6b7862801e4d)) by Michael Usachenko

### Fixes

* **ontology:** use derived tables to prevent JOIN OOM in Group/Project ETL ([47af2ac](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/47af2acf51b2434ac8a31fe3e5bf0ec3ab4c6ecb)) by Michael Angelo Rivera
* **proto:** regenerate Go stubs from current proto ([277f095](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/277f09527a5fe73d21a565a9c347526ce45a18b8)) by Michael Angelo Rivera

### Other

* **code-graph:** reorganize into parent crate with sub-crates ([14a3a63](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/14a3a63ec7fa066d2cdea2199fc4b705e431cbf5)) by Michael Usachenko

## [0.18.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.17.0...v0.18.0) (2026-03-30)

### Features

* **ci:** enable secret detection scanning ([c915c9a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c915c9a6859cdee3331eeb07f864d9faa26209ff)) by Lyle Kozloff
* **compiler:** add params_in_order() for positional param binding ([75c3afa](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/75c3afaaf541c2b8150e22e7c834ae8ac4f3ed66)) by Michael Angelo Rivera
* **compiler:** edge-only aggregation lowering and CTE dedup ([e8df6a1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e8df6a1b399a867e15dcf00954bc40a0dc5e794d)) by Michael Usachenko
* **compiler:** enable ClickHouse query cache for cursor pagination ([e0c3eab](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e0c3eab1596e150f0590cf1cd596c8e76d0f9b44)) by Michael Usachenko
* **compiler:** entity_kind filter injection and optimizer cleanup ([012c6dd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/012c6dd0e37485430addc14c3a975c9e42569ad8)) by Michael Usachenko
* **compiler:** rewrite neighbors to edge-only pattern ([9614686](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/961468639b862289f6914e40e41bb3cc82faebad)) by Michael Usachenko
* **compiler:** row deduplication for node tables using LIMIT BY + argMax ([7207828](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/720782868e65d6c65ae2be5b625af30358b5f8f5)) by Michael Usachenko
* **duckdb:** add duckdb-client crate with schema and Arrow inserts ([5b01b40](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5b01b40bb2742a9f7d5234655181c44efbb3f396)) by Michael Angelo Rivera
* **formatters:** add columns descriptor and fix ungrouped aggregation formatting ([f66bcd2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f66bcd296356b7e0014a9672cdefcac3724371ca)) by Michael Usachenko
* **indexer:** store commit_sha on code graph entities ([f22c270](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f22c270067ed420b5714cff7eccddd00a9bef174)) by Michael Usachenko
* **ontology:** add depends_on to virtual fields and enable content columns ([05837a7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/05837a7c49b76a094750943ca30ad83288c4285f)) by Michael Usachenko
* **profiler:** query diff tool, mkdir -p for output, skill docs ([83fbc1e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/83fbc1e62fe1abae13c980267df5fda86e52c652)) by Michael Usachenko
* **profiler:** support multi-query files with --filter ([aa4e6b0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/aa4e6b004da4b4201354685ecafd889c1d94c36a)) by Michael Usachenko
* **schema:** add by_id projections to all node tables and status/state projections ([3b9ad1c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3b9ad1caee4453e7d363c8321528cdebbe2d49a3)) by Michael Usachenko
* **server:** add VirtualService trait and wire virtual column resolution ([6aec06e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6aec06eabf5803a9c247544727c1300df2241188)) by Michael Usachenko

### Fixes

* **ci:** exclude duckdb-client from --all-features to avoid bundled C++ build ([4591ed5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4591ed57ba4d302f4aa912c08a05f87cb19c33bb)) by Michael Usachenko
* **ci:** switch duckdb from bundled C++ to prebuilt [secure] library ([e196d74](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e196d74c4e94165cfda3f6a1c877f3dd80099a3f)) by Michael Usachenko
* **compiler:** batch hardening fixes ([e75cf39](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e75cf3980f09fc2279866d8b53c1c81cde31d6c2)) by Michael Usachenko
* **compiler:** harden LIKE filter operators + filterable columns ([da114f7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/da114f7d1fdff0d1d56e2345f8c1057d745374a7)) by Michael Usachenko
* **docker:** build workspace with duckdb-client/bundled for forward compatibility ([93d23d6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/93d23d69a1900754a74e2c35c633e0a5ecaefa7b)) by Michael Usachenko
* **docker:** exclude test crates from workspace build ([85974e7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/85974e7b250b1d109e32ff7427a8609fe12018eb)) by Michael Usachenko
* **grpc:** propagate correlation ID into spawned execute_query task ([e19b18d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e19b18d2407890b905f46236327e576a7007136d)) by Michael Angelo Rivera
* **indexer:** strip Gitaly archive root during tar extraction ([e79a165](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e79a165f79d6122d381bb9d449dcf2301516b837)) by Michael Usachenko
* **indexer:** use datalake client for project traversal paths query ([828a560](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/828a56025d32249cb79499a7fb1902c12c6d7a9c)) by Bohdan Parkhomchuk
* **ontology:** add due_date and start_date to milestone default columns ([53196b8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/53196b822097b76cbf9ce13af9a8c8ff5e65f307)) by Michael Angelo Rivera
* **server:** use primary key column for static hydration + add regression tests ([06f3e3d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/06f3e3d72cb2dee810ef58001f9db3224fa98d35)) by Michael Usachenko

### Other

* add hugo build and review app jobs ([a51d2a1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a51d2a1227d17c5b084391294f0e69633f5bb002)) by Phillip Wells
* add lgtm-agent automated MR review component ([09a4f65](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/09a4f65b141b7d73638009401888cae14fca2168)) by Dmitry Gruzd
* **code-graph:** extract edge_kind and source_target_kinds methods ([7b3a506](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7b3a5063f76e4e43950dfdb1c1a9a046489f3344)) by Michael Angelo Rivera
* **compiler:** dead code removal and deduplication ([c7af276](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c7af27645fffcd64a09686d0b8422bd25e20d139)) by Michael Usachenko
* **compiler:** derive skip_security_filter_for_tables from ontology ([39ab045](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/39ab04538830def6766a5c4159ede4f1694d64b0)) by Michael Usachenko
* **compiler:** hydration plan cleanup and virtual property support ([2cc4f95](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2cc4f9586303be9093e5000f0cff5f1dbf53825e)) by Michael Usachenko
* **compiler:** remove traversal_path from edge SELECT expressions ([36bd4d2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/36bd4d2843389f39754d68708388217943d2e374)) by Michael Angelo Rivera
* **duo:** add Duo Agent Platform flow execution config ([7160803](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7160803661cbf72eb65b8fe50f3ea92c0f9ed282)) by Michael Angelo Rivera
* **ontology:** load internal_column_prefix from ontology YAML ([df945bb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df945bb068698a66836b8d23d77a55c95edded3e)) by Michael Usachenko
* **proto:** remove unused PaginationInfo from QueryMetadata ([a378137](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a3781371939f37ee5223d4188882fb2ea44ba2ef)) by Michael Usachenko
* **test:** auto-discover non-Docker test targets ([6acd976](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6acd976599dc36675e3a1014ffff9da9545c5a34)) by Michael Usachenko

## [0.17.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.16.0...v0.17.0) (2026-03-26)

### Features

* **compiler:** add composable, type-safe compiler pipeline scaffolding ([0cce46d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0cce46d037039eadff445fb45b3a7e55d03368aa)) by Michael Usachenko
* **compiler:** add DuckDB codegen backend and local compilation pipeline ([e05f8f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e05f8f913454471b0eb954b7c84eb7d99e9c9ffd)) by Michael Usachenko
* **compiler:** replace keyset cursor with agent-driven pagination ([091d761](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/091d7617d97879e9a0f224e186d42ef1f52d0215)) by Michael Usachenko
* **indexer:** leverage traversal_path projection in Group and Project ETL ([856e0d3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/856e0d310b1e2eacfd65d8f9a11f52cceb7c9e21)) by Jean-Gabriel Doyon
* migrate work items from hierarchy_work_items to work_items table ([0ca40dd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0ca40dd82b5e84ce5f79733ce31ab1d0c4b33c48)) by Jean-Gabriel Doyon PTO until 2024-04-17
* **pagination:** surface cursor data to proto, and raw + llm formatters ([274637f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/274637f5b79f1c1f6773b97138fdf4960a335f82)) by Michael Usachenko
* **query-engine:** wire pipeline presets into compile() and compile_input() ([21312c7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/21312c70388e9d8a9f4e528a63ced8bff7ea07d4)) by Michael Usachenko
* **schema:** add aggregation projection to gl_edge and by_id to gl_stage/gl_finding ([968d06b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/968d06b311594b77897d061e613dfa0a8bbe34e3)) by Michael Angelo Rivera
* **server:** add optional TLS for gRPC ([457a044](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/457a04467a3746f371c70cbd0bb866f05da323d9)) by Bohdan Parkhomchuk
* **testing:** sqlparser-based test assertions in compiler integration tests ([49e003d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/49e003d83501d7076e58f42a076b7a2c8395f8fd)) by Michael Usachenko

### Fixes

* **build:** unbreak CI docker build ([1433ba8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1433ba8064495ee04a373810394d2e607ec34292)) by Bohdan Parkhomchuk
* **compiler:** emit correct redaction columns for non-default id entities ([d62ac2b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d62ac2b12bc8d7744605340474f92d168883d4e9)) by Michael Angelo Rivera
* **compiler:** join node table when order_by references non-id property ([da4dd8d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/da4dd8df457dd299048ee2c76f53d4c830a1cbc6)) by Michael Angelo Rivera
* **ontology:** read traversal_path from file table in MergeRequestDiffFile ETL ([5b51617](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5b51617b5cbf2ba404e97ebb94e2f0812567957b)) by Jean-Gabriel Doyon PTO until 2024-04-17
* **tilt:** auto-sync vendored helm chart and bump fd limit ([6a90329](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6a90329854ebf1f17bc27f463346b477da02d000)) by Bohdan Parkhomchuk

### Other

* add indexing and configuration runbooks ([e23efee](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e23efee625700bb265bcef03a867a67b523e4103)) by Jean-Gabriel Doyon PTO until 2024-04-17
* add Orbit landing page ([55e7b1a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/55e7b1a777c227df90f00a570167f9080c58d0ea)) by Phillip Wells
* **build:** simplify build-dev.sh ([8970980](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/89709800d21f88cdd4f89b3323891e5a59d566c6)) by Bohdan Parkhomchuk
* **ci:** drop helm-lint and fix tag release jobs ([1f89d80](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1f89d806df0f475ac11415990e772abec9767215)) by Bohdan Parkhomchuk
* **compiler:** consolidate _gkg_* column emission in enforce pass ([7a416af](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7a416af8a7c390cacf278239632a937db4adff57)) by Michael Usachenko
* **compiler:** replace hardcoded strings with named constants ([6130bf9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6130bf999b15da70f7d39a731cf7662d411ba4db)) by Michael Usachenko
* **deps:** update rust crate arrow-ipc to v58.1.0 ([e8ecf38](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e8ecf3803b0c04dc9dbfc5808acd8634c4a1b897)) by GitLab Renovate Bot
* **deps:** update rust crate datafusion to v53 ([41a7e87](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/41a7e87f176c1c975504197915177bc581e33f02)) by GitLab Dependency Bot
* **deps:** update rust crate moka to v0.12.15 ([020deca](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/020deca8157e26fd328e9d1ddd0fada602ccf955)) by GitLab Renovate Bot
* **deps:** update rust crate toml to v1.1.0 ([6e93c25](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6e93c25a9bb57a44db42a394891fa4a32a626d00)) by GitLab Renovate Bot
* **helm:** replace helm-dev with vendored official charts ([aa9181a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/aa9181ac8c71feb2c7e21e5e37bf56e5ca4961a7)) by Bohdan Parkhomchuk
* **metrics:** standardize OTel metric names ([114425b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/114425be0262e9f070295bf17c2e9359718253ff)) by Bohdan Parkhomchuk
* remove datalake-generator crate ([d5f3fc3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d5f3fc33673c7fd45d58b140b7e909912d9b1c12)) by Jean-Gabriel Doyon
* **security:** traversal path scoping tests for search and path_finding ([8fe2c91](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8fe2c9169a69d9ae151caaf85e203e1222baa1f4)) by Michael Usachenko
* **tests:** extract compiler tests into standalone test files ([2981328](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/29813287d4262a15d26ca553d3badfda345909fb)) by Michael Usachenko
* **tests:** extract data correctness seeds to SQL file ([1a57768](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1a57768b49b4ccb3a4e98a5b5783bf9346cce582)) by Michael Usachenko

## [0.16.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.15.0...v0.16.0) (2026-03-23)

### Features

* **compiler:** unified edge-only traversal with multi-rel and multi-hop ([db49fae](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/db49faeb88298e020b336c50407649e69bc4d1da)) by Michael Usachenko
* **indexer:** add branch node to graph ([17caa8c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/17caa8c943e88f90a775f5a5481ef2818c8e6d94)) by Jean-Gabriel Doyon
* **indexer:** add incremental file fetching with rename detection ([0741ec9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0741ec9760db3563b8b8e89c1ec1aacb401211bf)) by Jean-Gabriel Doyon
* **indexer:** stream archive downloads to disk instead of buffering in memory ([d44af8b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d44af8bc5745bdc40c0a52d57b61ac10bff6be31)) by Jean-Gabriel Doyon
* **protos:** add gitaly-protos crate ([be4536f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/be4536fe3fd07deea6d19769cef3717c2e6eb70c)) by Jean-Gabriel Doyon
* **query:** add cascading SIP, neighbors UNION ALL, and path hop frontiers ([df7a970](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df7a9702c09d3cd94c8323c7333fdf58945ecadd)) by Michael Usachenko
* **query:** add ClickHouse query profiling and instrumentation ([c92a3ea](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c92a3ea6a27be96bea41efdfa178f9d8167fe13c)) by Michael Angelo Rivera
* **query:** consolidate dynamic hydration into single UNION ALL query ([165ea18](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/165ea1802516c97e2580e7e6d5c4afbbde187f55)) by Michael Angelo Rivera
* **schema:** namespace-first edge PK for traversal_path pruning ([6637a5c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6637a5ca47f19271447d7b87e29d2098cc13140f)) by Michael Angelo Rivera
* **scripts:** add run-dispatcher.sh for local dispatch-indexing mode ([cb18e34](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cb18e341b3063607f3bc83d44a0cbac315c38000)) by Jean-Gabriel Doyon
* **synth:** add evaluation settings override and optimization benchmark queries ([5767a1c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5767a1cfef0f4bf115654cf7eb6ffb2dee0ed8ed)) by Michael Usachenko

### Fixes

* **deps:** upgrade rustls-webpki ([9e01b18](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9e01b18bc7136c4d11c83f531ab1a8b7b88e0d8c)) by Jean-Gabriel Doyon
* **profiler:** forward query settings to EXPLAIN queries ([5c3d53a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5c3d53a386b6fa1cd62396b14bc8f6b533efa7cf)) by Michael Usachenko
* **query:** extract keyset pagination so it applies to search queries ([9a4e154](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9a4e1543272f776bad9af4a9486e777ead92d20c)) by Michael Usachenko

### Performance

* **compiler:** skip node table join for edge-only count aggregations ([f2a65a5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f2a65a55ee37e03afb2d70ffab24fab653652bbd)) by Michael Usachenko
* **schema:** add by_rel projection to gl_edge ([58f0c7e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/58f0c7ee44632b44956c0f7bf2a2b04bd7f7e6c6)) by Michael Usachenko
* **schema:** add source_kind to by_rel projection ([c2892a7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c2892a7d5f11ba8a404282cf5c95c7f78b43d6d5)) by Michael Usachenko
* **schema:** rename by_rel to by_rel_source_kind and add by_rel_target_kind projection ([4c14281](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4c14281f0b40bd625b53c1f0817562d5f020ee9e)) by michaelusa

### Other

* **build:** add manual MR docker build job ([b385332](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b385332cb1f4b8fbe677ac39921ddd81f1bae647)) by Bohdan Parkhomchuk
* **ci:** remove docs linting from lefthook pre-commit ([e9f8d1b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e9f8d1b041c920aaf25ad792777e24b4b2beb5bb)) by michaelangeloio
* **compiler:** edge-centric traversal with cascading SIP ([713e5c6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/713e5c6bd205602b8e2b5a238115df03eb86fa3f)) by Michael Usachenko

## [0.15.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.14.0...v0.15.0) (2026-03-20)

### Features

* **metrics:** bump labkit-rs, add prometheus support ([4c2162c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4c2162c5358737b5c83b611a2a2f588263c83f7a)) by Bohdan Parkhomchuk

### Fixes

* **ci:** auto-retry release jobs on runner_system_failure ([fb82d3a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fb82d3aba7bc9d137a311fa07ab1927c2d6a5335)) by Bohdan Parkhomchuk
* **query:** add target-side SIP for aggregations ([45a0d51](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/45a0d515a2a662c6d63ba0fe70acb48fe8fde980)) by Michael Usachenko
* **xtask:** align simulator schema with graph.sql and fix association edge paths ([8bdcfd3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8bdcfd30697050862da548ca97769630b950b2d6)) by Michael Usachenko

### Performance

* **schema:** apply PK compression codecs and reduce index granularity ([07346cd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/07346cd3d5bc359b76d7dd0092c0b8d142b19059)) by Michael Usachenko

## [0.14.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.13.0...v0.14.0) (2026-03-20)

### Features

* **indexer:** add RepositoryCache and RepositoryResolver for full downloads ([5439def](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5439def89511ce31b676168c6f1c994e5b7b473e)) by Jean-Gabriel Doyon
* **query-engine:** add SIP pre-filtering and enable keyset pagination ([fee802b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fee802bdace7c84e95a778ff2453628c59d0a808)) by Michael Usachenko
* **schema:** add compression codecs to graph table columns ([854a94b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/854a94bbe537a22cebf38d8ae9192290e47b19fb)) by Jean-Gabriel Doyon
* **schema:** add skip indexes on frequently filtered columns and all booleans ([e8f3098](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e8f3098bdadf0a533e6761024e04d0e8fbc753c8)) by Michael Usachenko
* **schema:** apply LowCardinality to low-cardinality string columns ([4425f9f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4425f9f7214bf095142a113111976bb6c193c8fc)) by Michael Usachenko
* **schema:** reorder edge table PK and replace projections ([249c7b7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/249c7b7d484cd695c95317e62e885d39c3a07e5a)) by Michael Usachenko
* **schema:** replace id_lookup projections with bloom_filter skip indexes ([f268dc8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f268dc8b757ac9b571fcefdf2b65da6ebcff089f)) by Michael Usachenko

### Fixes

* **deps:** upgrade aws-lc-sys to 0.39.0 for RUSTSEC-2026-0044/0048 ([85122d0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/85122d0ca57f423ba44a96edd9e8b279cbcd2de6)) by Michael Usachenko
* **integration-testkit:** report failing subtest name on panic ([6c1ff32](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6c1ff324485dd38b3d7f579961e5bfbaf203ed3b)) by Michael Usachenko

## [0.13.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.12.0...v0.13.0) (2026-03-20)

### Features

* **config:** add configurable log level ([dc015f9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dc015f95c30a5d81452de90311c5a72a1bc6c74c)) by Bohdan Parkhomchuk
* **pipeline:** hoist up query pipeline as its own crate + decouple formatting ([7c8ee23](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7c8ee23a000fbeec3973b19ac20f55935b0c18d4)) by Michael Usachenko
* **query-engine:** add query observability, tune ch client settings ([95dbd84](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/95dbd8499cec55dcb8c84c218f5ccc14966b2750)) by Michael Usachenko
* **server:** add GetGraphStats gRPC endpoint ([693ae7a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/693ae7ad9582b142e55b0dc14e61ae829ddad5d2)) by Jean-Gabriel Doyon
* **tooling:** add ast-grep skill for structural code search and rewrite ([cd21bee](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cd21beedce7b44f5a953c7067c5163a30c7f1822)) by Michael Usachenko

### Fixes

* **indexer:** add `_deleted` column to code indexing Arrow batches ([edead02](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/edead02fe289ce7ff51adf5de79bc882b4060411)) by Jean-Gabriel Doyon
* **lint:** exclude .opencode, .sessions, and .dev folders from Vale scanning ([a13c70a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a13c70a6f1cfe007a50bc4f1cd3c985f35317fb8)) by Michael Usachenko
* **query-engine:** bound all unbounded query DSL fields ([2e8d00e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2e8d00e576f70dca33f6cca7c98caafc73495abd)) by Michael Usachenko
* **test:** update telemetry integration test for pipeline module rename ([d118aa3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d118aa331d6489e81b264dee96a467140f90d997)) by Jean-Gabriel Doyon

### Performance

* **schema:** use LowCardinality and set index on gl_edge ([4483068](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/448306818170c03689df1a4397e849de62968269)) by Michael Usachenko

### Other

* **deps:** update rust crate cliclack to 0.5.0 ([091de67](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/091de67dcad6362c4b7cc71616350902f1f7e77f)) by GitLab Renovate Bot
* **deps:** update rust crate tar to v0.4.45 ([5fb164d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5fb164d1554f8e47d08cdd8beed018ad8b79b4ad)) by GitLab Renovate Bot
* **observability:** migrate to official labkit-rs ([2c8e02b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2c8e02b12eb6e45a96ff2b26b88311956f2da41a)) by Bohdan Parkhomchuk
* **query-engine:** restructure into parent crate with subcrates ([dd7ad8a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dd7ad8afe17a7938fe9206b3dd74f76a08140480)) by Michael Usachenko

## [0.12.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.11.0...v0.12.0) (2026-03-18)

### Features

* **indexer:** add dead letter queue for exhausted retry messages ([bbc5aac](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bbc5aac2226c8a48ad81ae667bfebf6d2a22e656)) by Jean-Gabriel Doyon
* **indexer:** add NATS progress pings to code indexing pipeline ([fdf4c78](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fdf4c78cf8fed49006a6c84e924757181ff54ebf)) by Jean-Gabriel Doyon
* **indexer:** consume code indexing tasks instead of push events ([d33be50](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d33be509a562bb30974d9b96c4e2e4c012c12b24)) by Jean-Gabriel Doyon
* **indexer:** decouple code indexing dispatch from handler via NATS ([77586ca](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/77586ca8d696c3ab5b06b5ea774f07992607a8d8)) by Jean-Gabriel Doyon
* **indexer:** replace polling code reconciliation with event-driven backfill ([8575aa4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8575aa42f28cc4fca407f9cbc475602720ee478c)) by Jean-Gabriel Doyon
* **indexer:** send in-progress acks to prevent NATS message redelivery ([cd3da8d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cd3da8d3d046240daaa2ff0d21d7ac2be274742d)) by Jean-Gabriel Doyon
* **ontology:** edge schema from YAML not Rust ([fbe610b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fbe610b068b949f18256601d4874c46a852bebc4)) by Adam Mulvany
* **testing:** fix intermittent flakiness in concurrent integration tests ([41ab911](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/41ab91188dfb61e0d24850386ff58559c2250ce7)) by Michael Usachenko

### Fixes

* **ci:** route all Docker Hub images through GitLab dependency proxy ([6a56af2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6a56af2c2cdefc40b77a86eb5549e6259deb98ed)) by Michael Usachenko
* **data:** enforce assert_node_count on all search/traversal/neighbors tests ([3eac441](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3eac441db4bc08c266961454003fcb0ea4898063)) by Michael Usachenko
* **indexer:** term-ack dead messages to free WorkQueue subject slots ([cc6fb13](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cc6fb130dc6599d6f523039ff5fa84d66eaab059)) by Jean-Gabriel Doyon
* **indexer:** use create_or_update_stream to survive rolling updates ([a2f9f8b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a2f9f8b290c6b639b50d33891f9a373afe7906cd)) by Jean-Gabriel Doyon
* **query-engine:** type-check relationship filters against edge schema ([b5f9e48](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b5f9e487cc69b5aa25571f9e8860b0348cf46f3b)) by Adam Mulvany
* **server:** support wildcard expand in GetGraphSchema RPC ([9f7045e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9f7045e1e9f5c82e0b160123814a2c73e439ef9d)) by Michael Angelo Rivera
* **tests:** assert edge set instead of discarding in traversal_with_order_by + harden MustInspect ([589881e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/589881eeb522e2afed69e685fd33ebb9692bf3da)) by Michael Usachenko

### Performance

* **integration-testkit:** optimize test infrastructure performance ([c0144a7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c0144a76cf823e93c0bf9920c1bbe1fccc4a9dca)) by Michael Usachenko
* **query-engine:** fold WHERE filters into -If aggregate combinators ([ba31a0d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ba31a0d7be2c544d2bbadfa6d123386173dbc92d)) by Michael Usachenko
* **query-engine:** replace recursive CTE with bidirectional UNION ALL for path-finding ([c09ffba](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c09ffba96148d8d46bb811cd36dc3e69f5c949be)) by Michael Usachenko
* **schema:** add by_target projection and bloom filter index to gl_edge ([98624ce](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/98624ce850148fc7ec0de72de0b11ee599ace909)) by michaelusa

### Other

* add ADR 005 for code indexing task table ([b4798a6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b4798a6b9d36d2c9a6c4bfaf02a7029536dfa5ca)) by Jean-Gabriel Doyon
* **aggregation:** add traversal path authorization tests ([5258b7b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5258b7b22e1a14b3d0e51cea65245e82c665e2c5)) by Michael Usachenko
* **data-correctness:** add 19 missing integration tests ([99a7d4f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/99a7d4f4646fd77da95680e38d241c601d22acb8)) by Michael Usachenko
* **deps:** update rust crate bollard to v0.20.2 ([edd7ba4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/edd7ba4a3a251d750f646a8c4e382e9a971b1cac)) by GitLab Renovate Bot
* **deps:** update rust crate config to v0.15.22 ([51da395](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/51da395764390737b66f138adb8ee2f8eaeefdaa)) by GitLab Renovate Bot
* **deps:** update rust crate kube to v3.1.0 ([accf2da](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/accf2da53927c66582b0c190d71599b790e76e06)) by GitLab Renovate Bot
* **deps:** update rust crate toml to v1.0.7 ([83b64c5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/83b64c5e02502649c1dc7363cad4d219f0af6093)) by GitLab Renovate Bot
* **deps:** update rust crate tracing-subscriber to v0.3.23 ([3b40f90](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3b40f903536c7b630daf2cce9a1d89a8b5117734)) by GitLab Renovate Bot
* **indexer:** add NATS subject to message envelope ([e69d23c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e69d23c05f9dbc0d5a6f2c8f61d1169a2eaa43ab)) by Jean-Gabriel Doyon
* **indexer:** rename checkpoint fields and make commit_sha optional ([7e45821](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7e45821294e840dbab663695de06ba30ef2d52f5)) by Jean-Gabriel Doyon
* **indexer:** rename Topic to Subscription with explicit options ([0c60245](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0c602453b08323d1a5d260bfdc9dc3a9dd63fc23)) by Jean-Gabriel Doyon
* **readme:** add Mark Unthank as Product Designer ([02ec111](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/02ec111ea22d28dcd066ab560bfcc50ad4df54d4)) by Lyle Kozloff
* **readme:** update Design/UX related info ([f109c4e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f109c4ea0fd66ea73c2654eca135608cb110717e)) by Mark Unthank
* remove stale Gitaly references across documentation ([447579d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/447579d4e090c0107b769b24b50b0f62604c9292)) by Michael Angelo Rivera
* replace hardcoded relative paths with .cargo/config.toml env vars ([f702a07](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f702a0703aa4c098a5e64893d7ed70f2493fa2e7)) by Michael Usachenko
* **server:** remove gitaly-client crate ([c8b0e0e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c8b0e0ea06df44f41c0b6830dbb7bf67a70520b3)) by Michael Angelo Rivera
* **server:** replace ring with aws-lc-rs ([fe486f7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fe486f7475cc56b0d9729a8ab1eea3d6f23c0d81)) by Bohdan Parkhomchuk
* **testing:** add aggregation sort, sum, and redaction tests ([64296da](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/64296dafda74fec48479b4df42e63a8f984e7360)) by Michael Usachenko
* **testing:** add edge case tests for giant strings, sql injection, and empty results ([d9a189b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d9a189b012dda0a6ac3d6c16d06fa6c252402432)) by Michael Usachenko
* **testing:** add neighbors mixed entity types and redaction tests ([dfe2926](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dfe2926d4c27eeb16c5ac44a038f541bc2b64ff5)) by Michael Usachenko
* **testing:** add pagination, limit, empty result, and combined feature tests ([eb498af](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/eb498af6aedf2c00d1c7b1b474c4a02996764812)) by Michael Usachenko
* **testing:** add path finding max_depth and redaction tests ([7c8ff91](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7c8ff9157a216b80971960e2e49e79a1beeb1b9a)) by Michael Usachenko
* **testing:** add search tests for contains, is_null, ordering, redaction, and unicode ([0d7abf5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0d7abf5bb2dae0d7ec1d85b1d387684492358a82)) by Michael Usachenko
* **testing:** add traversal order_by, variable-length, incoming, and filter tests ([4a5bc04](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4a5bc04b634bb79067b9038b02b603989a762eca)) by Michael Usachenko
* **testing:** extend seed data with subgroups, notes, unicode user, and new edges ([742d275](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/742d2754c1a49a14442f71420cc47e473a5fdd41)) by Michael Usachenko
* **tests:** replace testkit extract helpers with ArrowUtils::get_column_by_name ([555eff3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/555eff386c4f5131a3c8ed9d982ef18ef0ca43f6)) by Michael Usachenko
* **tests:** split data_correctness.rs into modules by query type ([f49df90](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f49df902d76dfb935a9ffbb4a136ac0dd73af164)) by Michael Usachenko
* **utils:** consolidate arrow extraction helpers ([6a884dd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6a884dd59987338d76354824c152537f2a131fe4)) by Michael Usachenko

## [0.11.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.10.0...v0.11.0) (2026-03-13)

### Features

* **cli:** add workspace manager with index store and advisory locking ([523e5c4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/523e5c4bcbb6161cba6720ba1b7f2488e7e3ff7c)) by Michael Angelo Rivera
* **testing:** enforce assertion usage via query introspection in data correctness harness ([d2881ae](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d2881ae3d5b070b73e9aefb6172a3e9a71e5bc19)) by Michael Usachenko

### Fixes

* **clickhouse-client:** set Arrow string format options to match Cloud defaults ([f81b79d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f81b79d60ba11bc1b3ddd154c7fe28b60d2730ed)) by Jean-Gabriel Doyon

### Other

* **cleanup:** continue hardening data correctness harness + remove cruft ([fedad0b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fedad0b7386f0f4e59dd883d6f2ac4f5dab4ccf7)) by Michael Usachenko
* **ontology:** load embedded ontology once via [secure] Arc ([eb4485a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/eb4485a40cbbdad5201890a807d97dda9a9f8c19)) by Adam Mulvany
* **testing:** harden assertion enforcement ([698c80a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/698c80aef969bebe903d0f2875bc93ea7b4af72e)) by Michael Usachenko

## [0.10.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.9.0...v0.10.0) (2026-03-12)

### Features

* **ci:** skip tilt-ci if infra files or ci config didn't change ([6f1a352](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6f1a3520af51a6b504a3a0f9b3f3758a6b736b05)) by Michael Usachenko
* **cli:** rename gkg binary to orbit ([bb656e1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bb656e1c02ff55121720c3c014ff6fa0aebb216d)) by Michael Angelo Rivera
* **dev:** add gkg-dev.sh script to manage local dev environment ([a7feb60](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a7feb601e7b137079fe80a037db3bc96380dc1db)) by Adam Mulvany
* **dev:** add lefthook for pre-commit and pre-push hooks ([75f641a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/75f641ab8e4f94898d9819ccbe450a5837dc4f95)) by Michael Angelo Rivera
* **gkg-server:** add get_public_id and Float64 column value ([f57f902](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f57f902f276d61ced27c5cf0eef6eb2d1c411283)) by Michael Angelo Rivera
* **gkg-server:** add GraphFormatter with unified response schema ([3383510](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/33835107cd9be83c4f5ef564ad938cfd0d43b3b1)) by Michael Angelo Rivera
* **gkg-server:** wire GraphFormatter and GoonFormatter into pipeline ([2bae6e2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2bae6e28c74d1bd8ae91dee725a9fe2c1abede8a)) by Michael Angelo Rivera
* **indexer:** add gitlab auth check to readiness probe ([9b4eb60](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9b4eb607be4a15c3aef5114a44b51b60f608120d)) by Bohdan Parkhomchuk
* **indexer:** add namespace deletion handler and scheduler ([517cd8f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/517cd8f3c7e3b21ed397ad0911e03412d5a9dacb)) by Jean-Gabriel Doyon
* **indexer:** clean-up node tables with deleted entries ([09d3365](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/09d336518d9ea28e0833dfb5825b372edb385c61)) by Jean-Gabriel Doyon
* **indexer:** migrate merge_requests from hierarchy view ([a509764](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a5097647199648e68d995a20937e8a1a369c8314)) by Jean-Gabriel Doyon
* **indexer:** replace direct Gitaly access with Rails internal API ([2de7063](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2de70638df08412ac2bafb962e93222de4ec341a)) by Jean-Gabriel Doyon
* **query-engine:** add EdgeMeta to ResultContext ([a977c27](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a977c27bfedb654d6ca621a38225ca41be7145f4)) by Michael Angelo Rivera
* **simulator:** enable concurrent benchmarking ([cc75829](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cc758297e94444ef01a8fbd74c6551ac94048b3a)) by Michael Usachenko
* **simulator:** granular control over fake data distribution ([2520630](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2520630452574869f1e913024a1fb593fe19c354)) by Michael Usachenko
* **testing:** add data correctness harness + data integration tests ([a829d9d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a829d9d42ce3c6220f928de5bc3fe35089d5b6cf)) by Michael Usachenko
* **testing:** introduce integration-tests crate ([54a51b8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/54a51b821af95ad1b588a8fa28a70380f202f87f)) by Michael Usachenko

### Fixes

* **ci:** skip lefthook install in CI pipelines ([4eff22c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4eff22c3930dbf4a1ec40735e12c1f77a54e54b9)) by Michael Angelo Rivera
* **ci:** strip draft prefix before validating MR title ([4326cf0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4326cf05755389487cb995b0caf1dfe87ad1f799)) by Adam Mulvany
* **ci:** switch small runners to medium runners ([470860b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/470860b47123c3e12803baafee451cb94f5c3546)) by Michael Angelo Rivera
* **clickhouse:** restore TLS features removed by Renovate ([4879a5a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4879a5ac12a3c4f6e067b907a8104b90293ffce4)) by Bohdan Parkhomchuk
* **code-graph:** assign node ids during indexing ([95515fe](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/95515fe4b0b5772c05b5f3ae5b72a21359342142)) by michaelangeloio
* **hooks:** add post-checkout hook for worktree lefthook support ([1cfab33](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1cfab33a9140e93c4eabf1e873399710e534647e)) by Michael Angelo Rivera
* **query-engine:** project all columns in multi-hop union arms ([0acf84c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0acf84c3cad21c3920f4269ba58994c131232320)) by Michael Angelo Rivera
* **query-engine:** reject unsupported aggregation functions and validate numeric types ([359c3b9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/359c3b94b9395964bbbc3403ce6eca09896219f7)) by Michael Angelo Rivera
* **query-engine:** remove traversal path prefix from join conditions ([8dbf576](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8dbf5767e5f074c2dd48ffe98dfb2ef48d4be0cf)) by Michael Angelo Rivera
* **query-engine:** resolve duplicate table aliases in fan-in joins ([ad08cec](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ad08cec60f9891c799a14dbae697017bc886bf2c)) by Michael Angelo Rivera
* **query-engine:** track edge direction in bidirectional neighbor queries ([1ceba83](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1ceba83174709ebdbf26ba8ccbaef88c6cb0b19d)) by Michael Angelo Rivera
* **querying:** graph formatter removes node sort order with hashmap - use indexmap instead ([f79f0a6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f79f0a650aa0cdcf654b7645716e5ec0e5699f77)) by Michael Usachenko
* **simulator:** parameter interpolation broken for IN queries ([6f2653c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6f2653cfd7cdaa895cd1f1c062ef267d43b37c9f)) by Michael Usachenko
* update siphon proto filename and exclude output/ from lychee ([fa91fc9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fa91fc914144d006058e62e6babc590b9853b54e)) by Michael Angelo Rivera

### Other

* add unified query response schema ADR ([0e5a522](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0e5a522020400b5bc970a37c96a7f00e4b3e4ad7)) by Michael Angelo Rivera
* **ci:** cap ontology schema at 32 KB ([bb25136](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bb251362e65a2da542c376f10778823558858c1b)) by michaelangeloio
* consolidate JSON schemas into config/schemas ([7b927f2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7b927f20a52a7c9b82bddee5b1423d6c26f5bc8d)) by Michael Angelo Rivera
* **deps:** update rust crate cliclack to v0.4.1 ([89d1024](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/89d10240d311bd6155add5afb9416690c534a452)) by GitLab Renovate Bot
* **deps:** update rust crate config to v0.15.21 ([ce39c96](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ce39c96b641acfd2e5ccc52f90de809994fc2767)) by GitLab Renovate Bot
* **deps:** update rust crate datafusion to v52.3.0 ([9ae6bcb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9ae6bcb2031ac9041324ce4cb21f6af140730301)) by GitLab Renovate Bot
* **deps:** update rust crate once_cell to v1.21.4 ([926546c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/926546cb7a62293f87bdb2b3a275d738b6b15a27)) by GitLab Renovate Bot
* **deps:** update rust crate tempfile to v3.27.0 ([78f75b7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/78f75b7d6671fb6b8a5817dd69b955dcc16f405f)) by GitLab Renovate Bot
* **dev:** make GDK_ROOT configurable in mise.toml ([b254091](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b254091d8db21c73024fb27ae771446b41948d08)) by Lyle Kozloff
* discover cleanup tables dynamically from ClickHouse ([9cb840b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9cb840b926a053de9d8dc0bef32c880815dc2f23)) by Jean-Gabriel Doyon
* **gkg-server:** add integration test harness for GraphFormatter ([50fede6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/50fede6e0f649302dc3fdd92d6a4dfb62c5ebd62)) by Michael Angelo Rivera
* **gkg-server:** restructure formatter into module directory ([b96ac7b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b96ac7ba746a8c0995ef73b389189f2c8c65d8a2)) by Michael Angelo Rivera
* **indexer:** extract llqm_v1 module from sdlc plan ([ec3227b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ec3227b8703fbfab92e3c19a068812019944c8c4)) by Jean-Gabriel Doyon
* **indexer:** rename code watermark to checkpoint, add traversal_path ([a862469](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a86246937a1ff52de15d30b017e4e9770f0ba74e)) by Jean-Gabriel Doyon
* **indexer:** schema changes for namespace deletion and make checkpoint re-usable ([f85cd83](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f85cd83955367f5fffefcfb509a63934e9a22b6c)) by Jean-Gabriel Doyon
* **indexing:** update code indexing design document with as-built architecture ([78aa469](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/78aa46912e1766448e11490b6be61a77c11da5d0)) by Adam Mulvany
* move ontology data into crate, graph.sql into config ([a26ec44](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a26ec44a86a0a5a5efa6d7391778b37609b1d8af)) by Michael Angelo Rivera
* **simulator:** bump rand crate manually due to breaking changes ([245dcaf](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/245dcaf5bb5f8bb822b3be5b043eba70958c631a)) by Michael Usachenko
* **tests:** move health tests to integration-tests, extract MockRedactionService ([da75c89](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/da75c895f0971c446cc0ee9012fcb4e82f889ffc)) by Michael Usachenko
* **tests:** move indexer integration tests to integration-tests crate ([bcbcea3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bcbcea34d144d713c34e6fdd8ddb037db3cf6907)) by Jean-Gabriel Doyon
* **tests:** replace IndexerTestExt god trait with composable free functions ([da7c35e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/da7c35e69ee8ba6da5be3c9f1c33747dbaca217e)) by Jean-Gabriel Doyon
* **tools:** wrap TOON schema in XML tags instead of labeling format ([55e180d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/55e180de069ea8715529ee2285ca5b0ba11688b8)) by Jean-Gabriel Doyon
* unify siphon.sql into fixture/ and improve array_field schema docs ([47052b0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/47052b04ac0805082e238662cab1345537e5777a)) by Jean-Gabriel Doyon
* **webserver:** replace health probes with /live and /ready ([a348b80](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a348b80006486568a2317c32a2a3ea7c2dbe8bcc)) by Bohdan Parkhomchuk
* **xtask:** move simulation harness to xtask ([b19c097](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b19c097710d320f41a72becac9c63b4d3b41b1e5)) by Michael Usachenko

## [0.9.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.8.0...v0.9.0) (2026-03-10)

### Features

* **gitlab-client:** add resolve_host for PSC DNS override ([936eaa6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/936eaa6511421c54e5ef79ee142196d5676e02ae)) by Bohdan Parkhomchuk
* **indexer:** metrics cleanup in dispatcher, sdlc and code indexing ([f35126f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f35126fe6c9fdc958201e5a5a542dcee19cab726)) by Jean-Gabriel Doyon
* **indexer:** rewire SDLC handlers to use cursor-based keyset pagination ([586855f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/586855f37a2527ad206f5e9e1b541b120c52718b)) by Jean-Gabriel Doyon
* **querying:** validate values used in IN operator match column type ([8f77cca](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8f77cca95ce743a934b802970177390741900043)) by Michael Usachenko
* **skill:** introduce code-history skill ([f456da6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f456da6adb696f61adf1488bb8a1cbfe75df6e07)) by Michael Usachenko

### Other

* **deps:** update rust crate k8s-openapi to v0.27.1 ([74df1e6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/74df1e6630eaf072d038d0d49f92ad2567bfb572)) by GitLab Renovate Bot
* **deps:** update rust crate toml to v1.0.4 ([801bc2c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/801bc2caa1296ebbb1df5e00a11f09760fd39849)) by GitLab Renovate Bot
* **deps:** update rust crate toml to v1.0.6 ([123c071](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/123c0713a8a4fb416b92b97aebed8f2fc6c148de)) by GitLab Renovate Bot
* **deps:** update rust crate uuid to v1.22.0 ([f39a26a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f39a26ab6b29240e617d4fc260f5259378592f68)) by GitLab Renovate Bot
* **indexer:** extract ScheduledTask abstraction from Dispatcher ([f734de9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f734de92463aaa0accdbbee575581cd54fe35304)) by Jean-Gabriel Doyon
* **querying:** clean up hydration stage cruft + arrow code duplication ([504f3ca](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/504f3ca0a4d38726c49463a13d64ecd5f7b1f3df)) by Michael Usachenko
* **querying:** streamline & harden parameterization codepaths in graph engine ([48ff654](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/48ff65409adb7871e8767bc0374bcad862a1bfc6)) by Michael Usachenko
* remove unsued clickhouse-client and indexer code ([5cb0d6f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5cb0d6f6e390bf5893bd0b4bbe8c35af1b4be322)) by Jean-Gabriel Doyon
* **simulator:** fully derive simulator config from ontology & yaml specs ([e3ffa15](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e3ffa154195d175f0f5e04c8ff91da956c179b07)) by Michael Usachenko
* **simulator:** unify graph generation with epsilon node expansion ([06c129d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/06c129d6e3fc6b51dbe7aa66286606bea8404201)) by Michael Usachenko

## [0.8.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.7.0...v0.8.0) (2026-03-06)

### Features

* **indexer:** add plan module with AST, codegen, and ontology-driven pipeline plans ([4fb995d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4fb995d4e1cac16e02691a587dc3b2505320619d)) by Jean-Gabriel Doyon
* **ontology:** structured ETL fields and order_by support ([6c86fd3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6c86fd3348a701dce2552c6aea91f1e98753e242)) by Jean-Gabriel Doyon
* **querying:** add options field to json queries, apply it to dynamic hydration ([5660a25](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5660a250a02cc408e92aa928fb5ff44fc0dafec2)) by Michael Usachenko

### Fixes

* **clickhouse:** enable TLS for HTTPS connections ([ae073a5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ae073a5181cd4eb9be86b406c1678d153d850b02)) by Bohdan Parkhomchuk

### Other

* **indexer:** replace dispatcher KV locks with NATS per-subject dedup ([2fd1f66](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2fd1f6645c42bdd990c2a12d6af5accbdb108a08)) by Jean-Gabriel Doyon
* **ontology:** break up ontology crate ([89770c3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/89770c38b1917cc95ea83f28857a3b31b29553d5)) by Michael Usachenko
* **querying:** query pipeline state and execution flow cleanup ([1b2dd16](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1b2dd1610f65db95a3a3aecfcc432c01d937bde7)) by Michael Usachenko
* share ClusterHealthChecker, add health check docs ([49b833c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/49b833c553132a1e91b61807ff27a245dd09292e)) by Michael Angelo Rivera

## [0.7.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.6.0...v0.7.0) (2026-03-05)

### Features

* align gkg.proto for API design ([1c70ffa](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1c70ffa1d92b8d6884f9f711cdc2607db68f1360)) by Michael Angelo Rivera
* **graph:** add traversal_path join filtering for some edge lookups ([d772683](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d7726834b9713d919ce3c2f5d8bd86d2077c048a)) by Michael Usachenko

### Fixes

* **release:** disable semantic-release MR commenting ([74232ab](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/74232ab950f5fff407ff234024ad822cfe96bc03)) by Bohdan Parkhomchuk

### Other

* **adr:** add ADR 003 for Orbit API design ([a7eaae5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a7eaae5c99205be8c61f1539441cb0fbc0eb0795)) by Michael Angelo Rivera
* **deps:** update rust crate cliclack to 0.4.0 ([9118e4a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9118e4aebc869fac550f7fe33051a9126f25e0e5)) by GitLab Renovate Bot
* **graphsec:** harden engine + query pipeline w/ more gating and test coverage ([0ab725d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0ab725d4f623544adbff56986f98339544b17673)) by Michael Usachenko

## [0.6.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.5.0...v0.6.0) (2026-03-03)

### Features

* add Pajamas design system skill ([df2b439](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df2b439e0e02cec77152833721d50c7d7f422887)) by Mark Unthank
* **ci:** add docs review bot ([68c486e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/68c486e9cc1df3f6324e4caa71903cefa26fb3e2)) by Michael Angelo Rivera
* **ci:** add gkg bot ([91833c6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/91833c6b169f6458357dce3cd9530bb73db6bc35)) by Michael Angelo Rivera
* **config:** add secret file source for K8s mounted secrets ([488609d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/488609d3b4b62056009b04ef7b7333fc7289d5c6)) by Bohdan Parkhomchuk
* **e2e:** expose ui from e2e harness via serve xtask ([8129df8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8129df89c82c12fbc0598cebb5e6c83854ffa236)) by Michael Usachenko
* **engine:** add instrumentation for query pipeline errors ([0aac642](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0aac642e2a7551cdefaffd20f6a28bd4cb7e71a8)) by Michael Usachenko
* **engine:** add pagination support for querying ([e9f6c51](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e9f6c51b58f4771ca9b02336efff9ad9a56b2ff1)) by Michael Usachenko
* **engine:** add post-validation phase before codegen ([f0de45d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f0de45dea750f823fcc2de27fbe3d76d3331d2ea)) by Michael Usachenko
* **engine:** introduce hydration scaffolding into graph engine ([3b97015](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3b9701583c4cb19204daddb346a6de5513928621)) by Michael Usachenko
* **engine:** wire up new hydration codepaths for dynamic queries ([3944153](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/394415339d05defbf09154c2c89c4036c0db1d4c)) by Michael Usachenko
* **helm:** wire engine retry config into indexer configmap ([3ae67bd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3ae67bde41165f8c143eef9c9e3a12321b717b79)) by Jean-Gabriel Doyon
* **indexer:** add and use GitLab client for repository info ([3e4a4ad](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3e4a4adc13c00c506b8302dd3ed76ed356f92d89)) by Jean-Gabriel Doyon
* **indexer:** add graph database write metrics ([abd1ddb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/abd1ddb723694cd7540d88ed9c95a155a05bed33)) by Jean-Gabriel Doyon
* **indexer:** add health endpoint for Kubernetes probes ([96f8630](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/96f86301c04013fd99fc5e56bc8f101ad3060b76)) by Jean-Gabriel Doyon
* **indexer:** add interval support to the Dispatcher trait ([984e3b1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/984e3b1041e7b6e92367e585a5cccb27a08b1bb7)) by Jean-Gabriel Doyon
* **indexer:** add metrics and improve logging for code indexing handler ([d72d3e2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d72d3e23aca5301cf2bd6e29489fc41f32c52d3d)) by Jean-Gabriel Doyon
* **indexer:** add metrics for dispatch indexing mode ([730a63a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/730a63a43ca212757bbb04ba1ec3b7afedf42f3d)) by Jean-Gabriel Doyon
* **indexer:** add ProjectCodeDispatcher for code indexing reconciliation ([bbe8983](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bbe8983de05cf5e4c7375b5b5748021e785b024e)) by Jean-Gabriel Doyon
* **indexer:** add ProjectCodeIndexingHandler for code reconciliation ([8161935](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/81619350df0c1d90aeaf07c7f252a51401889352)) by Jean-Gabriel Doyon
* **indexer:** add write error and handler error metrics, clean up labels + add metrics in observability docs ([37f7d9c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/37f7d9c9e2652e99ad342aacd7e36b92602ec231)) by Jean-Gabriel Doyon
* **indexer:** delete nodes and edges from previous index ([0825cd0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0825cd09d9979511c6f5547d3f09a9e97e51b123)) by Jean-Gabriel Doyon
* **indexer:** skip initial snapshot events in push event handler ([d838942](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d8389428be4f3dd21f475437118e5a59cdaeb01c)) by Jean-Gabriel Doyon
* **indexer:** support batch processing SDLC queries ([599c41b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/599c41b9b332529fbafd413e072bb01577c31451)) by Jean-Gabriel Doyon
* **ontology:** add default columns field to entities ([44ae354](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/44ae354de06ec420d9cafb5a054de46caf538e9a)) by Michael Usachenko
* **ontology:** add sort keys to ontology config for downstream use by query engine ([6b2d0df](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6b2d0df204851889b1b6c8ad74cfc61e16326352)) by Michael Usachenko
* **ontology:** global config for ontology values ([df43245](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/df432457347bdb456ca4dab54044593ae6d203da)) by Michael Usachenko
* **querying:** add having clause and subquery support to graph engine ([b54d169](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b54d16998d07321e5cbb7dda8fb34d1d2b4b7895)) by Michael Usachenko
* **querying:** wire up prometheus alerts for graph engine + querying pipeline ([74d97b0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/74d97b098de111256a90c12eeb52aa410b8b7afb)) by Michael Usachenko
* **server:** performance and error instrumentation for querying pipeline ([cfcbfc3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/cfcbfc397142c128da4bd8e973031593bb6859c6)) by Michael Usachenko
* **skill:** add drift repair skill for e2e harness ([6c42047](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6c42047939543b5c8ed61a1b63386641a339c339)) by Michael Usachenko

### Fixes

* **ci:** add build-proto-gem dependency to semantic-release ([f446d56](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f446d56368efd4e3e2cdbbb08af18f099523fffd)) by Michael Angelo Rivera
* decode base64 JWT secret to match Rails signing ([61b75a2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/61b75a2a3cd412cbc16be6fc562b2a686c0f6866)) by Michael Angelo Rivera
* **e2e:** skip dropping active replication slot on re-run + env credentials fix ([5c0024f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5c0024fc5df9ac14c10b85111f77e764ff967d1a)) by Michael Usachenko
* **indexer:** replace panic with graceful failure on closed worker pool semaphore ([6e110c4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6e110c45648c6e591f52046cb9ef9162317d5ec2)) by Jean-Gabriel Doyon
* **indexer:** skip watermark update when no rows are indexed ([d8b5564](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d8b5564da3e6d70cb79000eecafeeb0f0cdaca0a)) by Jean-Gabriel Doyon
* **ontology:** correct redaction abilities for stage, work_item, scanner, and security_scan ([064acff](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/064acff3ceba739fc3d651649eea382e20f8d390)) by Michael Angelo Rivera
* redaction resource_type to singular, matching Rails ([1351383](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1351383403582316bacaa7172e3ba6cce444ebff)) by Michael Angelo Rivera
* **simulator:** better fake uuid + hash values in simulator ([b78286a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b78286a1fbdb011bfe1a8b2eb7cfdfa4d0b4e6ee)) by Michael Usachenko

### Other

* add ADR 002 for Rust as core runtime language ([99806da](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/99806da2d4567f022d32bbf8c23586cf08bcb45f)) by Michael Angelo Rivera
* **ci:** download clickhouse and nats before running integration tests ([bbecfba](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bbecfbac6a95a8826abbb874bb766727afdca8f8)) by Michael Usachenko
* **ci:** increase gkg bot timeout ([1bca9a6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1bca9a6e2d1cc3403dc17d5918484cd4189bfee4)) by Michael Angelo Rivera
* clean up CI pipeline ([47af9dc](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/47af9dcc8e32c297e2b8196e7a3537cfe281f4f2)) by Bohdan Parkhomchuk
* **config:** consolidate JWT secrets under gitlab.jwt section ([d37cb35](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d37cb354d6a768b803eec58904b2c53f55adba28)) by Bohdan Parkhomchuk
* default gRPC port from 50051 to 50054 ([6a204e1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6a204e15a16b1ebf07810aa063f9fd3ee002f883)) by Michael Angelo Rivera
* **deps:** update rust crate datafusion to v52.2.0 ([4fdd62c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4fdd62c757709e59b2ed2884dadf59fef3e47ef8)) by GitLab Renovate Bot
* **deps:** update rust crate jsonschema to 0.44.0 ([171e0b9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/171e0b9a56de0acf44a94c38070b47e0c7e8c7d2)) by GitLab Renovate Bot
* **deps:** update rust crate moka to v0.12.14 ([c026118](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c0261184d9bfd0c204948671176224c30788f40d)) by GitLab Renovate Bot
* **deps:** update rust crate rustls to v0.23.37 ([1788560](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/17885608bb590e6b6f2c58cddf32bb4e3d2d9c4a)) by GitLab Renovate Bot
* **deps:** update rust crate rustls to v0.23.37 ([9cec51a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9cec51aa1fe19c37a8aa0e35e77730c95d1de2c3)) by GitLab Renovate Bot
* **deps:** update rust crate tempfile to v3.26.0 ([6be74e8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6be74e80516a715bfc3e4a27f35be9b7794d78f5)) by GitLab Renovate Bot
* **deps:** update rust crate tokio to v1.50.0 ([60fd87d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/60fd87d4819755408bff716485f1dc5faa2887b1)) by GitLab Renovate Bot
* **dispatcher:** modularize dispatchers in preparation of adding more ([6727d0d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6727d0d93030a1e5ed0b87d130fcc9c06d546eac)) by Jean-Gabriel Doyon
* **dispatcher:** replace string-keyed dispatch config with typed structs ([7881886](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/788188606fd3463cb00c8cefe26ab9292465bd52)) by Jean-Gabriel Doyon
* **docs:** document otel metrics for querying pipeline in gkg-server ([f9aa1ba](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f9aa1ba057f920c59d2d361d23f7c111c0744b85)) by Michael Usachenko
* **e2e:** drift management + documentation ([b019b2f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b019b2fdd4a75d107b0ecb744b59e238849e86fe)) by Michael Usachenko
* **e2e:** get e2e harness ready for local dev and ci ([b798a30](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b798a30a108872100f37fb4fc7ff8544e50dbd64)) by Michael Usachenko
* **e2e:** replace docker shellouts with bollard ([2d8870b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2d8870b8de0a8d4ee9bacec145ebd99620be0474)) by Michael Usachenko
* **e2e:** use kube crate instead of kubectl shellouts ([4edadd9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4edadd9320f99536c74c5db7c5e2459afa1e1d10)) by Michael Usachenko
* **engine:** move engine configuration from per-module to per-handler ([bccfee3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bccfee3c66d11403fc2b9a012ea55815c313b016)) by Jean-Gabriel Doyon
* **engine:** refactor pathfinding lowering to scale better ([5aef4a0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5aef4a08e72c157712259061dc2e29b368651013)) by Michael Usachenko
* **engine:** remove Module abstraction, register handlers directly ([442efe5](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/442efe55b650c739556624ae8c365620998302c9)) by Jean-Gabriel Doyon
* **engine:** replace string-keyed handler configs with typed structs ([62db4ea](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/62db4eaf446f596f7fff8594e4bb111a99167573)) by Jean-Gabriel Doyon
* **engine:** simplify neighbors lowering + fix auth resolution bugs ([e8939eb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e8939eb103cd1cbb9db10d85e8f5105d419e9a22)) by Michael Usachenko
* **etl:** replace remaining table hardcoding and derive names via ontology ([e1f4e28](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e1f4e2830a8688b6d41b76304134a02743a1f231)) by Michael Usachenko
* **indexer:** add metrics and improve logging for code indexing handler ([9927405](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/9927405b1e29c85d553cecaeac5b761def4460d4)) by Jean-Gabriel Doyon
* **indexer:** configure durable NATS consumer for message persistence ([02cb38e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/02cb38ef801397e2293b1b440fd579c0d702abf0)) by Jean-Gabriel Doyon
* **indexer:** deduplicate constants, extract helpers, clarify naming ([d824c76](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d824c76566322b2d6349a8407caceacb15d0bdfb)) by Jean-Gabriel Doyon
* **indexer:** unify module configurations ([c92034f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c92034fbedde1ab34ca0992042b705fc18fb3a5b)) by Jean-Gabriel Doyon
* **ontology:** move to centralized constants file ([63afcd9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/63afcd930787cbdce23e7b9e453f1e3039ef623c)) by Michael Usachenko
* **querying:** cleanup boilerplate struct instantiations in graph engine tests ([af0d66c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/af0d66cb0dbded92079e514b2a0e1b974425b247)) by Michael Usachenko
* **querying:** more test coverage + cleanup normalize phase ([028caa8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/028caa80d4be117e2c8cfee39692cedd9828b92a)) by Michael Usachenko
* **querying:** wire default_columns from ontology into the query engine/pipeline ([5f88c67](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5f88c6779bca3c09d225628bf29aa37e0ad7d959)) by Michael Usachenko
* **tests:** improve sdlc test speed ([fe131f0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fe131f0c3e4dd3ba22e1c0c100602527aeac6611)) by Jean-Gabriel Doyon
* **tests:** share test context to parallelize integration tests ([e7d830b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e7d830b9cc6d2aecdba028889864fa6004d1878a)) by Michael Usachenko

## [0.5.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.4.0...v0.5.0) (2026-02-23)

### Features

* **indexer:** add per-module retry policy configuration ([d9fe7b7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d9fe7b70fef6f0da9dd1328ad2e527780c60f3de)) by Jean-Gabriel Doyon
* **xtask:** add in e2e test suite for e2e harness ([d96ac3c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d96ac3c04c5f4207ace344e4a1df5509abf2dddc)) by Michael Usachenko
* **xtask:** deploy and configure gkg services for e2e harness ([2cc569c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2cc569cee0221596ecfe0ad44040671ddea3129f)) by Michael Usachenko
* **xtask:** wire up e2e harness to tilt + hardening ([fe41c24](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/fe41c248c13b26d1b66101e4a2526191722d9d17)) by Michael Usachenko

### Fixes

* **indexer:** use correct code edge labels ([0ec50a1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0ec50a1de727ebe9222963d52c9fa8b57052edee)) by Jean-Gabriel Doyon
* proto gem require path for generated gRPC files ([f7157ca](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f7157ca27126d22735cd12e8d53ee19e81ef31fc)) by Michael Angelo Rivera

### Other

* **deps:** update rust crate chrono to v0.4.44 ([b7c8922](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b7c8922a9330f57c15ac2f2521a0641fa5ae5190)) by GitLab Renovate Bot
* **deps:** update rust crate parquet to v58 ([8140a09](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8140a09796a5ebc62128765af6914ec2fc891bc3)) by GitLab Renovate Bot
* **deps:** update rust crate strum to 0.28.0 ([0f565e0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0f565e0a6a2d953f293801978365fe3144b8f6d9)) by GitLab Renovate Bot
* **deps:** update rust crate testcontainers-modules to 0.15.0 ([128b6c1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/128b6c15d276ab9b72843483701ee749bab97471)) by GitLab Dependency Bot
* **xtask:** add centralized config.yaml for e2e harness ([f406800](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f40680088e0cfa53f543ffd63d0dd8d2b807c589)) by Michael Usachenko
* **xtask:** remove tilt from e2e harness ([8ef98e2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8ef98e2d50da92bbcce3751a86972934c4995255)) by Michael Usachenko

## [0.4.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.3.1...v0.4.0) (2026-02-22)

### Features

* **indexer:** add sdlc indexing metrics ([6a1951b](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6a1951b865e2c57413e2ed22ab477120376c2a16)) by Jean-Gabriel Doyon
* **indexer:** add traversal_path to gl_edge table ([f397d68](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f397d689c3ba0dab46e36a6fc7861bc91d81e04b)) by Jean-Gabriel Doyon
* **proto:** add gkg-proto Ruby gem build pipeline ([6eb77da](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6eb77daff361ffdf50963ce421aa515922f3872c)) by Michael Angelo Rivera
* setup xtask crate ([7f37c08](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7f37c08c5b34e0797295186220fe83fac6bc03c9)) by Michael Usachenko
* xtask for e2e testing infra - gitlab cloud native cluster setup ([0b35d0a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0b35d0a5b8a0ea4f2168aed3b4436df9c7a640f3)) by Michael Usachenko
* **xtask:** clickhouse setup and migrations for e2e harness ([6ff0e44](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6ff0e44213af61b7e7ef5e1c26b1cd1cce23254b)) by Michael Usachenko
* **xtask:** handle cng post-deployment tasks for e2e harness ([ddef2bd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/ddef2bdaa653accffb85f056e37acf8690ae06c7)) by Michael Usachenko

## [0.3.1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.3.0...v0.3.1) (2026-02-20)

### Fixes

* **ci:** ensure release builds run for tags ([1991373](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/199137395a34f7b1e69a47b09689920d75b00910)) by Bohdan Parkhomchuk

### Other

* **indexer:** move dispatcher into indexer crate and hook datalake-generator ([04236f1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/04236f13bb71ba9dc0d0c325546aec71d59bd5c9)) by Jean-Gabriel Doyon

## [0.3.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.2.0...v0.3.0) (2026-02-20)

### Features

* add update-docs skill for documentation maintenance ([6c9d73f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/6c9d73fad5266d34f01bf4d84e21ebac97af8393)) by Michael Angelo Rivera
* centralize config with config crate and YAML support ([b077137](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b0771379c4fa56d9b5d67c39d6530146a68fd69a)) by Michael Angelo Rivera
* **docker:** multi-arch builds with UBI base ([13d5776](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/13d5776bfafbff2b0fe4f15efdcc17a03c29be59)) by Bohdan Parkhomchuk
* **docker:** native multi-arch builds with ARM runners ([349df2f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/349df2fad2df6f501fe85575e8cc6da370184386)) by Bohdan Parkhomchuk
* **indexer:** add stream creation, stream connection, retry  and configuration logs ([e6925b3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/e6925b3b042cc4e2ef9a3f2c566cf184a2d477cc)) by Jean-Gabriel Doyon
* **indexer:** introduce locking service trait ([73c98c4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/73c98c4db3077f653f02a117edc3639de8054c1a)) by Jean-Gabriel Doyon
* **indexer:** use push_event_payloads project_id field directly ([d413d07](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d413d0707284df651a35ac9f71e99f6416d18adc)) by Jean-Gabriel Doyon
* **sdlc:** add indexing errors logs and remove duplicated debug logs ([dfc48a0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/dfc48a0c6489e5662654e673c115ce612d181f13)) by Jean-Gabriel Doyon
* **skill:** add dataflow mapping skill ([7787e56](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/7787e5630fb926a9db6b8d25eae0aed6a7ea9121)) by Michael Usachenko

### Fixes

* **config:** fix env variables handling ([18deca4](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/18deca43d5f8d3bbbcf3d879a6c721e42c996ea5)) by Bohdan Parkhomchuk
* **config:** use with_list_parse_key for env parsing ([bac464d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/bac464d64cd5c40539cc2630852ebc0bbf0f18ec)) by Bohdan Parkhomchuk
* **docker:** use buildx imagetools for manifest creation ([2017030](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/2017030963c2fd7d30e5222d18752f4e7b3e50e9)) by Bohdan Parkhomchuk
* **graphsec:** handle ontology-defined redaction specs in engine ([4eb1390](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4eb13902504f29dc5a8acf98acc1fd7acc383466)) by Michael Usachenko
* **indexer:** add stacker to guard against recursion in all languages ([38acaa9](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/38acaa9acb6c9d9f3aa9d33e10a4e652815d6278)) by Jean-Gabriel Doyon

### Other

* add gRPC communication protocol ADR ([d157957](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d157957fd9d6eac6b320b0d6077198f63abb71a2)) by Michael Angelo Rivera
* add markdown linting with markdownlint, Vale, and lychee ([1653ed8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1653ed8823c985a6ea62c5a0d87e1ddd506ced15)) by Michael Angelo Rivera
* **agents:** improve agents.md ([a60fb2d](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/a60fb2d205d009ad08188151112225a7416329f4)) by Michael Angelo Rivera
* **deps:** update rust crate anyhow to v1.0.102 ([90664d8](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/90664d88bb4fe47a8fa7b902044d4bcd1ecde0db)) by GitLab Renovate Bot
* **deps:** update rust crate anyhow to v1.0.102 ([08cbdba](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/08cbdba6bc6cb8b1c9db7ef946eb0fdcba505c4e)) by GitLab Renovate Bot
* **deps:** update rust crate clap to v4.5.59 ([58879fa](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/58879fa878d054f0498f71d0c8fc120385c59994)) by GitLab Renovate Bot
* **deps:** update rust crate clap to v4.5.60 ([041f45a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/041f45afe8a494fa7ba6cdfcdc353b3e2a295eac)) by GitLab Renovate Bot
* **deps:** update rust crate futures to v0.3.32 ([5d86a79](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/5d86a79e12944a7db9bf2e8ac047bc74fd8384a4)) by GitLab Renovate Bot
* **deps:** update rust crate toml to v1 ([3e25dc3](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/3e25dc3620fa5635ac9e704c41e31d98b94e3df1)) by GitLab Renovate Bot
* **deps:** update rust crate tonic to v0.14.5 ([0cb42bd](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/0cb42bd311efb7116937a21fa422ff00e8edcb4b)) by GitLab Renovate Bot
* **deps:** update rust crate tonic-build to v0.14.5 ([145768c](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/145768cc87413d58a00a6f973f5c30f9669cb957)) by GitLab Renovate Bot
* **deps:** update rust crate tonic-prost to v0.14.4 ([8e7c47a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8e7c47a81b2afca7e02faaac62e310279334006a)) by GitLab Renovate Bot
* **deps:** update rust crate tonic-prost to v0.14.5 ([c7e577a](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c7e577a56c192bf1cb1c906d525cd2d4cdd59b7e)) by GitLab Renovate Bot
* **deps:** update rust crate tonic-prost-build to v0.14.5 ([1f14f16](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/1f14f16c6bac29c85ba45b080f09850ac262106a)) by GitLab Renovate Bot
* **docker:** bump runtime base to ubi10 ([8efe1e1](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8efe1e1b6f68269e99685dcb0ff52ab5074ff6ce)) by Bohdan Parkhomchuk
* **engine:** migrate to enforce phase + centralized constants ([f6bde1f](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/f6bde1f30c786cd5e1f61f1de3843fee3b2b4719)) by Michael Usachenko
* **etl:** remove plural from graph table names ([4a5e7ea](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4a5e7ea96349b2d30a0af6ec70c24e5692e64ca9)) by Jean-Gabriel Doyon
* few changes to speed up local compilation ([837e6bb](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/837e6bb0bc73fd134f7fea33da610be3031cc979)) by Michael Usachenko
* **helm:** switch from env vars to config files ([66f48d0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/66f48d0895f22a00e5e48b0bcd837989b3dd06d0)) by Bohdan Parkhomchuk
* **indexer:** fold etl-engine, sdlc module and code module into a single indexer crate ([b0aa6ed](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/b0aa6ed47fb18e7a6b8f25161df76c6af896a76c)) by Jean-Gabriel Doyon
* **indexer:** rename code-indexer to code-graph ([8675cad](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/8675cad2fcea4c800d172211d0ba2186475269c8)) by Jean-Gabriel Doyon
* **indexer:** update datalake fixture schema and support uuid ([259b15e](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/259b15e5585ed26e54f237e701e8ab0c0e95704c)) by Jean-Gabriel Doyon
* move design documents from handbook into repository ([444add7](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/444add753f990a182fd7b38c0399804c1efcc017)) by Michael Angelo Rivera
* replace README with project SSOT, move dev setup to docs/ ([c9816b6](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/c9816b6e7fd3a2a477969782abfa6279276a521c)) by Michael Angelo Rivera

## [0.2.0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/compare/v0.1.0...v0.2.0) (2026-02-18)

### Features

* **ci:** add semantic-release for Docker image releases ([d87eab0](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d87eab066baa444a44e62ead56ea24f2c362ba2f)) by Bohdan Parkhomchuk
* **sdlc:** save per namespace entity watermark ([499dd03](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/499dd03b6d7bd9af32c7874cd081de499b312c12)) by Jean-Gabriel Doyon
* **testing:** add datalake generator test tool ([22dcd45](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/22dcd45606cdc8e71882580fc9f11c41bf6170cd)) by Jean-Gabriel Doyon

### Fixes

* install rustls CryptoProvider to prevent webserver crash ([d9aa544](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/d9aa544c0f916913d555f1f5ffe7c0f950c7286a)) by Michael Angelo Rivera

### Other

* **deps:** update rust crate jsonschema to 0.42.0 ([4900ee2](https://gitlab.com/gitlab-org/orbit/knowledge-graph/commit/4900ee2468369172dc13a5d653f22f39bf99074c)) by GitLab Renovate Bot
