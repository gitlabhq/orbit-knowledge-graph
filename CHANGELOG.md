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
