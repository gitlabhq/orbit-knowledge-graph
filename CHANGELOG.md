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
