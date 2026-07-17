# Changelog

## [1.15.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.14.0...v1.15.0) (2026-07-17)


### Features

* **airflow:** schedule no-row-checks pipeline by default ([8a30377](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/8a30377a067a8de909a60102820885aab227e26f))
* **deploy:** add Docker Compose release bundle ([e47ef52](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e47ef526da446cc5adf97a03abed5b88ac7b5d44))
* **transcripts:** enforce per-video language selection ([3517e39](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3517e39c33c940924cd59b08b8c9f20c612cff3a))
* **transcripts:** enforce per-video language selection ([4d992d1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/4d992d14f6380d9ea984318fdfe2e9ecdbd22138))
* **youtube:** enrich content with thumbnails ([2d736d6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/2d736d614a674b0e085bd895be2c7d073e4ee8b4))


### Bug Fixes

* **ml:** preserve unknown engagement labels ([7fc92ca](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/7fc92cabe2b877c8dfe1cc8ec7a36fc60f4852a5))
* **pipeline:** restore metadata and transcript flow ([4082dc9](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/4082dc984676bd31f435f337a1403483cd14c593))

## [1.14.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.13.0...v1.14.0) (2026-07-10)


### Features

* **analytics:** add content entity layer ([6c54fb8](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/6c54fb877f63fd71c3c3844e74451aac84ca0561))
* **analytics:** add content entity layer ([ac77bde](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/ac77bde0a1035310343aca2bed2f74bcd164f93b)), closes [#93](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/93)
* **analytics:** backfill youtube transcripts ([4d48bf6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/4d48bf6861b58dac5e160f5dadb7f9cf6e20830b))
* **analytics:** populate content entity relationships ([9f9fbe7](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/9f9fbe7ad8d5d072e90220ecc7370d840147fecb))
* **reddit:** collect subreddit community metadata ([e8237c7](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e8237c72e09b0d072bec22728f8e45f2df0b5790))


### Bug Fixes

* **analytics:** keep reddit comments out of content text ([c9858b4](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c9858b4a40a2ecddae6767c9b7e5ef7bc43e47fc))
* **dashboard:** derive reddit subreddit filters from urls ([7286119](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/72861192d2a4bf50d976bd83f232e05a93866d9d))
* **dashboard:** show reddit community metadata ([6810e31](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/6810e31fc5b85a938a214278bc8ca1bb829a35f3))
* **reddit:** enrich community metadata fallbacks ([1741d45](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/1741d45833283b92a775881a1ebc26d2206814df))

## [1.13.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.12.0...v1.13.0) (2026-07-01)


### Features

* **ai:** ai behaviour prediction ([8fc2608](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/8fc2608848a3cc364bdab14eb923670990d616f4))
* **ml:** channel audience features + real YouTube subscriber enrichment; docs + reddit crawler fixes ([da767b8](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/da767b825b33a034e1d8855ccecfbfa00d65070b))

## [1.12.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.11.0...v1.12.0) (2026-07-01)


### Features

* **dashboard:** surface model pipeline tables ([29937a9](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/29937a994a3a70f8c81150b330ef8d1566cf09ab)), closes [#83](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/83) [#84](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/84) [#85](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/85) [#86](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/86)
* **data:** standardize social event text fields Refs [#82](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/82) ([8270281](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/82702812c5f707c6dd39558297a625e71a441234))
* **silver:** add model-ready classification layers ([3a74392](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3a7439299ec1b5ea8d5427bfcd2bdf1df27fc7e9))
* **silver:** add model-ready classification layers ([2af7ea6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/2af7ea607886eff6fd753b81f8d15bdf4bd59ed1)), closes [#83](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/83) [#84](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/84) [#85](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/85) [#86](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/86)

## [1.11.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.10.1...v1.11.0) (2026-07-01)


### Features

* collect follower, subscriber and subreddit member counts ([d5f76c9](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/d5f76c9325bbd49bc748cc8a9090b0cfde7edb30))
* **reddit:** restore engagement metadata ([03d94bd](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/03d94bd86d7790ef3449310f27087e2ee7a6939c))


### Bug Fixes

* add defensive checks to count parsers ([fdb29e1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/fdb29e152f1ffae7b2ca9d8095ecbd46f4c46a0a))
* correct lock typo and fix airflow dag test ([fe8f28d](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/fe8f28d5acb642b74d6fc5f4ee3f34c5486b2894))
* **dashboard:** clarify missing engagement metrics ([ad5b5f1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/ad5b5f1d478231d42a3907f97e0765410abb7797))
* **reddit:** backfill engagement counts from html ([379794b](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/379794ba4df00a97bc3b25c9af845e5f93258c40))

## [1.5.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.4.0...v1.5.0) (2026-06-26)

### Features

* **ml:** add bilingual cognitive_friction content feature ([a4dbd00](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a4dbd00f6f27dacd3d0ebf62eeb5d575254ae0af))
* **ml:** add bilingual cognitive_friction content feature ([a446718](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a4467180263a833b3524bcc3bc94ba08bd1daaaf))
* **ml:** add cognitive_friction static feature(English VN) ([7990289](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/79902898cd1a965bd6fea7cf47042473758dffd6))

## [1.4.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.3.0...v1.4.0) (2026-06-24)

### Features

* **crawler:** raise default collection limits ([c4ce999](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c4ce9995c493fa95bc19bcfe478151d565669c14))
* **crawler:** refresh comment metadata collection ([e303824](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e303824d2e9735f78a160217d5579c2ebf60598a))

## [1.3.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.2.0...v1.3.0) (2026-06-24)

### Features

* **dashboard:** add author progression analytics ([dd4b5c4](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/dd4b5c46a9cedc575024e0791cd1f157ab3c5955)), closes [#33](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/33)
* **dashboard:** containerize streamlit app ([c62b8d5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c62b8d579248d38a1fcee9f2e04e4239407365d2)), closes [#30](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/30)
* **dashboard:** display engagement metadata ([a1ea317](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a1ea317444aaeb6711d763ba6333abe287895873))
* **dashboard:** include youtube collaborators in user stats ([aaefa59](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/aaefa595f6ef774ee37a8c077edce6a53a13aaad))
* **dashboard:** show balancing report ([7ce894f](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/7ce894f0a47b5d613f917d8d1bd53ebbb55eaeea)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **pipeline:** add crawler insight management ([aec934f](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/aec934f413b16630d0dc97a0179125abfc7dc367)), closes [#30](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/30) [#31](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/31)
* **pipeline:** add stable refresh metadata and balancing ([55571da](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/55571dafae1ba31ab858331e74d6d008bacd1baf))
* **pipeline:** allow thousand events per run ([9c251b3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/9c251b3ac3a09ce0dfb038b0d1af62019905ee51)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **pipeline:** propagate engagement metadata ([4ddd06c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/4ddd06c073010cd7bc85734ca87012515f0eea67))
* **pipeline:** reduce engagement metric contract ([4327458](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/43274582810b283fed8951396ec22355cb81b695))
* **pipeline:** refresh and manage crawler metadata ([98cd941](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/98cd941bb32672630f3d0e63a1e5058f5261f04d))
* **pipeline:** refresh balancing report after crawl ([a256334](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a2563341225c759c917f755192658fb814c0a764)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **youtube:** collect video owner collaborators ([f6cbc15](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f6cbc15f3423037a52b599b62e2abcdc26ed9d19)), closes [#33](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/33)

### Bug Fixes

* **dashboard:** aggregate youtube metrics by video ([edb950c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/edb950c5880574790d9f0f71447aba6f09994b29)), closes [#30](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/30) [#33](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/33)
* **dashboard:** translate interface to english ([60d0ab0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/60d0ab05f00e3b20db605e22da654a0efd657621)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **insights:** skip unavailable social refresh endpoints ([a2c563b](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a2c563bc4da82b78ebb8553fc7bf512a376674b2)), closes [#31](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/31) [#33](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/33)
* **orchestrator:** recover orphaned pipeline locks ([0a7a940](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/0a7a9400eaba002cb592e0dfc893288a49ae203a))
* **pipeline:** align producer with metric contract ([adc70bf](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/adc70bf8e44efba893f9e965706cf8d7d52628e6))
* **pipeline:** balance dataset by source ([e7ca0dd](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e7ca0dd3608f23f2e127bed63a00f2d85e282a67)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **pipeline:** skip x crawl when cdp is unavailable ([f373675](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f373675909d2ec00a026162bc0e99b303232a931)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **pipeline:** tolerate malformed avro events ([c80b205](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c80b205a508e4cb74e1e61494df91cb182de8d66)), closes [#34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/34)
* **x:** extract post views from analytics links ([45445a0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/45445a0f3712a4433f1241ffdcea6a4fd0979016))

## [1.2.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.1.0...v1.2.0) (2026-06-12)

### Features

* **dashboard:** add dashboard application ([726ddb2](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/726ddb245b65ab8b1e8543e13edba18f65c7fb8d))
* **dashboard:** add Iceberg-backed social media dashboard ([b38e27b](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b38e27b64eae95a28edcde4f904f39638b239a11))
* **dashboard:** add sample social media data ([a6d1b7d](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a6d1b7d1ef6907ab8a588c216b76fb2b56bbb6af))
* **dashboard:** read events from Iceberg Silver ([1adc6bc](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/1adc6bc9c1a4b66214d3957fe70033f0e4f76f76))

### Bug Fixes

* **dashboard:** resolve data paths with pathlib ([ff7e28c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/ff7e28c6171d48d497ae70da37942466619dcc7e))

## [1.1.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.0.1...v1.1.0) (2026-06-11)

### Features

* add resilient scheduled pipeline variant ([10fe3b1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/10fe3b1fb9a0f4801d51073db5b97a580a0f30d2))
* add X crawler reply collection ([3bcf2a3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3bcf2a3cf3180c5b9ce6dd201e3c18da489518b5))
* add X crawler reply collection ([5fa45eb](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/5fa45ebe2c2fc623bb44f8e938e0d3a5e8fe2330))
* configure scheduled collection from Airflow ([b455c39](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b455c3970e786752e658c58e6d90388d044e13d5))
* ingest API and scraped social data through the lakehouse pipeline ([f7e1d31](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f7e1d31269c1d5fe92e3e9d6cec865219da29eb8))
* integrate online social collectors with lakehouse ([2c66403](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/2c66403f6937f2d2843f627f04569999c996c86d))
* **maintenance:** add transactional Iceberg compaction ([c4a7878](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c4a7878d193d876b7a049ad801d629b3f44d8f37))
* **orchestrator:** add social_clean_pipeline DAG (clean jobs + ingestion in parallel) ([28a07e5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/28a07e58004f6674c104effa3ba97f2d24da7745))
* **pipeline:** add DLQ report + CSV replay tool; DLQ holds only real errors ([e0aa438](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e0aa438f7f317d50d9cade71b51d670dddb48d52))
* **pipeline:** add privacy lakehouse orchestration ([35f2aca](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/35f2acae8473b8d376da7262bedf1eba061c7be6))
* **pipeline:** anonymize PII in text (email, mention, phone) for ethical AI ([fb070b6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/fb070b685fb4f3ff5addb079596d555da82fa619))
* **pipeline:** clean and anonymize data before bronze ([b5a7907](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b5a7907ca274a590890c5abac2bbd553b5ad152f))
* **pipeline:** handle reddit and x in multi-platform clean pipeline ([55eac58](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/55eac5846fbdde1393bf8f8247e368fcb57355b8))
* **pipeline:** multi-platform clean pipeline + orchestrator DAG (US3) ([9b8aace](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/9b8aace9a1dff83bf7530b895a12a96faa297361))
* **pipeline:** scaffold yotube data propresscing pipeline ([3eb50e3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3eb50e33d9324dc008b5400c65cce3185c3f3128))
* **pipeline:** youtube clean+DLQ pipeline verified end-to-end ([74fd59e](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/74fd59e00f11a74f22da6ae56fdcd4623ad1b671))
* **scrapers:** add crawlers for Youtube, Reddit and X for EV project ([51783df](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/51783df53182ea307db60e042cae87f293b5ed7e))
* test commitlint check ([b83d261](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b83d26192904fceccb8139ab730f679b01e3f1dd))
* test commitlint check ([6bca401](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/6bca401fecff5f8072da6d7a1ec12e52a6cef7cd))

### Bug Fixes

* .gitignore ([ace28b0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/ace28b04983b407feaca8087c4feda770f0090f7))
* **ci:** allow core commit type ([13e7282](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/13e728281aa8100066170dbd71260124617ae2f6))
* **x:** collect complete post text ([b53bc51](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b53bc517b2220ce8a72c42f9ad0c9aab45d28f35))
* **x:** stabilize Edge CDP collection ([a8cc3de](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a8cc3dec5b47b97c0d4cfea56ce3d343a39ab9b3))

## [1.0.1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.0.0...v1.0.1) (2026-06-10)

### Bug Fixes

* .gitignore ([ace28b0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/ace28b04983b407feaca8087c4feda770f0090f7))

## 1.0.0 (2026-06-10)

### Features

* add X crawler reply collection ([3bcf2a3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3bcf2a3cf3180c5b9ce6dd201e3c18da489518b5))
* add X crawler reply collection ([5fa45eb](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/5fa45ebe2c2fc623bb44f8e938e0d3a5e8fe2330))
* **orchestrator:** add social_clean_pipeline DAG (clean jobs + ingestion in parallel) ([28a07e5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/28a07e58004f6674c104effa3ba97f2d24da7745))
* **pipeline:** add DLQ report + CSV replay tool; DLQ holds only real errors ([e0aa438](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e0aa438f7f317d50d9cade71b51d670dddb48d52))
* **pipeline:** add privacy lakehouse orchestration ([35f2aca](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/35f2acae8473b8d376da7262bedf1eba061c7be6))
* **pipeline:** anonymize PII in text (email, mention, phone) for ethical AI ([fb070b6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/fb070b685fb4f3ff5addb079596d555da82fa619))
* **pipeline:** handle reddit and x in multi-platform clean pipeline ([55eac58](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/55eac5846fbdde1393bf8f8247e368fcb57355b8))
* **pipeline:** multi-platform clean pipeline + orchestrator DAG (US3) ([9b8aace](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/9b8aace9a1dff83bf7530b895a12a96faa297361))
* **pipeline:** scaffold yotube data propresscing pipeline ([3eb50e3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3eb50e33d9324dc008b5400c65cce3185c3f3128))
* **pipeline:** youtube clean+DLQ pipeline verified end-to-end ([74fd59e](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/74fd59e00f11a74f22da6ae56fdcd4623ad1b671))
* test commitlint check ([b83d261](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b83d26192904fceccb8139ab730f679b01e3f1dd))
* test commitlint check ([6bca401](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/6bca401fecff5f8072da6d7a1ec12e52a6cef7cd))

### Bug Fixes

* **ci:** allow core commit type ([13e7282](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/13e728281aa8100066170dbd71260124617ae2f6))
