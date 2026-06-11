# Changelog

<<<<<<< HEAD
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

=======
>>>>>>> origin/production
## 1.0.0 (2026-06-10)


### Features

<<<<<<< HEAD
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
=======
* **pipeline:** add privacy lakehouse orchestration ([35f2aca](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/35f2acae8473b8d376da7262bedf1eba061c7be6))
>>>>>>> origin/production


### Bug Fixes

* **ci:** allow core commit type ([13e7282](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/13e728281aa8100066170dbd71260124617ae2f6))
