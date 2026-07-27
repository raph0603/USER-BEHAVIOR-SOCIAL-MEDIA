# Changelog

## [1.22.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.21.0...v1.22.0) (2026-07-27)


### Features

* **ml:** build stage-2 sequences with a strict time split ([d016d09](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/d016d097fe88c0f18e323b29be47691de0d08cad))
* **ml:** calibrate viral probability and pick threshold out-of-fold ([81a7f36](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/81a7f36524888e2fad0f20d5c37c4c9821dc629a))
* **ml:** fuse the stage-1 prior into a stage-2 engagement model ([85e06e8](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/85e06e814b84675980292cd4b29dacaadef8b7bd))
* **ml:** resolve channel audience through the official YouTube API ([bea9835](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/bea98356d92ad5a73cc1380365b57ef7c8a1f721))


### Bug Fixes

* **ml:** correct the audience contract, calibrate the score, seed stage 2 ([eed0e0d](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/eed0e0d08a47a2b2d21d0bb31f625c3e22c41d6c))
* **ml:** hand the verdict to the report model instead of letting it guess ([e6971d4](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e6971d46b255cce4b0bc379b4b1300cc78262813))
* **ml:** keep API subscriber counts when applying channel cache ([eda2bea](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/eda2bea9eca249aa69d46a5f44b4c69f95a7df17))
* **ml:** keep the prediction when report generation fails ([12b33ae](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/12b33aed48f3bbed09eff4135a44038dca623fbe))
* **ml:** stop serving an invented zero audience ([3349f70](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3349f70e72e0941d398a6024f82fe0c2ea1b34c3))
* **ml:** trust observed audience values over coverage flags ([69d866c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/69d866c4a72aa7e791d1bd684826424f02834838))
* **test:** update xgboost version constraint in requirements ([004e418](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/004e418e9c408736c23ee3a3178cf175c445373c))
* **test:** update xgboost version constraints in requirements ([f3a1c7e](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f3a1c7e28fab6c31fe467d09dac8bbf7b725cea3))


### Performance Improvements

* **ml:** shallower trees picked on out-of-fold PR-AUC ([972bc8a](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/972bc8a3ead4124a07bee97b60d169a15214d8b9))

## [1.21.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.20.0...v1.21.0) (2026-07-24)


### Features

* **ml:** support remote inference server ([26f1ce2](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/26f1ce28d84d345fb89c9e277ddf64bf41f3575d))
* **ml:** support remote inference server ([27140d3](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/27140d30b6381bcb77cbbb5cb4b364dfd27ad8fb))

## [1.20.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.19.0...v1.20.0) (2026-07-22)


### Features

* **ml:** deploy ai-server through docker hub ([793521f](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/793521f3cd63ce410bb9630a0e8651376c4bcc4d))

## [1.19.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.18.0...v1.19.0) (2026-07-22)


### Features

* **ml:** add ai-server api (predict + report) for the stage-1 viral model ([d99f59d](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/d99f59db285449d1877e988cbc01dd26164430f4))
* **ml:** add ai-server api (predict + report) for the stage-1 viral model ([0f84f3e](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/0f84f3e829c24df206c8c244fc08a33bf6288a6e))

## [1.18.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.17.0...v1.18.0) (2026-07-22)


### Features

* **data:** add portable transfer tooling ([1c2181a](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/1c2181a58953c75d170282a2a1130bd9532da65e))
* **data:** add portable transfer tooling ([db49739](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/db4973945c4a0091e9bd0827797f2e9877cf3b58))

## [1.17.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.16.0...v1.17.0) (2026-07-21)


### Features

* **youtube:** add transcript model failover ([acea82c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/acea82cc29e74a5c765457364708286a22f06749))
* **youtube:** add transcript provider fallback ([43edd6f](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/43edd6f5d11108de2745efb28c2a3f4eeab60bff))
* **youtube:** add transcript provider fallback ([efe1847](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/efe1847d004fedcec34ad0ad9e9ba4e828dddf7c))
* **youtube:** schedule transcript gap recovery ([d51af6d](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/d51af6db50b380b7397a0c861359f2da6050cea8))


### Bug Fixes

* **analytics:** link gemini transcripts to source videos ([f4a57a5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f4a57a528d3c3e398f1410bee1e96567feb0da36))
* **dashboard:** render thumbnail URL fallback ([63ed5c5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/63ed5c54b44fcf49dc4b165fb613823137d0964f))
* **deployment:** pass Gemini config through Airflow ([49c5127](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/49c512734eb819d862e1d9b5c622ed72c0f62b16))
* **pipeline:** surface recovered transcripts in dashboard ([8343652](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/83436528a04bb9c2eb267fd4342b0cfe07f4590f))
* **youtube:** surface known comments and metadata ([e5bf9b8](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/e5bf9b85352e198d94aafa54f317eadf2dee5609))
* **youtube:** unblock transcript backfill workers ([9153adf](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/9153adfb91086b05b852b541377bb0df01d7f026))


### Performance Improvements

* **youtube:** prioritize long Gemini transcripts ([26a4b34](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/26a4b345656b7c0d5cba81aaad81785ffdd2163d))

## [1.16.0](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/compare/v1.15.0...v1.16.0) (2026-07-20)


### Features

* **dashboard:** show data freshness and coverage ([3aa72d5](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/3aa72d5b3325dc1425591d554fed391f90e8e27a)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99) [#101](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/101) [#103](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/103) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#110](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/110) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **ml:** build reproducible lakehouse datasets ([db2938c](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/db2938cf0a938d559bc5e38e4e90195282f593e5)), closes [#103](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/103)
* **monitoring:** persist quota and pipeline health ([8d01240](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/8d01240d70322d83cdea29cfdbbfc2ca7be0dc18)), closes [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **pipeline:** add bronze silver reconciliation ([7899f23](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/7899f230e0a73571e6c4be34502058a2eb733272)), closes [#102](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/102) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)
* **pipeline:** append idempotent engagement snapshots ([7604c68](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/7604c6850e91c0c7e8b3dc8a52b710cbcc636e34)), closes [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)
* **quality:** add configurable lakehouse checks ([fe3c758](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/fe3c7589407392d95e942cd1204a47c1bd4da149)), closes [#115](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/115)
* **schema:** expose provenance and coverage ([f248b01](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/f248b014ff5962ca3b20425b6ae77be3822f1252)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99) [#100](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/100) [#103](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/103) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **youtube:** add incremental metadata pipeline ([b461665](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/b46166553e0427cfb6457004eb60f42a34b7a625)), closes [#107](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/107) [#110](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/110)
* **youtube:** add worker throughput metrics ([bbb4c4f](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/bbb4c4f6e7407884ec306cb2dbd9fdef26983c39)), closes [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **youtube:** decouple enrichment workers ([c718765](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/c71876513089da3ddbd3a48284e5e8ccb42e3897)), closes [#108](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/108) [#111](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/111) [#112](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/112) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)


### Bug Fixes

* **airflow:** isolate recoverable pipeline stages ([8904b12](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/8904b12b9ebb12ade3abd2c5f9cf3e24da085912)), closes [#102](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/102) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#111](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/111) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#115](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/115)
* **analytics:** materialize applied event history ([50a33cd](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/50a33cd483559ea550e7f8f64fd5b5f4349ee7db)), closes [#101](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/101) [#102](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/102) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)
* **analytics:** refresh features from current state ([17ef322](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/17ef322736369901be0fea58719ec2e17f5adc28)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99) [#103](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/103) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)
* **ci:** restore required validation status ([32c80d6](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/32c80d639835a1b066b03f430a11e7cdf5623d06)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99)
* **pipeline:** harden end-to-end lakehouse reliability ([8897ada](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/8897adac691f962ebcc1293dbbe8a32f772e4d7f))
* **pipeline:** persist committed bronze events ([0da7274](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/0da7274245fc64530d16ddcb891ddd16a12b0ae4)), closes [#100](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/100) [#102](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/102)
* **transcripts:** model explicit collection lifecycle ([486fe54](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/486fe545bcd245fb36ce0ade0b31b4a6bdc56d82)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99) [#101](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/101) [#111](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/111) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **types:** validate modified module invariants ([522676b](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/522676bfd89f86bfd491ad1d08953d8b35b09a9d)), closes [#99](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/99) [#100](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/100) [#101](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/101) [#107](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/107) [#108](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/108) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#110](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/110) [#111](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/111) [#112](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/112) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114) [#115](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/115)
* **youtube:** batch adaptive engagement refresh ([95c10c1](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/95c10c1825dcf3228f737ed897bbaea1164bc947)), closes [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#112](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/112)
* **youtube:** enforce secondary quota budgets ([7e1bd37](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/7e1bd37bbf2224735637b2bdaa7b661bef805ecc)), closes [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)
* **youtube:** make engagement snapshots idempotent ([a1db492](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/a1db492a99d45304f12f44f6b27ae2968feb7a72)), closes [#103](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/103) [#109](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/109) [#112](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/112) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113)
* **youtube:** persist worker delivery outcomes ([0843764](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/commit/08437649b9ab6821c04efc1991803a09d3a79913)), closes [#108](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/108) [#111](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/111) [#112](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/112) [#113](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/113) [#114](https://github.com/raph0603/USER-BEHAVIOR-SOCIAL-MEDIA/issues/114)

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
