# [1.10.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.9.1...v1.10.0) (2025-12-15)


### Features

* add ThreadCleanupInterval and ResourcesCleanupInterval parameters [release] ([36c40ba](https://github.com/opencitations/virtuoso_utilities/commit/36c40ba3174e1b64ab2e77e48a151ee92462364b))

## [1.9.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.9.0...v1.9.1) (2025-12-15)


### Bug Fixes

* write all VIRT parameters to virtuoso.ini for redundancy [release] ([739a625](https://github.com/opencitations/virtuoso_utilities/commit/739a625faa55d330219afc656031c0e5ba5ac8a0))

# [1.9.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.8.3...v1.9.0) (2025-12-15)


### Bug Fixes

* **benchmarks:** fail tests when SPARQL endpoint is unreachable ([f6e8359](https://github.com/opencitations/virtuoso_utilities/commit/f6e83599ef0c79c3ef8323af858c8d41df052c05))
* **docs:** add benchmark results image to documentation ([b2c8093](https://github.com/opencitations/virtuoso_utilities/commit/b2c80939af2526c806fc33adaba4bef78c484e76))
* prevent lock contention with AdjustVectorSize=0 and frequent checkpoints [release] ([b5fe651](https://github.com/opencitations/virtuoso_utilities/commit/b5fe65126de9793a8ef8e1ff59a9cc01ee2fc48d))
* **tests:** exclude benchmarks from default test run ([46ac637](https://github.com/opencitations/virtuoso_utilities/commit/46ac6372dc77982880c45e333faea5132cbab41f))
* **tests:** update VectorSize tests to match new configuration [release] ([6fdeaac](https://github.com/opencitations/virtuoso_utilities/commit/6fdeaac130082c7b4be7eebb05cd70794572d608))


### Features

* **benchmarks:** add parallel SPARQL query benchmarks ([51de91a](https://github.com/opencitations/virtuoso_utilities/commit/51de91a71e88ea646c2f51af8ddeb03fcb2cb404))

## [1.8.3](https://github.com/opencitations/virtuoso_utilities/compare/v1.8.2...v1.8.3) (2025-12-07)


### Bug Fixes

* **isql_helpers:** remove verbose error prints from run_isql_command ([55f205f](https://github.com/opencitations/virtuoso_utilities/commit/55f205f6888b8f711368174a1ea382201fa5644d))

## [1.8.2](https://github.com/opencitations/virtuoso_utilities/compare/v1.8.1...v1.8.2) (2025-12-05)


### Bug Fixes

* **native_entrypoint:** write threading parameters directly to virtuoso.ini ([a4dd7c0](https://github.com/opencitations/virtuoso_utilities/commit/a4dd7c008074241b9ae43d5a208113c7df039723))

## [1.8.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.8.0...v1.8.1) (2025-12-05)


### Bug Fixes

* **native_entrypoint:** use container memory limit and pass VIRT env vars ([3089483](https://github.com/opencitations/virtuoso_utilities/commit/308948327ea9010452e3a309c43084cc4586cb53))

# [1.8.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.7.0...v1.8.0) (2025-12-05)


### Features

* **native_entrypoint:** add docker-compose compatible entrypoint ([6b89c69](https://github.com/opencitations/virtuoso_utilities/commit/6b89c69485eb5f7513213f40740bc3fd03618782))

# [1.7.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.6.1...v1.7.0) (2025-12-05)


### Features

* **launch_virtuoso:** add query parallelization and memory optimization ([10c0803](https://github.com/opencitations/virtuoso_utilities/commit/10c0803f8eab2a6ebbd22c4c02652d57f0a302a7))

## [1.6.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.6.0...v1.6.1) (2025-11-30)


### Bug Fixes

* **release:** remove redundant build step before semantic-release ([5ddade2](https://github.com/opencitations/virtuoso_utilities/commit/5ddade2c02218071b8c4b111aa99ee47d9fff024))

# [1.6.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.5.3...v1.6.0) (2025-11-30)


### Bug Fixes

* **launch_virtuoso:** fix launch_virtuoso.py to remove deprecated capture parameter ([c77b4fe](https://github.com/opencitations/virtuoso_utilities/commit/c77b4fea4ad3f7cce5d5b22d7aaef66591cb4b70))
* **release:** update semantic-release config for uv ([20edc20](https://github.com/opencitations/virtuoso_utilities/commit/20edc2031c2123d8d1d0a6123bdca3ac274a8be9))


### Features

* **launch_virtuoso:** add programmatic API for launch_virtuoso function ([eff8acf](https://github.com/opencitations/virtuoso_utilities/commit/eff8acf16c85f9cfcb42909583e3b028591492c3))

## [1.5.3](https://github.com/opencitations/virtuoso_utilities/compare/v1.5.2...v1.5.3) (2025-11-26)


### Bug Fixes

* **ci:** require tests to pass before release workflow ([8211d40](https://github.com/opencitations/virtuoso_utilities/commit/8211d401cbd30756834a891a8588aa1502216259))

## [1.5.2](https://github.com/opencitations/virtuoso_utilities/compare/v1.5.1...v1.5.2) (2025-11-26)


### Bug Fixes

* **bulk_load:** clean up load_list table before bulk load ([97d982a](https://github.com/opencitations/virtuoso_utilities/commit/97d982a4039f3045f49c8bb09f23182af4c1e23f))

## [1.5.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.5.0...v1.5.1) (2025-11-24)


### Bug Fixes

* **bulk_load:** clean up load_list table after successful bulk load ([31bb2b4](https://github.com/opencitations/virtuoso_utilities/commit/31bb2b47b0b4e6513327fd27c349cffd6f899275))

# [1.5.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.4.1...v1.5.0) (2025-11-24)


### Bug Fixes

* **bulk_load:** [release] remove non-essential functionality and defensive checks ([1874e27](https://github.com/opencitations/virtuoso_utilities/commit/1874e27bb2e1f625f36b4c3f901088ebf9a1e88b))
* **bulk_load:** replace print statements with configurable logging system ([5d5baca](https://github.com/opencitations/virtuoso_utilities/commit/5d5bacabfb6c5be35b25755f802d80145579cc11))


### Features

* make bulk_load importable as Python library and add test suite ([be9d3e4](https://github.com/opencitations/virtuoso_utilities/commit/be9d3e4098a765cd67ec5c42612080e56d4951a2))


### Performance Improvements

* **tests:** optimize integration test execution time ([f42440a](https://github.com/opencitations/virtuoso_utilities/commit/f42440a8ebdcefd25f893ba9affdd4a4a6132759))

## [1.4.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.4.0...v1.4.1) (2025-11-20)


### Bug Fixes

* **launch_virtuoso:** [release] add Docker memory reservation and optimize Virtuoso buffer calculation ([322e39b](https://github.com/opencitations/virtuoso_utilities/commit/322e39b2b54779337a9af076e3991815108a2149))

# [1.4.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.3.1...v1.4.0) (2025-10-29)


### Features

* assign DOI on Zenodo [release] ([ce81354](https://github.com/opencitations/virtuoso_utilities/commit/ce813542de75bed8afc1f1d57618ae5bfdbeb1e8))

## [1.3.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.3.0...v1.3.1) (2025-09-16)


### Bug Fixes

* **virtuoso:** enforce SQL_QUERY_TIMEOUT and SQL_TXN_TIMEOUT to 0 at launch [release] ([f68b7a1](https://github.com/opencitations/virtuoso_utilities/commit/f68b7a19baeb9ecb6a81110bbba42a8f0192f0e3))

# [1.3.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.2.1...v1.3.0) (2025-07-20)


### Features

* [release] Add options for specifying Virtuoso Docker image version and SHA ([3cc43b0](https://github.com/opencitations/virtuoso_utilities/commit/3cc43b0e0a4971b20db34474cfc4c7e995debe8b))

## [1.2.1](https://github.com/opencitations/virtuoso_utilities/compare/v1.2.0...v1.2.1) (2025-07-19)


### Bug Fixes

*  [release] Full-text index rebuild utility now can work inside of a container ([6942aaa](https://github.com/opencitations/virtuoso_utilities/commit/6942aaa77eb6b608d272821c4a59eb8022009c9c))

# [1.2.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.1.0...v1.2.0) (2025-07-04)


### Features

* Add `--enable-write-permissions` option to allow write access for 'nobody' and 'SPARQL' users ([830e99a](https://github.com/opencitations/virtuoso_utilities/commit/830e99a6c7d4738c2a74937407e1368cbcb6cf72))

# [1.1.0](https://github.com/opencitations/virtuoso_utilities/compare/v1.0.0...v1.1.0) (2025-07-04)


### Features

* Add `--network` option for specifying Docker network connections. ([190ee23](https://github.com/opencitations/virtuoso_utilities/commit/190ee23bbab3770f7b95a0b88c06ceacfbf38324))

# 1.0.0 (2025-06-03)


### Bug Fixes

* improve bulk load status checking with detailed file statistics and clearer error reporting ([6d127b1](https://github.com/opencitations/virtuoso_utilities/commit/6d127b1cd65c0c3bfc397a083645c858dac45061))
* **launch_virtuoso:** Enhance memory settings update in virtuoso.ini ([a28dd1e](https://github.com/opencitations/virtuoso_utilities/commit/a28dd1ed0179df15269dfd55b60aa7a931cd1fc7))
* Update dependencies and enhance memory configuration in Virtuoso utilities ([8ac7375](https://github.com/opencitations/virtuoso_utilities/commit/8ac7375b91b849e5c401a7bd526a14f614f4dbde))
* update release configuration to use master branch instead of main ([7c1c17a](https://github.com/opencitations/virtuoso_utilities/commit/7c1c17aa30770e24420ee8aec7e7f0a6d9c07475))
* update release workflow branch from main to master ([00b3ae2](https://github.com/opencitations/virtuoso_utilities/commit/00b3ae21347662ec1e7a6cb80fce24b6af1bb044))


### Features

* add command-line scripts for Virtuoso utilities and enhance README documentation ([d1f3e7c](https://github.com/opencitations/virtuoso_utilities/commit/d1f3e7cdb243d468895ddd07c36ec7c841def4f6))
* add quadstore dump utility and fix Virtuoso launch script ([b249f59](https://github.com/opencitations/virtuoso_utilities/commit/b249f59c2809451ba42883371181b66c86cef8fb))
* add utility to rebuild Virtuoso full-text index for bif:contains queries ([80afb0e](https://github.com/opencitations/virtuoso_utilities/commit/80afb0eef8acbe534a50379a6e11f205d6ee2ee0))
* Implement a step to clear the `DB.DBA.load_list` before processing ([4687c2f](https://github.com/opencitations/virtuoso_utilities/commit/4687c2f6986983e2cd1ecb0df29fb661581fd16f))
* implement automatic MaxCheckpointRemap configuration for Virtuoso ([53bd4f1](https://github.com/opencitations/virtuoso_utilities/commit/53bd4f11131322f2fba7d1035e8e66a8a80dc404))
* Initialize project structure for Virtuoso Utilities ([d0feabc](https://github.com/opencitations/virtuoso_utilities/commit/d0feabc458597d8cc42bd6fb7aaa3ec2fa5c374a))
* Refactor N-Quads loader, enhance launcher config, and update docs ([5216c2c](https://github.com/opencitations/virtuoso_utilities/commit/5216c2c182fa9cd4e35e23fd1d390eaaaf718375))
* Revise `bulk_load_parallel.py` to utilize Virtuoso's built-in bulk loading procedures ([00f0d01](https://github.com/opencitations/virtuoso_utilities/commit/00f0d01a8adc793177dee489bc1b5d1427eee94d))
* Transition to sequential loading of N-Quads Gzipped files in `bulk_load_parallel.py` ([9ff7150](https://github.com/opencitations/virtuoso_utilities/commit/9ff71506e07e7efc622be5a8fd959d47b358b50c))
