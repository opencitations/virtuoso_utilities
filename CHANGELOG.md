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
