# Changelog

## [4.1.0] 2020-07-15

### Added

- [TD-2774] Structure types cache 

## [4.0.0] 2020-07-01

### Changed

- Update `redix` to version 0.11.1

## [3.25.2] 2020-07-01

### Added

- [TD-2585] `IngestCache.get_domain_ids/1` returns domain and parent domain ids
  of a cached ingest

## [3.25.1] 2020-06-26

### Added

- [TD-2779] Support for publishing batched audit events

### Removed

- [TD-2779] Removed deprecated module `RuleResultCache`

## [3.25.0] 2020-06-17

### Changed

- [TD-2672] Read confidential value from concept instead of content

## [3.24.3] 2020-06-10

### Fixed

- [TD-2637] Timestamp on audit events should always be current time when
  publishing

## [3.24.2] 2020-06-03

### Added

- [TD-2261] `deleted_at` filed on structure cache

## [3.24.1] 2020-06-02

### Added

- New function `TdCache.UserCache.map/0` returns a map of cached users

## [3.24.0] 2020-06-02

### Added

- [TD-2637] Support for publishing audit events in a common format

## [3.22.1] 2020-05-12

### Fixed

- [TD-2490] Invalidate local cache when a domain is put or deleted

## [3.22.0] 2020-05-12

### Added

- [TD-2495] Cached structures metadata in Structure Cache

## [3.20.2] 2020-04-14

### Added

- [TD-2361] Permission `manage_raw_quality_rule_implementations`

## [3.20.1] 2020-04-13

### Changed

- Avoid redundant reads in `ConceptCache.get/3` on `:content` property

## [3.20.0] 2020-04-08

### Changed

- Requires elixir>=1.8

### Added

- `UserCache.list/1` to list all cached users

### Fixed

- `TemplateCache.fields_by_type!/2` was always returning an empty map

## [3.19.2] 2020-04-02

### Changed

- [TD-1691] `RuleCache.put/1` updates concept linked to a rule on
  rule creation and modification

## [3.19.1] 2020-03-25

### Changed

- [TD-2364] `DomainCache.put/1` emits event `domain_created` and
  `domain_updated` on domain creation or modification, respectively.

## [3.19.0] 2020-03-25

### Changed

- [TD-2365] `ConceptCache.get` now supports `refresh` option to force read from
  redis

## [3.18.0] 2020-03-17

### Added

- [TD-2328] Permissions `view_dashboard` and `view_lineage`

## [3.16.1] 2020-02-12

### Added

- [TD-2328] External id in taxonomy cache

## [3.16.0] 2020-02-12

### Changed

- [TD-2235] `TemplateCache.put` can now be passed keyword options

## [3.15.0] 2020-02-04

### Removed

- [TD-1595] `StructureCache.get_external_id/2` removed

## [3.9.1] 2019-10-25

### Added

- [TD-2187] `StructureCache`: `force` option in `put/2`

### Changed

- [TD-2187] `StructureCache`: store structure `external_id` and `updated_at`

## [3.9.0] 2019-10-22

### Added

- [TD-2170] permission to load metadata in dd

## [3.8.0] 2019-10-10

### Added

- [TD-1859] `ConceptCache`: Store content in key `business_concept:{id}:content`
- [TD-1721] `TemplateCache`: Emit `template_updated` event instead of
  `add_template` when a template is updated

## [3.7.3] 2019-10-07

### Changed

- [TD-1721] Only update template if `updated_at` has changed

## [3.7.2] 2019-10-01

### Added

- [TD-1721] `add_template` event when a template is put

## [3.7.1] 2019-09-25

### Added

- [TD-2084] `RuleCache.keys/0` to list rule keys in cache

## [3.7.0] 2019-09-16

### Added

- [TD-2090], [TD-2091] New permission `link_data_structure`

## [3.5.1] 2019-09-03

### Fixed

- [TD-2074] Changed `LinkCache.count` target to `data_structure`
- [TD-2075] Evict concept from local cache on put (`rule_count` was not
  reindexed correctly)
- [TD-2081] Event stream consumer did not respect `redis_host` and `port` config
  options

## [3.5.0] 2019-08-26

### Added

- [TD-1907] Rule results cache

## [3.4.0] 2019-08-16

### Added

- [TD-2044] New permission `execute_quality_rule`

## [3.3.3] 2019-08-01

### Changed

- [TD-2009] Changed `data_fields:external_ids` set for
  `structures:external_ids:{system_external_id}` set

## [3.3.2] 2019-07-26

### Changed

- [TD-1991] Stream consumers now each use a dedicated connection with Redis

## [3.3.1] 2019-07-25

### Changed

- [TD-2027] Performance improvements for `UserCache` and `TaxonomyCache`

## [3.3.0] 2019-07-24

### Added

- [TD-1776] New permission `view_quality_rule`
- [TD-1776] Set of confidential business concept ids
- [TD-1775] New permission `manage_quality_rule_implementations`

## [3.2.3] 2019-07-22

### Changed

- Performance improvements for `TemplateCache` and `ConceptCache`
- Support configurable port for Redis
- Publish `delete_link` command when a link is deleted

## [3.2.1] 2019-07-18

### Changed

- [TD-2002] Add static permissions config

## [3.2.0] 2019-07-15

### Changed

- [TD-1532] Retrieve linked ingests from `LinkCache`

## [3.0.5] 2019-06-27

### Added

- [TD-1782] Emit a `migrate_field` event when a new field is cached
- [TD-1782] Emit an `unlink_field` event for fields without `structure_id`

### Fixed

- Issue with `ConceptCache.put_active_ids/1` when keys do not exist in Redis

## [3.0.0] 2019-06-26

### Added

- Migrated and refactored cache modules from TdPerms
- Redis event stream consumer
- Use connection pool for Redis

## [2.21.1] 2019-06-06

### Changed

- Avoid starting Redix and Logger automatically except in test

## [2.21.0] 2019-06-06

### Added

- `TdCache.CacheCleaner` gen server for performing cleanup of deprecated cache
  entries
