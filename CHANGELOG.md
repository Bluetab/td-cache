# Changelog

## Unreleased

### Added

- [TD-2044] Permission execute_quality_rule

## Unreleased

### Changed

- [TD-2009] Changed data_fields:external_ids set for structures:external_ids:{system_external_id} set

## [3.3.2] 2019-07-26

### Changed

- [TD-1991] Stream consumers now each use a dedicated connection with Redis

## [3.3.1] 2019-07-25

### Changed

- [TD-2027] Performance improvements for `UserCache` and `TaxonomyCache`

## [3.3.0] 2019-07-24

### Added

- [TD-1776] Permission view_quality_rule
- [TD-1776] Set of confidential business concept ids
- [TD-1775] Permission manage_quality_rule_implementations

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
- [TD-1782] Emit an `unlink_field` event for fields without structure_id

### Fixed

- Issue with ConceptCache.put_active_ids/1 when keys do not exist in Redis

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

- `TdCache.CacheCleaner` gen server for performing cleanup of deprecated cache entries
