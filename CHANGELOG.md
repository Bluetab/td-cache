# Changelog

## [3.0.0] 2019-06-23

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
