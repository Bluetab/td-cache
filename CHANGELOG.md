# Changelog

## [4.45.0] 2022-05-26

### Changed

- [TD-4482] `ConceptCache.get/2` includes `link_tags` in response containing the
  set of tags on links to structures

## [4.44.0] 2022-05-18

### Changed

- [TD-4230] moved `td-df` template preprocessing logic to `td-cache`

## [4.40.5] 2022-03-17

### Added

- [TD-4623] Allow multiple domain ids for has_role?/3

## [4.40.4] 2022-03-11

### Added

- [TD-4271]
  - New permission `link_implementation_business_concept`
  - Support for `ImplementationCache`

## [4.40.3] 2022-03-11

### Changed

- [TD-4500] Structure cache entries now have `domain_ids` instead of `domain_id`
- Replaced `SystemCache.external_id_to_id_map/0` with
  `SystemCache.get_by_external_id/1`

## [4.40.2] 2022-03-09

### Removed

- `DomainCache.put_domain/2` no longer publishes `domain_updated` and
  `domain_created` events to the `domain:events` stream
- Removed deprecated function `TaxonomyCache.domain_map/0`

### Changed

- Renamed `Permissions.has_any_permission_on_resource_type?/2` to
  `has_any_permission?/2`
- Cached domain entries now include the `parent_id` prop
- `TaxonomyCache.get_domain/1` no longer includes `parent_ids` and
  `descendent_ids`

## [4.40.0] 2022-03-07

### Changed

- [TD-4491] Refactored permissions model to resolve permissions within the
  taxonomy tree (i.e. to consider permissions not only in the specific domain
  being queried, but also in its parent domains). The
  `Permissions.has_permission?/2` and `Permissions.has_permission?/4` functions
  now also considers global permissions (the permissions of the default role)
  before querying session-specific permissions.
- `CacheCleaner` is no longer a `GenServer`. Instead, use `CacheCleaner.clean/1`
  (e.g. from a scheduled task)

### Removed

- The `PermissionsConfig` module is no longer used and has been removed
- The following unused and deprecated functions have been removed:
  - `DomainCache.id_to_parent_ids_map/0`
  - `Permissions.get_acls_by_resource_type/2`
  - `TaxonomyCache.get_descendent_ids/1`
  - `TaxonomyCache.get_domain_name_to_id_map/0`
  - `TaxonomyCache.get_domain_external_id_to_id_map/0`
  - `TaxonomyCache.get_name/1`
  - `TaxonomyCache.get_parent_ids/1`
  - `TaxonomyCache.get_root_domain_ids/0`

## [4.39.0] 2022-03-04

### Added

- [TD-4378] Add structures description

## [4.38.1] 2022-02-22

### Added

- [TD-4481] New permission `manage_business_concepts_domain`

## [4.38.0] 2022-02-17

### Added

- [TD-4437] New manage_rule_results permission

## [4.37.0] 2022-01-10

### Added

- [TD-4456] Add user external_id to id mapping (**removes [TD-4212]**)

## [4.31.1] 2021-10-27

### Fixed

- [TD-4212] Remove ACLs after user deletion

## [4.31.0] 2021-10-21

### Fixed

- [TD-4203] Fixed `TdCache.Permissions.put_permission_roles` DEL expression

## [4.28.1] 2021-09-23

### Added

- [TD-4076] New permission `approve_grant_request`
- [TD-4076] Support for querying permissions in taxonomy hierarchy

## [4.28.0] 2021-09-20

### Added

- [TD-3780] `TaxonomyCache` persists `descendent_ids` and
  `TaxonomyCache.put_domain/1` forces update if specified

## [4.26.3] 2021-08-11

### Added

- `UserCache.exists?/1` determines whether a given `user_id` exists in the cache

## [4.26.2] 2021-08-09

### Added

- `DomainCache.external_id_to_id/1` to obtain the id of a domain with a given
  `external_id`

## [4.26.1] 2021-08-09

### Added

- [TD-3982] New permission `create_grant_request`

## [4.26.0] 2021-08-04

### Removed

- [TD-3952] `StructureTypeCache` is no longer used

## [4.25.4] 2021-07-26

### Fixed

- `UserCache.delete/1` was not removing entry from `users:user_name_to_id`

## [4.25.3] 2021-07-23

### Added

- [TD-3878] Added `domain_id` to structures cache

## [4.25.2] 2021-07-22

### Added

- [TD-3873] New permission `share_with_domain`

## [4.25.1] 2021-07-22

### Added

- [TD-3947] `UserCache.get_by_user_name` return a user by the user_name

## [4.25.0] 2021-07-20

### Added

- [TD-3945] New permissions for view and manage grants

## [4.24.0] 2021-07-09

### Added

- [TD-3894] `UserCache.id_to_email_map/0` returns a map of user ids with email
  addresses

## [4.23.2] 2021-06-18

### Added

- [TD-3842] `ConceptCache.is_confidential?/1` checks whether a specified id is
  confidential

### Removed

- [TD-3842] `ConceptCache.confidential_ids/0` is no longer used

## [4.23.1] 2021-06-17

### Added

- [TD-3720] New permission `manage_structures_domain`

## [4.23.0] 2021-06-17

### Added

- [TD-3522] Add permissions for Structure Notes management

## [4.22.1] 2021-06-15

### Added

- [TD-3447] Check permission over a list of domains

## [4.22.0] 2021-06-07

### Changed

- [TD-3837] `LinkCache.linked_source_ids/2` replaces `LinkCache.link_count_map/2`

## [4.20.1] 2021-05-13

### Added

- [TD-3526] `link_data_structure_tag` permission to link a structure to a tag
- [TD-3611] `LinkCache.link_count_map/2` and `TaxonomyCache.domain_map/0`

## [4.19.1] 2021-04-22

### Removed

- [TD-3526] `ImplementationCache` is no longer needed

## [4.19.0] 2021-04-20

### Changed

- [TD-3526] Allow each event stream consumer to specify its own `group`

## [4.17.0] 2021-04-07

### Removed

- [TD-3498] `SourceCache` is no longer used

## [4.16.0] 2021-03-10

### Added

- [TD-2951] Permission `profile_structure`

## [4.15.0] 2021-03-05

### Added

- [TD-3063] ConceptCache: added type field for business concept

### Changed

- ConceptCache: moved `content` property inside HashMap `business_concept:#{id}` instead of having its own key `business_concept:#{id}:content`

## [4.14.0] 2021-01-19

### Added

- [TD-3268] Get source id from external id

## [4.13.0] 2021-02-01

### Added

- [TD-3146] Collection of deleted domain ids

## [4.12.2] 2021-01-26

### Fixed

- [TD-3248] `ImplementationCache.referenced_structure_ids` was not returning
  referenced structure ids

## [4.12.1] 2021-01-13

### Changed

- [TD-3182] Allow to use redis with password

## [4.12.0] 2021-01-13

### Added

- [TD-2591] Include the number of related concepts

## [4.11.1] 2020-12-23

### Added

- [TD-3103] `ImplementationCache` for caching rule implementations

### Changed

- Improvements to `StructureCache.referenced_ids/0`

## [4.11.0] 2020-12-21

### Change

- [TD-3155] Allow to get parent ids of a domain directly from cache

## [4.9.1] 2020-11-27

### Added

- [TD-2486] Permissions `manage_data_sources` and `manage_configurations`

## [4.9.0] 2020-11-27

### Added

- [TD-3066] Collection to store deleted structures

## [4.7.1] 2020-11-03

### Added

- [TD-2952] As a user with permission to execute rules I want to run implementations manually
  from the implementations screen for Kubernetes Cluster. Cached sources

## [4.7.0] 2020-10-20

### Changed

- [TD-3047] Permission `execute_quality_rule` to `execute_quality_rule_implementations`

## [4.6.0] 2020-10-19

### Added

- [TD-2485] New function `SystemCache.external_id_to_id_map/0` to return a map of systems which key is the `external_id`

## [4.5.0] 2020-09-24

### Added

- [TD-2988] New function `StructureCache.referenced_ids/0` to return a list of
  structure ids referenced in rule or link cache entries

### Fixed

- [TD-2988] Structure cache was not updating if `deleted_at` changed

### Removed

- Deleted deprecated module `TdCache.FieldCache`

## [4.2.0] 2020-07-20

### Changed

- [TD-2280] get domain ids in cache

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
