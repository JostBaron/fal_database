# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`jbaron/fal_database` is a TYPO3 extension that adds a FAL (File Abstraction Layer) storage driver keeping files and folders as rows in a database table instead of on disk. It is one of Jost's personal open-source extensions: namespace `Jbaron\FalDatabase\`, `origin` is GitHub (`JostBaron/fal_database`), not Gerrit. Push normally. There is no `refs/for/` magic ref and no `Change-Id` trailer. Commit subjects still use the `[TASK]` / `[BUGFIX]` / `[CHORE]` prefixes. Never add `Co-Authored-By:` lines (or any other AI-attribution trailer) to commit messages.

The Mein-Bauernhof sites consume it through Composer. The workspace-level CLAUDE.md (`../../CLAUDE.md`) covers how it fits in.

## Commands

Everything runs in Docker through `Build/Scripts/runTests.sh`. PHP runs in TYPO3's `ghcr.io/typo3/core-testing-php81` image, not on the host, which has PHP 8.3 and no `pdo_mysql`. `vendor/`, `public/` and `composer.lock` are gitignored.

```bash
Build/Scripts/runTests.sh -s composerUpdate -t 11     # dev dependencies for TYPO3 11.5 (-t 12: TYPO3 12.4)
Build/Scripts/runTests.sh                              # functional tests against PostgreSQL (production DBMS)
Build/Scripts/runTests.sh -d mariadb                   # ... against MariaDB
Build/Scripts/runTests.sh -- --filter renameFile       # single test / class; args after -- go to phpunit
Build/Scripts/runTests.sh -s lint                      # php -l over Classes, Configuration, Tests
```

The functional suite runs against whichever TYPO3 version `composerUpdate` installed last, and prints it first. `composerUpdate` constrains the core with `composer update --with`, so `composer.json` stays unchanged. Run the suite on both versions before committing driver changes.

Every run of the functional suite starts a fresh database container on tmpfs, with fsync, doublewrite and binlog switched off. The container and its Docker network are removed when the run ends. The tests are `typo3/testing-framework` functional tests in `Tests/Functional/`, configured by `Build/phpunit/FunctionalTests.xml`. All of them extend `AbstractFunctionalTestCase`, which creates database storage UID 1. It also provides `insertEntries()` and `getEntryIdentifiers()`/`getEntryData()`, which seed and inspect `tx_jbaron_faldatabase_entry` directly, so a test of one driver method doesn't depend on others. There are no unit tests.

`ExistenceCacheTransactionTest` configures `TransientMemoryBackend` for the existence cache on purpose. With TYPO3's default database cache backend, cache writes happen in the same transaction as the data and get rolled back with it, which hides stale-cache bugs that a non-transactional backend like Redis exposes.

Composer 2.9+ blocks installing versions with security advisories by default, and every TYPO3 11.5/12.4 release has some. Jost decided to allow them for the dev install only: `composerUpdate` passes `--no-blocking`. Don't add a `policy` config to `composer.json`.

Test fixtures added through `ResourceStorage` must pass TYPO3 12's resource consistency check: the extension must be on the allow-list (`textfile_ext`/`mediafile_ext`/`miscfile_ext`, enforced in new installations), and the MIME type `finfo` detects must match it. Give `.txt` fixtures a few words of text, since `finfo` reads one-character files as `application/octet-stream`. Tests calling the driver directly aren't checked.

To exercise the code in a real site instead, install this extension in one of the Sites' dev environments. The one CLI entry point is:

```bash
vendor/bin/typo3 faldatabase:migrate-to-database-storage <storage-id-from> <storage-id-to> [-s <source-folder-id>] [-t <target-folder-id>]
```

## Supported versions

This is the 2.x branch (`master`): TYPO3 `^11.5 || ^12.4`, PHP `^7.4 || ^8.x`, in `composer.json` and mirrored in `ext_emconf.php`. Keep both in sync. 1.x (TYPO3 10.4/11.5) is maintained on the `1.x` branch. TYPO3 13 and 14 will need a separate 3.x branch: TYPO3 13 changed the FAL driver interface (native types, a `Capabilities` object instead of an int) in a way one driver class can't serve together with 12.

Where 11 and 12 differ, branch on `(new Typo3Version())->getMajorVersion()`. Use APIs available in both: `executeQuery()`/`executeStatement()` instead of `execute()`, `expr()->and()`/`or()` instead of `andX()`/`orX()`. The upgrade wizard still uses the DBAL 2/3 schema API (`getSchemaManager()`, `listTableDetails()`, `Comparator::diffTable()`, `getDatabasePlatform()->getName()`), deprecated in DBAL 3 but the only one DBAL 2.13 has. Keep the code PHP 7.4-compatible: no PHP 8 syntax or functions such as `str_starts_with()`, even though the Symfony polyfill TYPO3 pulls in would usually provide them. `Build/Scripts/runTests.sh -p 7.4 -s lint` checks the syntax. The test suite itself only runs on PHP 8.1+, as the locked dev dependencies need it.

## Architecture

**Storage model** (`ext_tables.sql`, `Classes/Driver/DatabaseDriver.php`): everything is stored in one table, `tx_jbaron_faldatabase_entry` (`entry_id`, `storage`, `data` longblob). There is no real hierarchy:
- An entry's identifier is its full path. Folders are entries whose identifier **ends in `/`** (`isFolderIdentifier()`/`isFileIdentifier()`), and the root folder is the entry `/`, created lazily by `getRootLevelFolder()`.
- Listing, recursive delete, move and copy all find a folder's entries with `getSubEntryIdentifiers()`: a `LIKE '<folder-id>%'` query with escaped wildcards (`getConditionForAllSubEntries()`) that only **narrows down**, followed by an exact, case-sensitive prefix check in PHP (`isWithinFolder()`). The SQL part is never exact: TYPO3 turns `like()` into `ILIKE` on PostgreSQL. Don't use its results unfiltered. Moving or renaming a folder rewrites the identifier of every entry under it.
- Folder listings filter, sort and paginate in PHP, in that order, sorting like `LocalDriver` (`strnatcasecmp` on the name).
- File metadata is computed on demand rather than stored. Size is `strlen(data)`. The MIME type comes from writing the blob to a temp file and probing it. There is no mtime or ctime.
- The primary key is `(storage, entry_id)`, and every query by identifier must also filter by `storage`. On MySQL/MariaDB, the table uses the `utf8mb4_bin` collation so identifiers are case-sensitive. That's a table option, because TYPO3's `TableBuilder` ignores column collations.
- Column types are passed **keyed by column name** (`COLUMNTYPES`). Doctrine applies positional types in parameter order, which for `UPDATE`/`DELETE` is not the column order.
- Schema changes need an upgrade path for existing tables. TYPO3's database analyzer can't change a primary key on PostgreSQL (it adds the new one without dropping the old one) and doesn't change table collations. `Classes/Upgrades/EntryTableUpgradeWizard.php` handles the current schema; it works on the old table and has no "database up to date" prerequisite for that reason.

**Existence cache:** the driver registers a TYPO3 cache, `tx_jbaron_faldatabase_existencecache`, in `ext_localconf.php`. It caches whether an identifier exists, keyed by `sha256(storageUid:identifier)`, so that `fileExists`/`folderExists` don't hit the database every time. Every driver method that creates, deletes, moves or renames an entry must update this cache through `setExistenceCacheEntry()`, **after** its writes are committed. While a caller's transaction is open, the driver neither reads nor writes the cache; `setExistenceCacheEntry()` removes the entry instead, since the caller may still roll back. Code writing entries behind the driver's back (like `MigrationService`) must flush the cache afterwards. The production sites configure this cache with `NullBackend` (in each site's `ServerspecificConfiguration.php.twig`), i.e. it's disabled there.

**Transactions:** every mutating method validates its input **before** opening a transaction and runs only its writes through `executeInTransaction()`, which commits, or rolls back and rethrows. Don't roll back on invalid input: inside a caller's transaction (e.g. `MigrationService`'s), a rollback marks the caller's whole transaction as rollback-only.

**Local processing:** `getFileForLocalProcessing()` writes the blob to a temp file. The file name keeps the original extension, because TYPO3 refuses to process files whose extension isn't on its allow-list. Temp files are tracked in `$tempfileNames` and deleted in `__destruct()`.

**Public URLs and delivery:** the driver claims `CAPABILITY_PUBLIC`. `getPublicUrl()` returns `index.php?eID=fal_database_download&id=<storageUid>:<identifier>`. That URL is answered by `Classes/Middleware/FileDeliveryMiddleware.php`, a frontend PSR-15 middleware (registered in `Configuration/RequestMiddlewares.php`) that sits before TYPO3's own eID handler and streams the file. It only delivers files of online, public storages that use this driver, and checks the storage **before** resolving the file, because resolving through `ResourceFactory` creates index entries. Everything else gets a 404. Note that the resolved middleware list (`frontend.middlewares`) is in reverse execution order.

**Migration** (`Classes/Command/` and `Classes/Service/MigrationService.php`): this moves a folder tree from another storage into a database storage. It runs inside a single database transaction. For each source file it inserts a `tx_jbaron_faldatabase_entry` row and rewrites the matching `sys_file` row in place (`storage`, `identifier`, `identifier_hash`, `folder_hash`), so `sys_file` UIDs and references survive the move. The source files and folders are deleted through the source driver only **after** the commit, and the existence cache is flushed. If the source is a `Local` storage, its base path must be absolute. Target folders are created through the regular FAL API, which runs on the same connection and is therefore part of the transaction. The source driver is created through `DriverRegistry`, like the core does; `ResourceFactory::getDriverObject()` no longer exists in TYPO3 11.

**Registration:** `ext_localconf.php` registers the driver under `DatabaseDriver::DRIVER_KEY` (`Jbaron.FalDatabase`). The FlexForm `Configuration/FlexForm/DriverFlexForm.xml` is intentionally empty because the driver has no configuration. `Configuration/Services.yaml` wires the console command, using TYPO3 11-style DI. The upgrade wizard (`falDatabaseEntryTable`) is registered in `ext_localconf.php` on TYPO3 11, and by its `#[UpgradeWizard]` attribute on TYPO3 12, which needs it to be a (private) service in `Services.yaml`. Its `getIdentifier()` is only needed by TYPO3 11.
