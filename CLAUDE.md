# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`jbaron/fal_database` is a TYPO3 extension that adds a FAL (File Abstraction Layer) storage driver keeping files and folders as rows in a database table instead of on disk. It is one of Jost's personal open-source extensions: namespace `Jbaron\FalDatabase\`, `origin` is GitHub (`JostBaron/fal_database`), not Gerrit. Push normally. There is no `refs/for/` magic ref and no `Change-Id` trailer. Commit subjects still use the `[TASK]` / `[BUGFIX]` / `[CHORE]` prefixes. Never add `Co-Authored-By:` lines (or any other AI-attribution trailer) to commit messages.

The Mein-Bauernhof sites consume it through Composer. The workspace-level CLAUDE.md (`../../CLAUDE.md`) covers how it fits in.

## Commands

There is no test suite, linter or build step. `vendor/` and `public/typo3/` come from a local `composer install`, which exists only so the IDE can resolve TYPO3 classes; both are gitignored, as is `composer.lock`. A quick sanity check is a PHP syntax lint:

```bash
find Classes Configuration -name '*.php' -exec php -l {} \;
```

To exercise the code you need a TYPO3 installation, such as one of the Sites' dev environments, with this extension installed. The one CLI entry point is:

```bash
vendor/bin/typo3 faldatabase:migrate-to-database-storage <storage-id-from> <storage-id-to> [-s <source-folder-id>] [-t <target-folder-id>]
```

## Supported versions

`composer.json` is authoritative: TYPO3 `^10.4 || ^11.5`, PHP `^7.4 || ^8.x`. `ext_emconf.php` is stale (it says TYPO3 8.7–10.4). Despite the PHP 7.4 claim, `DatabaseDriver::isWithin()` calls `str_starts_with()`, which only exists from PHP 8.0. Either keep the code 7.4-compatible or raise the constraint.

## Architecture

**Storage model** (`ext_tables.sql`, `Classes/Driver/DatabaseDriver.php`): everything is stored in one table, `tx_jbaron_faldatabase_entry` (`entry_id`, `storage`, `data` longblob). There is no real hierarchy:
- An entry's identifier is its full path. Folders are entries whose identifier **ends in `/`** (`isFolderIdentifier()`/`isFileIdentifier()`), and the root folder is the entry `/`, created lazily by `getRootLevelFolder()`.
- Listing, recursive delete, move and copy all use a `LIKE '<folder-id>%'` prefix query (`getConditionForAllSubEntries()`). The driver then filters the results in PHP to find direct children. Moving or renaming a folder rewrites the identifier of every entry under it.
- File metadata is computed on demand rather than stored. Size is `strlen(data)`. The MIME type comes from writing the blob to a temp file and probing it. There is no mtime or ctime.
- `entry_id` alone is the `PRIMARY KEY`, so identifiers are **not** scoped per storage even though every query filters on `storage`. Two database-backed storages therefore cannot both hold the same path, including the root `/`. Keep this in mind before adding a second storage or changing the schema.

**Existence cache:** the driver registers a TYPO3 cache, `tx_jbaron_faldatabase_existencecache`, in `ext_localconf.php`. It caches whether an identifier exists, keyed by `sha256(storageUid:identifier)`, so that `fileExists`/`folderExists` don't hit the database every time. Every driver method that creates, deletes, moves or renames an entry must update this cache through `setExistenceCacheEntry()`. If a mutation forgets to, the cache goes stale.

**Local processing:** `getFileForLocalProcessing()` writes the blob to a temp file. The file name keeps the original extension, because TYPO3 refuses to process files whose extension isn't on its allow-list. Temp files are tracked in `$tempfileNames` and deleted in `__destruct()`.

**Public URLs and delivery:** the driver claims `CAPABILITY_PUBLIC`. `getPublicUrl()` returns `index.php?eID=fal_database_download&id=<storageUid>:<identifier>`. That URL is answered by `Classes/Middleware/FileDeliveryMiddleware.php`, a frontend PSR-15 middleware (registered in `Configuration/RequestMiddlewares.php`) that sits before TYPO3's own eID handler and streams the file. The middleware resolves any combined identifier through `ResourceFactory` and does no access check of its own.

**Migration** (`Classes/Command/` and `Classes/Service/MigrationService.php`): this moves a folder tree from another storage into a database storage. It runs inside a single database transaction. For each source file it inserts a `tx_jbaron_faldatabase_entry` row and rewrites the matching `sys_file` row in place (`storage`, `identifier`, `identifier_hash`, `folder_hash`), so `sys_file` UIDs and references survive the move. The source files and folders are deleted through the source driver only **after** the commit. If the source is a `Local` storage, its base path must be absolute. Target folders are created through the regular FAL API, not inside the transaction.

**Registration:** `ext_localconf.php` registers the driver under `DatabaseDriver::DRIVER_KEY` (`Jbaron.FalDatabase`). The FlexForm `Configuration/FlexForm/DriverFlexForm.xml` is intentionally empty because the driver has no configuration. `Configuration/Services.yaml` wires the console command, using TYPO3 11-style DI.
