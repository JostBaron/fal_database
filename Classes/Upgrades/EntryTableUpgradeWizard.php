<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Upgrades;

use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Table;
use TYPO3\CMS\Core\Database\Connection;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Install\Attribute\UpgradeWizard;
use TYPO3\CMS\Install\Updates\UpgradeWizardInterface;

/**
 * Migrates tx_jbaron_faldatabase_entry from the old schema, where entry_id alone was
 * the primary key, to (storage, entry_id), and makes identifiers case-sensitive on
 * MySQL/MariaDB.
 *
 * The database analyzer can't do this alone: on PostgreSQL it adds the new primary key
 * without dropping the old one, which fails, and it doesn't change table collations.
 * So this wizard has no "database up to date" prerequisite and works on the old table.
 *
 * Registered by the attribute on TYPO3 12, and in ext_localconf.php on TYPO3 11, which
 * ignores the attribute. getIdentifier() is only needed by TYPO3 11.
 */
#[UpgradeWizard('falDatabaseEntryTable')]
class EntryTableUpgradeWizard implements UpgradeWizardInterface
{
    private const TABLENAME = 'tx_jbaron_faldatabase_entry';
    private const PRIMARY_KEY = ['storage', 'entry_id'];
    private const OBSOLETE_INDEX = 'uniqueEntriesInFolder';
    private const MYSQL_CHARSET = 'utf8mb4';
    private const MYSQL_COLLATION = 'utf8mb4_bin';

    public function getIdentifier(): string
    {
        return 'falDatabaseEntryTable';
    }

    public function getTitle(): string
    {
        return 'FAL database driver: Make file identifiers unique per storage and case-sensitive';
    }

    public function getDescription(): string
    {
        return 'Changes the primary key of ' . self::TABLENAME . ' to (storage, entry_id), so several '
            . 'database storages can be used, and removes the obsolete index. On MySQL/MariaDB, it also '
            . 'converts the table to the binary collation, so file names differing only in case are distinct.';
    }

    public function updateNecessary(): bool
    {
        $table = $this->getTable();
        if (null === $table) {
            return false;
        }

        return $this->needsPrimaryKeyChange($table)
            || $table->hasIndex(self::OBSOLETE_INDEX)
            || $this->needsCollationChange();
    }

    public function executeUpdate(): bool
    {
        $table = $this->getTable();
        if (null === $table) {
            return true;
        }

        if ($this->needsPrimaryKeyChange($table) && $table->hasPrimaryKey() && $this->isPostgreSql()) {
            // DBAL 2 drops primary keys with 'DROP INDEX "primary"' on PostgreSQL, where the
            // primary key is a constraint with its own name.
            $this->dropPostgreSqlPrimaryKey();
            $table = $this->getTable();
        }

        $newTable = clone $table;
        if ($newTable->hasIndex(self::OBSOLETE_INDEX)) {
            $newTable->dropIndex(self::OBSOLETE_INDEX);
        }
        if ($this->needsPrimaryKeyChange($table)) {
            if ($newTable->hasPrimaryKey()) {
                $newTable->dropPrimaryKey();
            }
            $newTable->setPrimaryKey(self::PRIMARY_KEY);
        }

        $connection = $this->getConnection();
        $tableDiff = (new Comparator())->diffTable($table, $newTable);
        if (false !== $tableDiff) {
            foreach ($connection->getDatabasePlatform()->getAlterTableSQL($tableDiff) as $statement) {
                $connection->executeStatement($statement);
            }
        }

        if ($this->needsCollationChange()) {
            $connection->executeStatement(sprintf(
                'ALTER TABLE %s CONVERT TO CHARACTER SET %s COLLATE %s',
                $connection->quoteIdentifier(self::TABLENAME),
                self::MYSQL_CHARSET,
                self::MYSQL_COLLATION
            ));
        }

        return true;
    }

    /**
     * @return string[]
     */
    public function getPrerequisites(): array
    {
        return [];
    }

    private function needsPrimaryKeyChange(Table $table): bool
    {
        $primaryKey = $table->getPrimaryKey();
        $columns = null === $primaryKey ? [] : array_map('strtolower', $primaryKey->getColumns());
        return self::PRIMARY_KEY !== $columns;
    }

    private function needsCollationChange(): bool
    {
        if (!$this->isMySql()) {
            return false;
        }

        $collation = $this->getConnection()->executeQuery(
            'SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?',
            [self::TABLENAME]
        )->fetchOne();

        return false !== $collation && self::MYSQL_COLLATION !== $collation;
    }

    private function getTable(): ?Table
    {
        $schemaManager = $this->getConnection()->getSchemaManager();
        if (!$schemaManager->tablesExist([self::TABLENAME])) {
            return null;
        }
        return $schemaManager->listTableDetails(self::TABLENAME);
    }

    private function dropPostgreSqlPrimaryKey(): void
    {
        $connection = $this->getConnection();
        $constraintName = $connection->executeQuery(
            'SELECT conname FROM pg_constraint WHERE conrelid = CAST(? AS regclass) AND contype = \'p\'',
            [self::TABLENAME]
        )->fetchOne();
        if (false === $constraintName) {
            return;
        }

        $connection->executeStatement(sprintf(
            'ALTER TABLE %s DROP CONSTRAINT %s',
            $connection->quoteIdentifier(self::TABLENAME),
            $connection->quoteIdentifier($constraintName)
        ));
    }

    private function isPostgreSql(): bool
    {
        return 'postgresql' === $this->getConnection()->getDatabasePlatform()->getName();
    }

    private function isMySql(): bool
    {
        return 'mysql' === $this->getConnection()->getDatabasePlatform()->getName();
    }

    private function getConnection(): Connection
    {
        return GeneralUtility::makeInstance(ConnectionPool::class)->getConnectionForTable(self::TABLENAME);
    }
}
