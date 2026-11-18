<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Upgrades;

use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use Jbaron\FalDatabase\Upgrades\EntryTableUpgradeWizard;
use TYPO3\CMS\Core\Database\Schema\SchemaMigrator;
use TYPO3\CMS\Core\Database\Schema\SqlReader;
use TYPO3\CMS\Core\Utility\GeneralUtility;

/**
 * Installations created before the schema change have the old entry table: entry_id
 * alone as primary key, plus a unique index, and on MySQL/MariaDB a case-insensitive
 * collation.
 */
class EntryTableUpgradeWizardTest extends AbstractFunctionalTestCase
{
    /**
     * @test
     */
    public function upgradeIsRegistered(): void
    {
        self::assertSame(
            EntryTableUpgradeWizard::class,
            $GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['ext/install']['update']['falDatabaseEntryTable'] ?? null
        );
    }

    /**
     * @test
     */
    public function freshInstallationNeedsNoUpgrade(): void
    {
        self::assertFalse((new EntryTableUpgradeWizard())->updateNecessary());
    }

    /**
     * @test
     */
    public function upgradesOldTableKeepingItsRows(): void
    {
        $this->createOldTable();
        $this->insertEntries(['/' => null, '/a.txt' => self::binaryContents()]);
        $subject = new EntryTableUpgradeWizard();

        self::assertTrue($subject->updateNecessary());
        self::assertTrue($subject->executeUpdate());

        self::assertFalse($subject->updateNecessary());
        self::assertSame(['/', '/a.txt'], $this->getEntryIdentifiers());
        self::assertSame(self::binaryContents(), $this->getEntryData('/a.txt'));
    }

    /**
     * @test
     */
    public function upgradedTableAllowsSeveralStoragesAndIsCaseSensitive(): void
    {
        $this->createOldTable();
        (new EntryTableUpgradeWizard())->executeUpdate();

        $this->insertEntries(['/' => null, '/a.txt' => 'a', '/A.txt' => 'A']);
        $this->insertEntries(['/' => null, '/a.txt' => 'other storage'], self::SECOND_DATABASE_STORAGE_UID);

        self::assertSame(['/', '/A.txt', '/a.txt'], $this->getEntryIdentifiers());
        self::assertSame(['/', '/a.txt'], $this->getEntryIdentifiers(self::SECOND_DATABASE_STORAGE_UID));
    }

    /**
     * Running the database analyzer afterwards must not fail or keep suggesting changes.
     *
     * @test
     */
    public function databaseAnalyzerCompletesTheUpgradedTable(): void
    {
        $this->createOldTable();
        (new EntryTableUpgradeWizard())->executeUpdate();

        $errors = $this->applyDatabaseAnalyzerSuggestions();

        self::assertSame([], $errors);
        self::assertSame([], $this->getDatabaseAnalyzerSuggestions());
    }

    private function createOldTable(): void
    {
        $connection = $this->getEntryConnection();
        $connection->executeStatement('DROP TABLE ' . self::ENTRY_TABLE);
        if ('mysql' === $connection->getDatabasePlatform()->getName()) {
            $connection->executeStatement(
                'CREATE TABLE ' . self::ENTRY_TABLE . ' (entry_id VARCHAR(255) NOT NULL, storage int(11) NOT NULL,'
                . ' data longblob, PRIMARY KEY (entry_id), UNIQUE KEY uniqueEntriesInFolder(storage, entry_id(240)))'
                . ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            );
        } else {
            $connection->executeStatement(
                'CREATE TABLE ' . self::ENTRY_TABLE . ' (entry_id VARCHAR(255) NOT NULL, storage INT NOT NULL,'
                . ' data BYTEA, PRIMARY KEY (entry_id))'
            );
            $connection->executeStatement(
                'CREATE UNIQUE INDEX uniqueEntriesInFolder ON ' . self::ENTRY_TABLE . ' (storage, entry_id)'
            );
        }
    }

    /**
     * @return array<string, string> Suggested statements concerning the entry table, keyed by hash.
     */
    private function getDatabaseAnalyzerSuggestions(): array
    {
        $suggestions = [];
        foreach ($this->getSchemaMigrator()->getUpdateSuggestions($this->getCreateTableStatements()) as $groups) {
            foreach ($groups as $statements) {
                foreach ($statements as $hash => $statement) {
                    if (false !== stripos($statement, self::ENTRY_TABLE)) {
                        $suggestions[$hash] = $statement;
                    }
                }
            }
        }
        return $suggestions;
    }

    /**
     * @return array<string, string> Errors, keyed by statement hash.
     */
    private function applyDatabaseAnalyzerSuggestions(): array
    {
        return $this->getSchemaMigrator()->migrate(
            $this->getCreateTableStatements(),
            $this->getDatabaseAnalyzerSuggestions()
        );
    }

    /**
     * @return string[]
     */
    private function getCreateTableStatements(): array
    {
        $sqlReader = GeneralUtility::makeInstance(SqlReader::class);
        return $sqlReader->getCreateTableStatementArray($sqlReader->getTablesDefinitionString());
    }

    private function getSchemaMigrator(): SchemaMigrator
    {
        return GeneralUtility::makeInstance(SchemaMigrator::class);
    }
}
