<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional;

use Doctrine\DBAL\ParameterType;
use Jbaron\FalDatabase\Driver\DatabaseDriver;
use TYPO3\CMS\Core\Core\Environment;
use TYPO3\CMS\Core\Database\Connection;
use TYPO3\CMS\Core\Resource\ResourceStorage;
use TYPO3\CMS\Core\Resource\StorageRepository;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\TestingFramework\Core\Functional\FunctionalTestCase;

/**
 * Common setup for all functional tests: loads the extension and creates a
 * database-backed file storage with UID {@see self::DATABASE_STORAGE_UID}.
 */
abstract class AbstractFunctionalTestCase extends FunctionalTestCase
{
    protected const DATABASE_STORAGE_UID = 1;
    protected const LOCAL_STORAGE_UID = 2;
    protected const SECOND_DATABASE_STORAGE_UID = 3;

    protected const ENTRY_TABLE = 'tx_jbaron_faldatabase_entry';

    /**
     * A valid 1x1 pixel PNG image.
     */
    private const PNG_CONTENTS_BASE64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==';

    /**
     * @var string[]
     */
    protected $testExtensionsToLoad = [
        'typo3conf/ext/fal_database',
    ];

    protected function setUp(): void
    {
        parent::setUp();
        $this->createDatabaseStorage(self::DATABASE_STORAGE_UID);
    }

    /**
     * Rolls back transactions a failing driver call left open. Otherwise, on PostgreSQL,
     * the aborted transaction breaks the setUp() of every following test. Leaks are
     * asserted explicitly by dedicated tests.
     */
    protected function tearDown(): void
    {
        $connection = $this->getEntryConnection();
        while ($connection->isTransactionActive()) {
            $connection->rollBack();
        }
        parent::tearDown();
    }

    protected function createDatabaseStorage(int $uid): void
    {
        $this->insertStorageRecord($uid, 'Database storage ' . $uid, DatabaseDriver::DRIVER_KEY, '');
    }

    protected function createLocalStorage(int $uid, string $absoluteBasePath): void
    {
        $absoluteBasePath = rtrim($absoluteBasePath, '/') . '/';
        GeneralUtility::mkdir_deep($absoluteBasePath);

        $configuration = '<?xml version="1.0" encoding="utf-8" standalone="yes" ?>'
            . '<T3FlexForms><data><sheet index="sDEF"><language index="lDEF">'
            . '<field index="basePath"><value index="vDEF">' . htmlspecialchars($absoluteBasePath) . '</value></field>'
            . '<field index="pathType"><value index="vDEF">absolute</value></field>'
            . '<field index="caseSensitive"><value index="vDEF">1</value></field>'
            . '</language></sheet></data></T3FlexForms>';

        $this->insertStorageRecord($uid, 'Local storage ' . $uid, 'Local', $configuration);
    }

    protected function getStorage(int $uid): ResourceStorage
    {
        $storage = GeneralUtility::makeInstance(StorageRepository::class)->findByUid($uid);
        self::assertInstanceOf(ResourceStorage::class, $storage, sprintf('Storage %d could not be loaded.', $uid));
        return $storage;
    }

    /**
     * Creates a driver instance the same way the TYPO3 core does for a storage.
     */
    protected function createDriver(int $storageUid = self::DATABASE_STORAGE_UID): DatabaseDriver
    {
        $driver = new DatabaseDriver([]);
        $driver->setStorageUid($storageUid);
        $driver->processConfiguration();
        $driver->initialize();
        return $driver;
    }

    /**
     * Writes entries directly into the entry table, bypassing the driver, so tests
     * of one driver method don't depend on other driver methods working.
     *
     * @param array<string, string|null> $entries Identifier => contents. Use null for folders.
     */
    protected function insertEntries(array $entries, int $storageUid = self::DATABASE_STORAGE_UID): void
    {
        foreach ($entries as $identifier => $data) {
            $this->getEntryConnection()->insert(
                self::ENTRY_TABLE,
                [
                    'entry_id' => $identifier,
                    'storage' => $storageUid,
                    'data' => $data,
                ],
                [
                    'entry_id' => ParameterType::STRING,
                    'storage' => ParameterType::INTEGER,
                    'data' => null === $data ? ParameterType::NULL : ParameterType::LARGE_OBJECT,
                ]
            );
        }
    }

    /**
     * @return string[] All entry identifiers of the given storage, sorted byte-wise.
     */
    protected function getEntryIdentifiers(int $storageUid = self::DATABASE_STORAGE_UID): array
    {
        $rows = $this->getEntryConnection()
            ->select(['entry_id'], self::ENTRY_TABLE, ['storage' => $storageUid])
            ->fetchAllAssociative();
        $identifiers = array_column($rows, 'entry_id');
        sort($identifiers, SORT_STRING);
        return $identifiers;
    }

    /**
     * Reads the raw stored contents of an entry directly from the database.
     */
    protected function getEntryData(string $identifier, int $storageUid = self::DATABASE_STORAGE_UID): ?string
    {
        $row = $this->getEntryConnection()
            ->select(['data'], self::ENTRY_TABLE, ['entry_id' => $identifier, 'storage' => $storageUid])
            ->fetchAssociative();
        self::assertIsArray($row, sprintf('There is no entry "%s" in storage %d.', $identifier, $storageUid));

        $data = $row['data'];
        if (is_resource($data)) {
            $data = stream_get_contents($data);
        }
        return $data;
    }

    protected function getEntryConnection(): Connection
    {
        return $this->getConnectionPool()->getConnectionForTable(self::ENTRY_TABLE);
    }

    /**
     * Creates a file on the local file system, e.g. as input for addFile().
     */
    protected function createLocalFile(string $contents, string $extension = 'txt'): string
    {
        $directory = Environment::getVarPath() . '/tests-fal-database/';
        GeneralUtility::mkdir_deep($directory);
        $path = $directory . bin2hex(random_bytes(8)) . '.' . $extension;
        file_put_contents($path, $contents);
        return $path;
    }

    /**
     * Contents covering every byte value, including NUL and bytes that are invalid UTF-8.
     */
    protected static function binaryContents(): string
    {
        return str_repeat(implode('', array_map('chr', range(0, 255))), 4);
    }

    protected static function pngContents(): string
    {
        return (string)base64_decode(self::PNG_CONTENTS_BASE64, true);
    }

    /**
     * Compares lists of identifiers, ignoring keys and order.
     *
     * @param string[] $expected
     * @param string[] $actual
     */
    protected static function assertSameIdentifiers(array $expected, array $actual, string $message = ''): void
    {
        $expected = array_values($expected);
        $actual = array_values($actual);
        sort($expected, SORT_STRING);
        sort($actual, SORT_STRING);
        self::assertSame($expected, $actual, $message);
    }

    private function insertStorageRecord(int $uid, string $name, string $driver, string $configuration): void
    {
        $this->getConnectionPool()->getConnectionForTable('sys_file_storage')->insert(
            'sys_file_storage',
            [
                'uid' => $uid,
                'pid' => 0,
                'name' => $name,
                'driver' => $driver,
                'configuration' => $configuration,
                'is_browsable' => 1,
                'is_public' => 1,
                'is_writable' => 1,
                'is_online' => 1,
                'auto_extract_metadata' => 0,
            ]
        );
        // StorageRepository caches all storage rows on first use.
        GeneralUtility::makeInstance(StorageRepository::class)->flush();
    }
}
