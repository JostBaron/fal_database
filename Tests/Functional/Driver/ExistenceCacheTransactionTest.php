<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Driver;

use Jbaron\FalDatabase\Driver\DatabaseDriver;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use TYPO3\CMS\Core\Cache\Backend\TransientMemoryBackend;

/**
 * Driver calls inside a transaction of the caller (e.g. MigrationService) must not
 * leave the existence cache claiming something the database doesn't contain once
 * that transaction is rolled back.
 *
 * Uses a non-transactional cache backend, like Redis. With TYPO3's default database
 * backend, the cache writes are part of the same transaction and would hide the bug.
 */
class ExistenceCacheTransactionTest extends AbstractFunctionalTestCase
{
    /**
     * @var array<string, mixed>
     */
    protected array $configurationToUseInTestInstance = [
        'SYS' => [
            'caching' => [
                'cacheConfigurations' => [
                    DatabaseDriver::CACHE_EXISTENCE_NAME => [
                        'backend' => TransientMemoryBackend::class,
                    ],
                ],
            ],
        ],
    ];

    private const FIXTURE = [
        '/' => null,
        '/a.txt' => 'a',
        '/folder/' => null,
        '/folder/x.txt' => 'x',
    ];

    /**
     * @return array<string, array{0: callable(DatabaseDriver, self): void, 1: string[]}>
     */
    public function operationsDataProvider(): array
    {
        return [
            'createFolder' => [
                static function (DatabaseDriver $driver): void {
                    $driver->createFolder('new', '/');
                },
                ['/new/'],
            ],
            'createFolder recursively' => [
                static function (DatabaseDriver $driver): void {
                    $driver->createFolder('new/deeper', '/', true);
                },
                ['/new/', '/new/deeper/'],
            ],
            'createFile' => [
                static function (DatabaseDriver $driver): void {
                    $driver->createFile('new.txt', '/');
                },
                ['/new.txt'],
            ],
            'addFile' => [
                static function (DatabaseDriver $driver, self $test): void {
                    $driver->addFile($test->createLocalFile('new'), '/', 'new.txt');
                },
                ['/new.txt'],
            ],
            'copyFileWithinStorage' => [
                static function (DatabaseDriver $driver): void {
                    $driver->copyFileWithinStorage('/a.txt', '/folder/', 'copy.txt');
                },
                ['/folder/copy.txt'],
            ],
            'renameFile' => [
                static function (DatabaseDriver $driver): void {
                    $driver->renameFile('/a.txt', 'renamed.txt');
                },
                ['/a.txt', '/renamed.txt'],
            ],
            'moveFileWithinStorage' => [
                static function (DatabaseDriver $driver): void {
                    $driver->moveFileWithinStorage('/a.txt', '/folder/', 'moved.txt');
                },
                ['/a.txt', '/folder/moved.txt'],
            ],
            'deleteFile' => [
                static function (DatabaseDriver $driver): void {
                    $driver->deleteFile('/a.txt');
                },
                ['/a.txt'],
            ],
            'deleteFolder' => [
                static function (DatabaseDriver $driver): void {
                    $driver->deleteFolder('/folder/', true);
                },
                ['/folder/', '/folder/x.txt'],
            ],
            'renameFolder' => [
                static function (DatabaseDriver $driver): void {
                    $driver->renameFolder('/folder/', 'renamed');
                },
                ['/folder/', '/folder/x.txt', '/renamed/', '/renamed/x.txt'],
            ],
            'moveFolderWithinStorage' => [
                static function (DatabaseDriver $driver): void {
                    $driver->createFolder('target', '/');
                    $driver->moveFolderWithinStorage('/folder/', '/target/', 'moved');
                },
                ['/folder/', '/folder/x.txt', '/target/', '/target/moved/', '/target/moved/x.txt'],
            ],
            'copyFolderWithinStorage' => [
                static function (DatabaseDriver $driver): void {
                    $driver->copyFolderWithinStorage('/folder/', '/', 'copy');
                },
                ['/copy/', '/copy/x.txt'],
            ],
            'existence lookup of uncommitted entry' => [
                static function (DatabaseDriver $driver, self $test): void {
                    $test->insertEntries(['/uncommitted.txt' => 'u']);
                    $driver->fileExists('/uncommitted.txt');
                },
                ['/uncommitted.txt'],
            ],
            'existence lookup of entry deleted uncommitted' => [
                static function (DatabaseDriver $driver, self $test): void {
                    $test->getEntryConnection()->delete(self::ENTRY_TABLE, ['entry_id' => '/a.txt']);
                    $driver->fileExists('/a.txt');
                },
                ['/a.txt'],
            ],
        ];
    }

    /**
     * @test
     * @dataProvider operationsDataProvider
     *
     * @param callable(DatabaseDriver, self): void $operation
     * @param string[] $identifiersToCheck
     */
    public function rolledBackOperationLeavesNoStaleCacheEntries(callable $operation, array $identifiersToCheck): void
    {
        $this->insertEntries(self::FIXTURE);
        $driver = $this->createDriver();

        $this->getEntryConnection()->beginTransaction();
        $operation($driver, $this);
        $this->getEntryConnection()->rollBack();

        $this->assertCacheMatchesDatabase($identifiersToCheck);
    }

    /**
     * @test
     */
    public function rolledBackRootFolderCreationLeavesNoStaleCacheEntry(): void
    {
        $driver = $this->createDriver();

        $this->getEntryConnection()->beginTransaction();
        $driver->getRootLevelFolder();
        $this->getEntryConnection()->rollBack();

        $this->assertCacheMatchesDatabase(['/']);
        // Accessing the root again must recreate it.
        self::assertSame('/', $this->createDriver()->getRootLevelFolder());
        self::assertSame(['/'], $this->getEntryIdentifiers());
    }

    /**
     * Outside a caller's transaction, changes are cached as usual, so the tests above
     * aren't passing just because nothing is cached at all.
     *
     * @test
     */
    public function operationOutsideTransactionIsCached(): void
    {
        $this->insertEntries(self::FIXTURE);

        $this->createDriver()->createFile('new.txt', '/');

        // Remove the row behind the cache's back: only a cache hit still reports it.
        $this->getEntryConnection()->delete(self::ENTRY_TABLE, ['entry_id' => '/new.txt']);
        self::assertTrue($this->createDriver()->fileExists('/new.txt'));
    }

    /**
     * Made public for the data provider's closures.
     */
    public function createLocalFile(string $contents, string $extension = 'txt'): string
    {
        return parent::createLocalFile($contents, $extension);
    }

    /**
     * Made public for the data provider's closures.
     *
     * @param array<string, string|null> $entries
     */
    public function insertEntries(array $entries, int $storageUid = self::DATABASE_STORAGE_UID): void
    {
        parent::insertEntries($entries, $storageUid);
    }

    public function getEntryConnection(): \TYPO3\CMS\Core\Database\Connection
    {
        return parent::getEntryConnection();
    }

    /**
     * @param string[] $identifiers
     */
    private function assertCacheMatchesDatabase(array $identifiers): void
    {
        $existingEntries = $this->getEntryIdentifiers();
        $driver = $this->createDriver();
        foreach ($identifiers as $identifier) {
            $existsInDatabase = in_array($identifier, $existingEntries, true);
            $existsForDriver = '/' === substr($identifier, -1)
                ? $driver->folderExists($identifier)
                : $driver->fileExists($identifier);
            self::assertSame(
                $existsInDatabase,
                $existsForDriver,
                sprintf(
                    'Driver says "%s" %s, but it %s in the database.',
                    $identifier,
                    $existsForDriver ? 'exists' : 'does not exist',
                    $existsInDatabase ? 'exists' : 'does not exist'
                )
            );
        }
    }
}
