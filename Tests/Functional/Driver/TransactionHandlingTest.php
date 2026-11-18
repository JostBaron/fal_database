<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Driver;

use Jbaron\FalDatabase\Driver\DatabaseDriver;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;

/**
 * Driver operations that fail on invalid input (missing folder, name taken, ...)
 * must neither leave a transaction open nor spoil a transaction of the caller.
 */
class TransactionHandlingTest extends AbstractFunctionalTestCase
{
    private const FIXTURE = [
        '/' => null,
        '/a.txt' => 'a',
        '/b.txt' => 'b',
        '/folder/' => null,
        '/folder/x.txt' => 'x',
        '/other/' => null,
    ];

    /**
     * @return array<string, array{0: callable(DatabaseDriver, self): mixed}>
     */
    public function failingOperationsDataProvider(): array
    {
        return [
            'createFolder in missing parent' => [
                static function (DatabaseDriver $driver) {
                    return $driver->createFolder('new', '/missing/');
                },
            ],
            'addFile to missing folder' => [
                static function (DatabaseDriver $driver, self $test) {
                    return $driver->addFile($test->createLocalFile('new'), '/missing/', 'new.txt');
                },
            ],
            'addFile with taken name' => [
                static function (DatabaseDriver $driver, self $test) {
                    return $driver->addFile($test->createLocalFile('new'), '/', 'a.txt');
                },
            ],
            'addFile with unreadable local file' => [
                static function (DatabaseDriver $driver) {
                    return $driver->addFile('/does/not/exist.txt', '/', 'new.txt');
                },
            ],
            'createFile in missing folder' => [
                static function (DatabaseDriver $driver) {
                    return $driver->createFile('new.txt', '/missing/');
                },
            ],
            'copyFileWithinStorage with taken name' => [
                static function (DatabaseDriver $driver) {
                    return $driver->copyFileWithinStorage('/a.txt', '/', 'b.txt');
                },
            ],
            'copyFileWithinStorage to missing folder' => [
                static function (DatabaseDriver $driver) {
                    return $driver->copyFileWithinStorage('/a.txt', '/missing/', 'a.txt');
                },
            ],
            'renameFile with taken name' => [
                static function (DatabaseDriver $driver) {
                    return $driver->renameFile('/a.txt', 'b.txt');
                },
            ],
            'moveFileWithinStorage to missing folder' => [
                static function (DatabaseDriver $driver) {
                    return $driver->moveFileWithinStorage('/a.txt', '/missing/', 'a.txt');
                },
            ],
            'replaceFile of missing file' => [
                static function (DatabaseDriver $driver, self $test) {
                    return $driver->replaceFile('/missing.txt', $test->createLocalFile('new'));
                },
            ],
            'deleteFile of missing file' => [
                static function (DatabaseDriver $driver) {
                    return $driver->deleteFile('/missing.txt');
                },
            ],
            'deleteFolder of non-empty folder, not recursive' => [
                static function (DatabaseDriver $driver) {
                    return $driver->deleteFolder('/folder/');
                },
            ],
            'moveFolderWithinStorage to missing folder' => [
                static function (DatabaseDriver $driver) {
                    return $driver->moveFolderWithinStorage('/folder/', '/missing/', 'moved');
                },
            ],
            'copyFolderWithinStorage with taken name' => [
                static function (DatabaseDriver $driver) {
                    return $driver->copyFolderWithinStorage('/folder/', '/', 'other');
                },
            ],
        ];
    }

    /**
     * @test
     * @dataProvider failingOperationsDataProvider
     *
     * @param callable(DatabaseDriver, self): mixed $operation
     */
    public function failingOperationLeavesNoTransactionOpen(callable $operation): void
    {
        $this->insertEntries(self::FIXTURE);

        $this->runAndIgnoreFailure($operation);

        self::assertFalse($this->getEntryConnection()->isTransactionActive());
        self::assertSame(array_keys(self::FIXTURE), $this->getSortedFixtureIdentifiers());
    }

    /**
     * @test
     * @dataProvider failingOperationsDataProvider
     *
     * @param callable(DatabaseDriver, self): mixed $operation
     */
    public function failingOperationDoesNotSpoilTransactionOfCaller(callable $operation): void
    {
        $this->insertEntries(self::FIXTURE);
        $connection = $this->getEntryConnection();

        $connection->beginTransaction();
        $this->runAndIgnoreFailure($operation);
        // The caller handled the failure and continues with other work.
        $this->createDriver()->createFile('after.txt', '/');
        $connection->commit();

        self::assertFalse($connection->isTransactionActive());
        self::assertContains('/after.txt', $this->getEntryIdentifiers());
    }

    /**
     * Made public for the data provider's closures.
     */
    public function createLocalFile(string $contents, string $extension = 'txt'): string
    {
        return parent::createLocalFile($contents, $extension);
    }

    /**
     * @param callable(DatabaseDriver, self): mixed $operation
     */
    private function runAndIgnoreFailure(callable $operation): void
    {
        try {
            $result = $operation($this->createDriver(), $this);
        } catch (\Throwable $throwable) {
            self::assertStringStartsWith(
                'TYPO3\\CMS\\Core\\Resource\\Exception',
                get_class($throwable),
                'Expected a FAL exception, got: ' . get_class($throwable) . ': ' . $throwable->getMessage()
            );
            return;
        }
        self::assertFalse($result, 'Expected the operation to fail.');
    }

    /**
     * @return string[] The fixture's identifiers in fixture order, if they all still exist.
     */
    private function getSortedFixtureIdentifiers(): array
    {
        $existing = $this->getEntryIdentifiers();
        return array_values(array_filter(
            array_keys(self::FIXTURE),
            static function (string $identifier) use ($existing): bool {
                return in_array($identifier, $existing, true);
            }
        ));
    }
}
