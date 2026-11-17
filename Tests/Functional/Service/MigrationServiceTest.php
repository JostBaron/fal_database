<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Service;

use Jbaron\FalDatabase\Service\MigrationService;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use TYPO3\CMS\Core\Core\Environment;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class MigrationServiceTest extends AbstractFunctionalTestCase
{
    private string $sourcePath;

    protected function setUp(): void
    {
        parent::setUp();
        // Local storages with absolute paths must be inside the public path.
        $this->sourcePath = Environment::getPublicPath() . '/migration-source/';
        $this->createLocalStorage(self::LOCAL_STORAGE_UID, $this->sourcePath);
        // Initializes the target storage, as the backend would when it is first opened.
        $this->getStorage(self::DATABASE_STORAGE_UID)->getRootLevelFolder();
    }

    /**
     * @test
     */
    public function migratesWholeStorageAndKeepsSysFileUids(): void
    {
        $textUid = $this->createIndexedSourceFile('a.txt', 'text');
        $binaryUid = $this->createIndexedSourceFile('sub/b.bin', self::binaryContents());

        $errors = $this->getSubject()->migrateFolderToDatabaseStorage(
            self::LOCAL_STORAGE_UID,
            self::DATABASE_STORAGE_UID
        );

        self::assertSame([], $errors);
        self::assertSame(['/', '/a.txt', '/sub/', '/sub/b.bin'], $this->getEntryIdentifiers());
        self::assertSame('text', $this->getEntryData('/a.txt'));
        self::assertSame(self::binaryContents(), $this->getEntryData('/sub/b.bin'));

        $this->assertSysFileRow($textUid, '/a.txt', '/');
        $this->assertSysFileRow($binaryUid, '/sub/b.bin', '/sub/');

        self::assertFileDoesNotExist($this->sourcePath . 'a.txt');
        self::assertDirectoryDoesNotExist($this->sourcePath . 'sub');
    }

    /**
     * @test
     */
    public function migratedFilesCanBeReadThroughTheTargetStorage(): void
    {
        $uid = $this->createIndexedSourceFile('sub/b.bin', self::binaryContents());

        $this->getSubject()->migrateFolderToDatabaseStorage(self::LOCAL_STORAGE_UID, self::DATABASE_STORAGE_UID);

        $file = $this->getStorage(self::DATABASE_STORAGE_UID)->getFile('/sub/b.bin');
        self::assertSame($uid, $file->getUid());
        self::assertSame(self::binaryContents(), $file->getContents());
    }

    /**
     * @test
     */
    public function migratesOnlyTheGivenSourceFolderIntoTheGivenTargetFolder(): void
    {
        $this->createIndexedSourceFile('stays.txt', 'stays');
        $movedUid = $this->createIndexedSourceFile('moving/moved.txt', 'moved');
        $this->getStorage(self::DATABASE_STORAGE_UID)->getRootLevelFolder()->createFolder('target');

        $errors = $this->getSubject()->migrateFolderToDatabaseStorage(
            self::LOCAL_STORAGE_UID,
            self::DATABASE_STORAGE_UID,
            '/moving/',
            '/target/'
        );

        self::assertSame([], $errors);
        self::assertSame(['/', '/target/', '/target/moved.txt'], $this->getEntryIdentifiers());
        $this->assertSysFileRow($movedUid, '/target/moved.txt', '/target/');
        self::assertFileExists($this->sourcePath . 'stays.txt');
    }

    /**
     * @test
     */
    public function reportsMissingStorages(): void
    {
        self::assertSame(
            ['The source storage does not exist.', 'The target storage does not exist.'],
            $this->getSubject()->migrateFolderToDatabaseStorage(998, 999)
        );
    }

    /**
     * @test
     */
    public function rollsBackEverythingAndKeepsSourceFilesIfAFileCannotBeMigrated(): void
    {
        $goodUid = $this->createIndexedSourceFile('good.txt', 'good');
        $conflictingUid = $this->createIndexedSourceFile('taken.txt', 'source');
        // An entry with the same identifier already exists in the target.
        $this->insertEntries(['/taken.txt' => 'existing']);

        $errors = $this->getSubject()->migrateFolderToDatabaseStorage(
            self::LOCAL_STORAGE_UID,
            self::DATABASE_STORAGE_UID
        );

        self::assertNotSame([], $errors);
        self::assertSame(['/', '/taken.txt'], $this->getEntryIdentifiers());
        self::assertSame('existing', $this->getEntryData('/taken.txt'));
        foreach ([$goodUid, $conflictingUid] as $uid) {
            $row = $this->getSysFileRow($uid);
            self::assertSame(self::LOCAL_STORAGE_UID, (int)$row['storage']);
        }
        self::assertFileExists($this->sourcePath . 'good.txt');
        self::assertFileExists($this->sourcePath . 'taken.txt');
    }

    private function getSubject(): MigrationService
    {
        return GeneralUtility::makeInstance(MigrationService::class);
    }

    /**
     * Creates a file in the local source storage and indexes it, returning its sys_file UID.
     */
    private function createIndexedSourceFile(string $relativePath, string $contents): int
    {
        $absolutePath = $this->sourcePath . $relativePath;
        GeneralUtility::mkdir_deep(dirname($absolutePath));
        file_put_contents($absolutePath, $contents);

        return $this->getStorage(self::LOCAL_STORAGE_UID)->getFile('/' . $relativePath)->getUid();
    }

    private function assertSysFileRow(int $uid, string $identifier, string $folderIdentifier): void
    {
        $row = $this->getSysFileRow($uid);
        self::assertSame(self::DATABASE_STORAGE_UID, (int)$row['storage']);
        self::assertSame($identifier, $row['identifier']);
        // Must match the hashes TYPO3 computes itself, or index lookups by folder miss the file.
        $driver = $this->createDriver();
        self::assertSame($driver->hashIdentifier($identifier), $row['identifier_hash']);
        self::assertSame($driver->hashIdentifier($folderIdentifier), $row['folder_hash']);
    }

    private function getSysFileRow(int $uid): array
    {
        $row = $this->getConnectionPool()->getConnectionForTable('sys_file')
            ->select(['*'], 'sys_file', ['uid' => $uid])
            ->fetchAssociative();
        self::assertIsArray($row, sprintf('sys_file row %d does not exist.', $uid));
        return $row;
    }
}
