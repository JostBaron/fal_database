<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Resource;

use Jbaron\FalDatabase\Driver\DatabaseDriver;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use TYPO3\CMS\Core\Resource\File;
use TYPO3\CMS\Core\Resource\ResourceStorage;

/**
 * Uses the driver the way TYPO3 does: through ResourceStorage, including the
 * sys_file index.
 */
class DatabaseStorageTest extends AbstractFunctionalTestCase
{
    private ResourceStorage $storage;

    protected function setUp(): void
    {
        parent::setUp();
        $this->storage = $this->getStorage(self::DATABASE_STORAGE_UID);
    }

    /**
     * @test
     */
    public function storageUsesTheDatabaseDriver(): void
    {
        self::assertSame(DatabaseDriver::DRIVER_KEY, $this->storage->getDriverType());
        self::assertTrue($this->storage->isOnline());
        self::assertTrue($this->storage->isWritable());
        self::assertTrue($this->storage->isPublic());
    }

    /**
     * @test
     */
    public function addedFileIsIndexedWithCorrectMetadata(): void
    {
        $file = $this->storage->addFile(
            $this->createLocalFile(self::pngContents(), 'png'),
            $this->storage->getRootLevelFolder(),
            'image.png'
        );

        self::assertInstanceOf(File::class, $file);
        $row = $this->getConnectionPool()->getConnectionForTable('sys_file')
            ->select(['*'], 'sys_file', ['uid' => $file->getUid()])
            ->fetchAssociative();
        self::assertIsArray($row);
        self::assertSame(self::DATABASE_STORAGE_UID, (int)$row['storage']);
        self::assertSame('/image.png', $row['identifier']);
        self::assertSame($this->storage->hashFileIdentifier('/image.png'), $row['identifier_hash']);
        self::assertSame($this->storage->hashFileIdentifier('/'), $row['folder_hash']);
        self::assertSame(strlen(self::pngContents()), (int)$row['size']);
        self::assertSame('image/png', $row['mime_type']);
        self::assertSame(sha1(self::pngContents()), $row['sha1']);
        self::assertSame(0, (int)$row['missing']);
    }

    /**
     * @test
     */
    public function contentsAndPublicUrlOfAddedFileCanBeRetrieved(): void
    {
        $file = $this->storage->addFile(
            $this->createLocalFile(self::binaryContents(), 'bin'),
            $this->storage->getRootLevelFolder(),
            'data.bin'
        );

        $reloaded = $this->storage->getFile('/data.bin');

        self::assertInstanceOf(File::class, $reloaded);
        self::assertSame($file->getUid(), $reloaded->getUid());
        self::assertSame(self::binaryContents(), $reloaded->getContents());
        self::assertStringEndsWith(
            'index.php?eID=fal_database_download&id=1%3A%2Fdata.bin',
            (string)$reloaded->getPublicUrl()
        );
    }

    /**
     * @test
     */
    public function foldersAndFilesCanBeListed(): void
    {
        $root = $this->storage->getRootLevelFolder();
        $folder = $root->createFolder('folder');
        $this->storage->addFile($this->createLocalFile('a'), $folder, 'a.txt');
        $this->storage->addFile($this->createLocalFile('b'), $folder, 'b.txt');
        $folder->createFolder('sub');

        self::assertSame(['a.txt', 'b.txt'], self::getNames($folder->getFiles()));
        self::assertSame(['sub'], self::getNames($folder->getSubfolders()));
        // The root also contains the processing folder TYPO3 creates on its own.
        self::assertContains('folder', self::getNames($root->getSubfolders()));
    }

    /**
     * @test
     */
    public function renamingFileUpdatesTheIndex(): void
    {
        $file = $this->storage->addFile($this->createLocalFile('x'), $this->storage->getRootLevelFolder(), 'old.txt');

        $file->rename('new.txt');

        $row = $this->getConnectionPool()->getConnectionForTable('sys_file')
            ->select(['identifier'], 'sys_file', ['uid' => $file->getUid()])
            ->fetchAssociative();
        self::assertSame('/new.txt', $row['identifier'] ?? null);
        self::assertSame('x', $this->storage->getFile('/new.txt')->getContents());
    }

    /**
     * @test
     */
    public function movingFileIntoFolderUpdatesTheIndex(): void
    {
        $root = $this->storage->getRootLevelFolder();
        $target = $root->createFolder('target');
        $file = $this->storage->addFile($this->createLocalFile('x'), $root, 'x.txt');

        $file->moveTo($target);

        $row = $this->getConnectionPool()->getConnectionForTable('sys_file')
            ->select(['identifier'], 'sys_file', ['uid' => $file->getUid()])
            ->fetchAssociative();
        self::assertSame('/target/x.txt', $row['identifier'] ?? null);
        self::assertContains('/target/x.txt', $this->getEntryIdentifiers());
        self::assertNotContains('/x.txt', $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function deletingFileRemovesItsEntry(): void
    {
        $file = $this->storage->addFile($this->createLocalFile('x'), $this->storage->getRootLevelFolder(), 'x.txt');

        self::assertTrue($file->delete());

        self::assertNotContains('/x.txt', $this->getEntryIdentifiers());
        self::assertFalse($this->storage->hasFile('/x.txt'));
    }

    /**
     * @test
     */
    public function deletingFolderRecursivelyRemovesAllEntries(): void
    {
        $folder = $this->storage->getRootLevelFolder()->createFolder('folder');
        $this->storage->addFile($this->createLocalFile('x'), $folder, 'x.txt');

        self::assertTrue($folder->delete(true));

        self::assertSame([], array_values(array_filter(
            $this->getEntryIdentifiers(),
            static function (string $identifier): bool {
                return 0 === strpos($identifier, '/folder/');
            }
        )));
    }

    /**
     * @param array<File|\TYPO3\CMS\Core\Resource\Folder> $items
     * @return string[] Sorted names.
     */
    private static function getNames(array $items): array
    {
        $names = array_map(
            static function ($item): string {
                return $item->getName();
            },
            array_values($items)
        );
        sort($names);
        return $names;
    }
}
