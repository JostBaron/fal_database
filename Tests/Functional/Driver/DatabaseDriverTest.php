<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Driver;

use Jbaron\FalDatabase\Driver\DatabaseDriver;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use TYPO3\CMS\Core\Resource\Exception\FileOperationErrorException;
use TYPO3\CMS\Core\Resource\Exception\FolderDoesNotExistException;
use TYPO3\CMS\Core\Resource\ResourceStorageInterface;

/**
 * Exercises the driver directly against the FAL DriverInterface contract.
 *
 * Fixtures are written straight into the entry table, so a test for one driver
 * method doesn't depend on other driver methods working.
 */
class DatabaseDriverTest extends AbstractFunctionalTestCase
{
    private DatabaseDriver $subject;

    protected function setUp(): void
    {
        parent::setUp();
        $this->subject = $this->createDriver();
        // ResourceStorage always asks for the root folder first, which creates it.
        $this->subject->getRootLevelFolder();
    }

    // ---------------------------------------------------------------------
    // Basics
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function rootLevelFolderIsCreatedOnFirstAccess(): void
    {
        self::assertSame('/', $this->subject->getRootLevelFolder());
        self::assertSame(['/'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function rootLevelFolderIsCreatedOnlyOnce(): void
    {
        $this->subject->getRootLevelFolder();
        $this->createDriver()->getRootLevelFolder();

        self::assertSame(['/'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function defaultFolderIsUserUpload(): void
    {
        self::assertSame('/user_upload/', $this->subject->getDefaultFolder());
    }

    /**
     * @test
     */
    public function driverIsBrowsablePublicAndWritable(): void
    {
        self::assertTrue($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_BROWSABLE));
        self::assertTrue($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_PUBLIC));
        self::assertTrue($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_WRITABLE));
    }

    /**
     * @test
     */
    public function mergeConfigurationCapabilitiesRemovesCapabilitiesTheStorageDoesNotAllow(): void
    {
        $this->subject->mergeConfigurationCapabilities(ResourceStorageInterface::CAPABILITY_BROWSABLE);

        self::assertTrue($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_BROWSABLE));
        self::assertFalse($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_PUBLIC));
        self::assertFalse($this->subject->hasCapability(ResourceStorageInterface::CAPABILITY_WRITABLE));
    }

    /**
     * @test
     */
    public function filesAreReadableAndWritable(): void
    {
        $this->insertEntries(['/a.txt' => 'a']);

        self::assertSame(['r' => true, 'w' => true], $this->subject->getPermissions('/a.txt'));
    }

    /**
     * @test
     */
    public function publicUrlPointsToTheDeliveryEndpointWithTheCombinedIdentifier(): void
    {
        $url = $this->subject->getPublicUrl('/some folder/a b.txt');

        self::assertStringEndsWith(
            'index.php?eID=fal_database_download&id=1%3A%2Fsome%20folder%2Fa%20b.txt',
            $url
        );
    }

    /**
     * @test
     */
    public function isWithinDetectsEntriesInsideAFolder(): void
    {
        self::assertTrue($this->subject->isWithin('/a/', '/a/b/c.txt'));
        self::assertTrue($this->subject->isWithin('/', '/a.txt'));
        self::assertFalse($this->subject->isWithin('/a/', '/b/c.txt'));
        self::assertFalse($this->subject->isWithin('/a/', '/ab/c.txt'));
    }

    // ---------------------------------------------------------------------
    // Existence checks
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function fileExistsIsTrueOnlyForExistingFiles(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'x']);

        self::assertTrue($this->subject->fileExists('/a/x.txt'));
        self::assertFalse($this->subject->fileExists('/a/y.txt'));
        self::assertFalse($this->subject->fileExists('/a/'), 'A folder is not a file.');
    }

    /**
     * @test
     */
    public function folderExistsIsTrueOnlyForExistingFolders(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'x']);

        self::assertTrue($this->subject->folderExists('/a/'));
        self::assertTrue($this->subject->folderExists('/a'), 'The trailing slash is optional.');
        self::assertFalse($this->subject->folderExists('/b/'));
        self::assertFalse($this->subject->folderExists('/a/x.txt'), 'A file is not a folder.');
    }

    /**
     * @test
     */
    public function fileExistsInFolderAndFolderExistsInFolderCheckDirectChildren(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'x', '/a/sub/' => null]);

        self::assertTrue($this->subject->fileExistsInFolder('x.txt', '/a/'));
        self::assertFalse($this->subject->fileExistsInFolder('y.txt', '/a/'));
        self::assertTrue($this->subject->folderExistsInFolder('sub', '/a/'));
        self::assertFalse($this->subject->folderExistsInFolder('other', '/a/'));
    }

    /**
     * @test
     */
    public function existenceIsNotStaleAcrossDriverInstancesAfterDeletion(): void
    {
        $this->insertEntries(['/a.txt' => 'a']);
        $otherDriver = $this->createDriver();
        self::assertTrue($otherDriver->fileExists('/a.txt'));

        $this->subject->deleteFile('/a.txt');

        self::assertFalse($otherDriver->fileExists('/a.txt'));
        self::assertFalse($this->createDriver()->fileExists('/a.txt'));
    }

    // ---------------------------------------------------------------------
    // Folders
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function createFolderCreatesFolderInRootByDefault(): void
    {
        $identifier = $this->subject->createFolder('a');

        self::assertSame('/a/', $identifier);
        self::assertSame(['/', '/a/'], $this->getEntryIdentifiers());
        self::assertTrue($this->subject->folderExists('/a/'));
    }

    /**
     * @test
     */
    public function createFolderCreatesFolderInGivenParent(): void
    {
        $this->insertEntries(['/a/' => null]);

        self::assertSame('/a/b/', $this->subject->createFolder('b', '/a/'));
        self::assertTrue($this->subject->folderExists('/a/b/'));
    }

    /**
     * @test
     */
    public function createFolderRecursivelyCreatesAllMissingParents(): void
    {
        $identifier = $this->subject->createFolder('a/b/c', '/', true);

        self::assertSame('/a/b/c/', $identifier);
        self::assertSame(['/', '/a/', '/a/b/', '/a/b/c/'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function createFolderThrowsForMissingParent(): void
    {
        $this->expectException(FolderDoesNotExistException::class);
        $this->subject->createFolder('b', '/missing/');
    }

    /**
     * @test
     */
    public function getFolderInfoReturnsIdentifierNameAndStorage(): void
    {
        $this->insertEntries(['/a/' => null, '/a/b/' => null]);

        self::assertSame(
            ['identifier' => '/a/b/', 'name' => 'b', 'storage' => self::DATABASE_STORAGE_UID],
            $this->subject->getFolderInfoByIdentifier('/a/b/')
        );
    }

    /**
     * @test
     */
    public function isFolderEmptyIsTrueForEmptyFolder(): void
    {
        $this->insertEntries(['/a/' => null]);

        self::assertTrue($this->subject->isFolderEmpty('/a/'));
    }

    /**
     * @test
     */
    public function isFolderEmptyIsFalseForFolderWithContents(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'x']);

        self::assertFalse($this->subject->isFolderEmpty('/a/'));
    }

    /**
     * @test
     */
    public function deleteFolderDeletesEmptyFolder(): void
    {
        $this->insertEntries(['/a/' => null]);

        self::assertTrue($this->subject->deleteFolder('/a/'));
        self::assertSame(['/'], $this->getEntryIdentifiers());
        self::assertFalse($this->subject->folderExists('/a/'));
    }

    /**
     * @test
     */
    public function deleteFolderKeepsNonEmptyFolderWhenNotRecursive(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'x']);

        self::assertFalse($this->subject->deleteFolder('/a/'));
        self::assertSame(['/', '/a/', '/a/x.txt'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function deleteFolderRecursivelyDeletesWholeSubtreeButNothingElse(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/sub/' => null,
            '/a/sub/y.txt' => 'y',
            '/ab/' => null,
            '/ab/z.txt' => 'z',
        ]);

        self::assertTrue($this->subject->deleteFolder('/a/', true));

        self::assertSame(['/', '/ab/', '/ab/z.txt'], $this->getEntryIdentifiers());
        self::assertFalse($this->subject->fileExists('/a/sub/y.txt'));
        self::assertTrue($this->subject->fileExists('/ab/z.txt'));
    }

    /**
     * @test
     */
    public function deleteFolderThrowsForMissingFolder(): void
    {
        $this->expectException(FolderDoesNotExistException::class);
        $this->subject->deleteFolder('/missing/');
    }

    /**
     * @test
     */
    public function renameFolderRenamesWholeSubtreeAndReturnsIdentifierMap(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/sub/' => null,
            '/a/sub/y.txt' => 'y',
        ]);

        $map = $this->subject->renameFolder('/a/', 'renamed');

        ksort($map);
        self::assertSame(
            [
                '/a/' => '/renamed/',
                '/a/sub/' => '/renamed/sub/',
                '/a/sub/y.txt' => '/renamed/sub/y.txt',
                '/a/x.txt' => '/renamed/x.txt',
            ],
            $map
        );
        self::assertSame(
            ['/', '/renamed/', '/renamed/sub/', '/renamed/sub/y.txt', '/renamed/x.txt'],
            $this->getEntryIdentifiers()
        );
        self::assertFalse($this->subject->folderExists('/a/'));
        self::assertTrue($this->subject->fileExists('/renamed/sub/y.txt'));
        self::assertSame('y', $this->subject->getFileContents('/renamed/sub/y.txt'));
    }

    /**
     * @test
     */
    public function moveFolderWithinStorageMovesSubtreeIntoTargetFolder(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/target/' => null,
        ]);

        $map = $this->subject->moveFolderWithinStorage('/a/', '/target/', 'moved');

        ksort($map);
        self::assertSame(['/a/' => '/target/moved/', '/a/x.txt' => '/target/moved/x.txt'], $map);
        self::assertSame(['/', '/target/', '/target/moved/', '/target/moved/x.txt'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function moveFolderWithinStorageThrowsForMissingTarget(): void
    {
        $this->insertEntries(['/a/' => null]);

        $this->expectException(FolderDoesNotExistException::class);
        $this->subject->moveFolderWithinStorage('/a/', '/missing/', 'moved');
    }

    /**
     * @test
     */
    public function copyFolderWithinStorageCopiesSubtreeAndKeepsSource(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/sub/' => null,
            '/a/sub/y.bin' => self::binaryContents(),
            '/target/' => null,
        ]);

        self::assertTrue($this->subject->copyFolderWithinStorage('/a/', '/target/', 'copy'));

        self::assertSame(
            [
                '/',
                '/a/',
                '/a/sub/',
                '/a/sub/y.bin',
                '/a/x.txt',
                '/target/',
                '/target/copy/',
                '/target/copy/sub/',
                '/target/copy/sub/y.bin',
                '/target/copy/x.txt',
            ],
            $this->getEntryIdentifiers()
        );
        self::assertSame(self::binaryContents(), $this->getEntryData('/target/copy/sub/y.bin'));
        self::assertTrue($this->subject->fileExists('/target/copy/sub/y.bin'));
        self::assertTrue($this->subject->folderExists('/target/copy/sub/'));
    }

    /**
     * @test
     */
    public function copyFolderWithinStorageCopiesEmptyFolder(): void
    {
        $this->insertEntries(['/a/' => null, '/target/' => null]);

        self::assertTrue($this->subject->copyFolderWithinStorage('/a/', '/target/', 'copy'));
        self::assertTrue($this->subject->folderExists('/target/copy/'));
    }

    /**
     * @test
     */
    public function copyFolderWithinStorageThrowsIfTargetNameIsTaken(): void
    {
        $this->insertEntries(['/a/' => null, '/target/' => null, '/target/copy/' => null]);

        $this->expectException(FileOperationErrorException::class);
        $this->subject->copyFolderWithinStorage('/a/', '/target/', 'copy');
    }

    // ---------------------------------------------------------------------
    // Folder listings
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function getFilesInFolderReturnsOnlyDirectChildFiles(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/y.txt' => 'y',
            '/a/sub/' => null,
            '/a/sub/z.txt' => 'z',
        ]);

        self::assertSameIdentifiers(['/a/x.txt', '/a/y.txt'], $this->subject->getFilesInFolder('/a/'));
    }

    /**
     * @test
     */
    public function getFilesInFolderRecursivelyReturnsAllFilesBelow(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/sub/' => null,
            '/a/sub/z.txt' => 'z',
        ]);

        self::assertSameIdentifiers(['/a/x.txt', '/a/sub/z.txt'], $this->subject->getFilesInFolder('/a/', 0, 0, true));
    }

    /**
     * @test
     */
    public function getFoldersInFolderReturnsOnlyDirectChildFolders(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/sub1/' => null,
            '/a/sub2/' => null,
            '/a/sub2/deeper/' => null,
        ]);

        self::assertSameIdentifiers(['/a/sub1/', '/a/sub2/'], $this->subject->getFoldersInFolder('/a/'));
        self::assertSameIdentifiers(
            ['/a/sub1/', '/a/sub2/', '/a/sub2/deeper/'],
            $this->subject->getFoldersInFolder('/a/', 0, 0, true)
        );
    }

    /**
     * "_" and "%" are LIKE wildcards. "user_upload" is TYPO3's default folder name.
     *
     * @test
     */
    public function folderListingsDoNotIncludeEntriesOfSimilarlyNamedFolders(): void
    {
        $this->insertEntries([
            '/user_upload/' => null,
            '/user_upload/mine.txt' => 'mine',
            '/userXupload/' => null,
            '/userXupload/other.txt' => 'other',
            '/userXupload/sub/' => null,
        ]);

        self::assertSameIdentifiers(['/user_upload/mine.txt'], $this->subject->getFilesInFolder('/user_upload/', 0, 0, true));
        self::assertSameIdentifiers([], $this->subject->getFoldersInFolder('/user_upload/', 0, 0, true));
        self::assertSame(1, $this->subject->countFilesInFolder('/user_upload/', true));
    }

    /**
     * The driver reports a case-sensitive file system.
     *
     * @test
     */
    public function entriesDifferingOnlyInCaseAreDistinct(): void
    {
        self::assertTrue($this->subject->isCaseSensitiveFileSystem());
        $this->subject->createFile('a.txt', '/');
        $this->subject->createFile('A.txt', '/');
        $this->subject->setFileContents('/a.txt', 'lower');
        $this->subject->setFileContents('/A.txt', 'upper');

        self::assertSame('lower', $this->subject->getFileContents('/a.txt'));
        self::assertSame('upper', $this->subject->getFileContents('/A.txt'));
        self::assertFalse($this->subject->fileExists('/b.txt'));
        self::assertFalse($this->subject->fileExists('/B.TXT'));
    }

    /**
     * @test
     */
    public function folderListingsAndDeletesAreCaseSensitive(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/lower.txt' => 'l',
            '/A/' => null,
            '/A/upper.txt' => 'u',
        ]);

        self::assertSameIdentifiers(['/a/lower.txt'], $this->subject->getFilesInFolder('/a/', 0, 0, true));

        $this->subject->deleteFolder('/a/', true);

        self::assertSame(['/', '/A/', '/A/upper.txt'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function deletingFolderDoesNotDeleteEntriesOfSimilarlyNamedFolders(): void
    {
        $this->insertEntries([
            '/user_upload/' => null,
            '/user_upload/mine.txt' => 'mine',
            '/userXupload/' => null,
            '/userXupload/other.txt' => 'other',
        ]);

        $this->subject->deleteFolder('/user_upload/', true);

        self::assertSame(['/', '/userXupload/', '/userXupload/other.txt'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function getFilesInFolderAppliesNameFilterCallbacks(): void
    {
        $this->insertEntries(['/a/' => null, '/a/keep.txt' => 'k', '/a/drop.log' => 'd']);
        $filter = static function (string $itemName): int {
            return '.log' === substr($itemName, -4) ? -1 : 1;
        };

        self::assertSameIdentifiers(['/a/keep.txt'], $this->subject->getFilesInFolder('/a/', 0, 0, false, [$filter]));
        self::assertSame(1, $this->subject->countFilesInFolder('/a/', false, [$filter]));
    }

    /**
     * @test
     */
    public function nameFilterCallbacksGetNameIdentifierAndParentIdentifier(): void
    {
        $this->insertEntries(['/root.txt' => 'r', '/a/' => null, '/a/x.txt' => 'x']);
        $calls = [];
        $filter = static function (string $name, string $identifier, string $parentIdentifier) use (&$calls): int {
            $calls[$identifier] = [$name, $parentIdentifier];
            return 1;
        };

        $this->subject->getFilesInFolder('/', 0, 0, true, [$filter]);

        ksort($calls);
        self::assertSame(['/a/x.txt' => ['x.txt', '/a/'], '/root.txt' => ['root.txt', '/']], $calls);
    }

    /**
     * @test
     */
    public function getFilesInFolderSortsAndPaginatesFiles(): void
    {
        $this->insertEntries([
            '/0sub/' => null,
            '/0sub/nested.txt' => 'n',
            '/c.txt' => 'c',
            '/a.txt' => 'a',
            '/b.txt' => 'b',
        ]);

        self::assertSame(['/a.txt', '/b.txt'], array_values($this->subject->getFilesInFolder('/', 0, 2, false, [], 'name')));
        self::assertSame(['/c.txt'], array_values($this->subject->getFilesInFolder('/', 2, 2, false, [], 'name')));
        self::assertSame(
            ['/c.txt', '/b.txt', '/a.txt'],
            array_values($this->subject->getFilesInFolder('/', 0, 0, false, [], 'name', true))
        );
    }

    /**
     * @test
     */
    public function countFilesAndFoldersInFolder(): void
    {
        $this->insertEntries([
            '/a/' => null,
            '/a/x.txt' => 'x',
            '/a/y.txt' => 'y',
            '/a/sub/' => null,
            '/a/sub/z.txt' => 'z',
        ]);

        self::assertSame(2, $this->subject->countFilesInFolder('/a/'));
        self::assertSame(3, $this->subject->countFilesInFolder('/a/', true));
        self::assertSame(1, $this->subject->countFoldersInFolder('/a/'));
    }

    /**
     * @test
     */
    public function getFilesInFolderThrowsForMissingFolder(): void
    {
        $this->expectException(FolderDoesNotExistException::class);
        $this->subject->getFilesInFolder('/missing/');
    }

    // ---------------------------------------------------------------------
    // Files
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function addFileStoresContentsAndRemovesOriginal(): void
    {
        $this->insertEntries(['/a/' => null]);
        $localFile = $this->createLocalFile(self::binaryContents(), 'bin');

        $identifier = $this->subject->addFile($localFile, '/a/', 'added.bin');

        self::assertSame('/a/added.bin', $identifier);
        self::assertSame(self::binaryContents(), $this->getEntryData('/a/added.bin'));
        self::assertTrue($this->subject->fileExists('/a/added.bin'));
        self::assertFileDoesNotExist($localFile);
    }

    /**
     * @test
     */
    public function addFileKeepsOriginalIfRequested(): void
    {
        $localFile = $this->createLocalFile('contents');

        $this->subject->addFile($localFile, '/', 'added.txt', false);

        self::assertFileExists($localFile);
        self::assertSame('contents', $this->getEntryData('/added.txt'));
    }

    /**
     * @test
     */
    public function addFileUsesLocalFileNameIfNoNameIsGiven(): void
    {
        $localFile = $this->createLocalFile('contents');

        self::assertSame('/' . basename($localFile), $this->subject->addFile($localFile, '/'));
    }

    /**
     * @test
     */
    public function addFileThrowsIfFileAlreadyExists(): void
    {
        $this->insertEntries(['/existing.txt' => 'old']);

        try {
            $this->subject->addFile($this->createLocalFile('new'), '/', 'existing.txt');
            self::fail('Expected an exception.');
        } catch (FileOperationErrorException $exception) {
            // expected
        }
        self::assertSame('old', $this->getEntryData('/existing.txt'));
    }

    /**
     * A failed operation must not leave a transaction open: everything the request
     * does afterwards would silently be part of it and never be committed.
     *
     * @test
     */
    public function addFileToMissingFolderThrowsAndLeavesNoTransactionOpen(): void
    {
        try {
            $this->subject->addFile($this->createLocalFile('x'), '/missing/', 'x.txt');
            self::fail('Expected an exception.');
        } catch (FolderDoesNotExistException $exception) {
            // expected
        }
        self::assertFalse($this->getEntryConnection()->isTransactionActive());
    }

    /**
     * @test
     */
    public function createFileCreatesEmptyFile(): void
    {
        $this->insertEntries(['/a/' => null]);

        self::assertSame('/a/new.txt', $this->subject->createFile('new.txt', '/a/'));
        self::assertSame('', $this->subject->getFileContents('/a/new.txt'));
        self::assertTrue($this->subject->fileExists('/a/new.txt'));
    }

    /**
     * @test
     */
    public function createFileInMissingFolderThrowsAndLeavesNoTransactionOpen(): void
    {
        try {
            $this->subject->createFile('new.txt', '/missing/');
            self::fail('Expected an exception.');
        } catch (FileOperationErrorException $exception) {
            // expected
        }
        self::assertFalse($this->getEntryConnection()->isTransactionActive());
    }

    /**
     * @test
     */
    public function getFileContentsReturnsBinaryContentsUnchanged(): void
    {
        $this->insertEntries(['/a.bin' => self::binaryContents()]);

        self::assertSame(self::binaryContents(), $this->subject->getFileContents('/a.bin'));
    }

    /**
     * @test
     */
    public function getFileContentsThrowsForMissingFile(): void
    {
        $this->expectException(FileOperationErrorException::class);
        $this->subject->getFileContents('/missing.txt');
    }

    /**
     * @test
     */
    public function dumpFileContentsOutputsContents(): void
    {
        $this->insertEntries(['/a.txt' => 'dumped']);

        $this->expectOutputString('dumped');
        $this->subject->dumpFileContents('/a.txt');
    }

    /**
     * @test
     */
    public function setFileContentsReplacesContentsAndReturnsNumberOfBytes(): void
    {
        $this->insertEntries(['/a.bin' => 'old']);

        self::assertSame(strlen(self::binaryContents()), $this->subject->setFileContents('/a.bin', self::binaryContents()));
        self::assertSame(self::binaryContents(), $this->getEntryData('/a.bin'));
    }

    /**
     * @test
     */
    public function setFileContentsWithUnchangedContentsReturnsNumberOfBytes(): void
    {
        $this->insertEntries(['/a.txt' => 'same']);

        self::assertSame(4, $this->subject->setFileContents('/a.txt', 'same'));
    }

    /**
     * @test
     */
    public function replaceFileReplacesContentsWithLocalFile(): void
    {
        $this->insertEntries(['/a.bin' => 'old']);

        self::assertTrue($this->subject->replaceFile('/a.bin', $this->createLocalFile(self::binaryContents(), 'bin')));
        self::assertSame(self::binaryContents(), $this->getEntryData('/a.bin'));
    }

    /**
     * @test
     */
    public function replaceFileWithUnreadableLocalFileThrowsAndKeepsContents(): void
    {
        $this->insertEntries(['/a.txt' => 'old']);

        try {
            $this->subject->replaceFile('/a.txt', '/does/not/exist.txt');
            self::fail('Expected an exception.');
        } catch (FileOperationErrorException $exception) {
            // expected
        }
        self::assertSame('old', $this->getEntryData('/a.txt'));
    }

    /**
     * @test
     */
    public function replaceFileThrowsForMissingFile(): void
    {
        $this->expectException(FileOperationErrorException::class);
        $this->subject->replaceFile('/missing.txt', $this->createLocalFile('new'));
    }

    /**
     * @test
     */
    public function deleteFileDeletesFile(): void
    {
        $this->insertEntries(['/a.txt' => 'a', '/b.txt' => 'b']);

        self::assertTrue($this->subject->deleteFile('/a.txt'));
        self::assertSame(['/', '/b.txt'], $this->getEntryIdentifiers());
        self::assertFalse($this->subject->fileExists('/a.txt'));
    }

    /**
     * @test
     */
    public function deleteFileThrowsForMissingFile(): void
    {
        $this->expectException(FileOperationErrorException::class);
        $this->subject->deleteFile('/missing.txt');
    }

    /**
     * @test
     */
    public function renameFileRenamesFileInPlace(): void
    {
        $this->insertEntries(['/a/' => null, '/a/old.txt' => 'contents']);

        self::assertSame('/a/new.txt', $this->subject->renameFile('/a/old.txt', 'new.txt'));
        self::assertSame(['/', '/a/', '/a/new.txt'], $this->getEntryIdentifiers());
        self::assertSame('contents', $this->subject->getFileContents('/a/new.txt'));
        self::assertFalse($this->subject->fileExists('/a/old.txt'));
    }

    /**
     * @test
     */
    public function renameFileThrowsIfTargetNameIsTaken(): void
    {
        $this->insertEntries(['/old.txt' => 'old', '/new.txt' => 'new']);

        $this->expectException(FileOperationErrorException::class);
        $this->subject->renameFile('/old.txt', 'new.txt');
    }

    /**
     * @test
     */
    public function moveFileWithinStorageMovesFileAndReturnsNewIdentifier(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'contents', '/b/' => null]);

        self::assertSame('/b/moved.txt', $this->subject->moveFileWithinStorage('/a/x.txt', '/b/', 'moved.txt'));
        self::assertSame(['/', '/a/', '/b/', '/b/moved.txt'], $this->getEntryIdentifiers());
        self::assertSame('contents', $this->subject->getFileContents('/b/moved.txt'));
    }

    /**
     * @test
     */
    public function moveFileWithinStorageThrowsForMissingTargetFolderAndKeepsFile(): void
    {
        $this->insertEntries(['/a.txt' => 'a']);

        try {
            $this->subject->moveFileWithinStorage('/a.txt', '/missing/', 'a.txt');
            self::fail('Expected an exception.');
        } catch (FolderDoesNotExistException $exception) {
            // expected
        }
        self::assertSame(['/', '/a.txt'], $this->getEntryIdentifiers());
    }

    /**
     * @test
     */
    public function copyFileWithinStorageCopiesContentsAndKeepsSource(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.bin' => self::binaryContents(), '/b/' => null]);

        self::assertSame('/b/copy.bin', $this->subject->copyFileWithinStorage('/a/x.bin', '/b/', 'copy.bin'));
        self::assertSame(self::binaryContents(), $this->getEntryData('/b/copy.bin'));
        self::assertSame(self::binaryContents(), $this->getEntryData('/a/x.bin'));
        self::assertTrue($this->subject->fileExists('/b/copy.bin'));
    }

    /**
     * @test
     */
    public function copyFileWithinStorageThrowsIfTargetNameIsTaken(): void
    {
        $this->insertEntries(['/a/' => null, '/a/x.txt' => 'source', '/b/' => null, '/b/x.txt' => 'existing']);

        try {
            $this->subject->copyFileWithinStorage('/a/x.txt', '/b/', 'x.txt');
            self::fail('Expected an exception.');
        } catch (FileOperationErrorException $exception) {
            // expected
        }
        self::assertSame('existing', $this->getEntryData('/b/x.txt'));
    }

    /**
     * @test
     */
    public function hashHashesFileContents(): void
    {
        $this->insertEntries(['/a.bin' => self::binaryContents()]);

        self::assertSame(sha1(self::binaryContents()), $this->subject->hash('/a.bin', 'sha1'));
        self::assertSame(md5(self::binaryContents()), $this->subject->hash('/a.bin', 'md5'));
    }

    /**
     * @test
     */
    public function hashThrowsForUnsupportedAlgorithm(): void
    {
        $this->insertEntries(['/a.txt' => 'a']);

        $this->expectException(FileOperationErrorException::class);
        $this->subject->hash('/a.txt', 'no-such-algorithm');
    }

    /**
     * @test
     */
    public function getFileForLocalProcessingWritesContentsToTemporaryFileWithSameExtension(): void
    {
        $this->insertEntries(['/a/image.png' => self::pngContents()]);

        $localPath = $this->subject->getFileForLocalProcessing('/a/image.png');

        self::assertSame('png', pathinfo($localPath, PATHINFO_EXTENSION));
        self::assertSame(self::pngContents(), file_get_contents($localPath));
    }

    /**
     * @test
     */
    public function getFileInfoByIdentifierReturnsAllInformation(): void
    {
        $this->insertEntries(['/a/' => null, '/a/image.png' => self::pngContents()]);

        $info = $this->subject->getFileInfoByIdentifier('/a/image.png');

        self::assertSame(strlen(self::pngContents()), $info['size']);
        self::assertSame('image/png', $info['mimetype']);
        self::assertSame('image.png', $info['name']);
        self::assertSame('png', $info['extension']);
        self::assertSame('/a/image.png', $info['identifier']);
        self::assertSame($this->subject->hashIdentifier('/a/image.png'), $info['identifier_hash']);
        // TYPO3 hashes folder identifiers without the trailing slash.
        self::assertSame($this->subject->hashIdentifier('/a/'), $info['folder_hash']);
        self::assertSame(self::DATABASE_STORAGE_UID, $info['storage']);
    }

    /**
     * @test
     */
    public function getFileInfoByIdentifierReturnsOnlyRequestedProperties(): void
    {
        $this->insertEntries(['/a.txt' => 'abc']);

        self::assertSame(
            ['size' => 3, 'name' => 'a.txt'],
            $this->subject->getFileInfoByIdentifier('/a.txt', ['size', 'name'])
        );
    }

    // ---------------------------------------------------------------------
    // Several database storages
    // ---------------------------------------------------------------------

    /**
     * @test
     */
    public function twoDatabaseStoragesCanBothHaveARootFolder(): void
    {
        $this->createDatabaseStorage(self::SECOND_DATABASE_STORAGE_UID);

        self::assertSame('/', $this->createDriver(self::SECOND_DATABASE_STORAGE_UID)->getRootLevelFolder());
        self::assertSame(['/'], $this->getEntryIdentifiers(self::SECOND_DATABASE_STORAGE_UID));
    }

    /**
     * @test
     */
    public function operationsOnOneDatabaseStorageDoNotTouchSameIdentifierInAnother(): void
    {
        $this->createDatabaseStorage(self::SECOND_DATABASE_STORAGE_UID);
        $other = $this->createDriver(self::SECOND_DATABASE_STORAGE_UID);
        $other->getRootLevelFolder();
        $this->insertEntries(['/a.txt' => 'first', '/folder/' => null, '/folder/x.txt' => 'x']);
        $this->insertEntries(
            ['/a.txt' => 'second', '/folder/' => null, '/folder/x.txt' => 'x2'],
            self::SECOND_DATABASE_STORAGE_UID
        );

        $this->subject->setFileContents('/a.txt', 'changed');
        $this->subject->renameFile('/a.txt', 'renamed.txt');
        $this->subject->renameFolder('/folder/', 'moved');

        self::assertSame(['/', '/a.txt', '/folder/', '/folder/x.txt'], $this->getEntryIdentifiers(self::SECOND_DATABASE_STORAGE_UID));
        self::assertSame('second', $other->getFileContents('/a.txt'));

        $this->subject->deleteFile('/renamed.txt');
        $this->subject->deleteFolder('/moved/', true);

        self::assertSame(['/', '/a.txt', '/folder/', '/folder/x.txt'], $this->getEntryIdentifiers(self::SECOND_DATABASE_STORAGE_UID));
        self::assertSame('x2', $other->getFileContents('/folder/x.txt'));
    }

    /**
     * @test
     */
    public function entriesOfOneDatabaseStorageAreInvisibleToAnother(): void
    {
        $this->createDatabaseStorage(self::SECOND_DATABASE_STORAGE_UID);
        $this->insertEntries(['/only-in-first.txt' => 'first']);

        $otherDriver = $this->createDriver(self::SECOND_DATABASE_STORAGE_UID);

        self::assertFalse($otherDriver->fileExists('/only-in-first.txt'));
    }
}
