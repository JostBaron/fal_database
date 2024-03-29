<?php
declare(strict_types=1);

namespace Jbaron\FalDatabase\Driver;

use Doctrine\DBAL\ConnectionException;
use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Driver\ResultStatement;
use Doctrine\DBAL\Query\Expression\CompositeExpression;
use Doctrine\DBAL\Types\Types;
use InvalidArgumentException;
use Psr\Log\LoggerInterface;
use TYPO3\CMS\Core\Cache\CacheManager;
use TYPO3\CMS\Core\Cache\Exception\NoSuchCacheException;
use TYPO3\CMS\Core\Cache\Frontend\FrontendInterface;
use TYPO3\CMS\Core\Database\Connection;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Database\Query\QueryBuilder;
use TYPO3\CMS\Core\Log\LogManager;
use TYPO3\CMS\Core\Resource\Driver\AbstractHierarchicalFilesystemDriver;
use TYPO3\CMS\Core\Resource\Exception\FileOperationErrorException;
use TYPO3\CMS\Core\Resource\Exception\FolderDoesNotExistException;
use TYPO3\CMS\Core\Resource\Exception\ResourceDoesNotExistException;
use TYPO3\CMS\Core\Resource\ResourceStorageInterface;
use TYPO3\CMS\Core\Type\File\FileInfo;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Core\Utility\PathUtility;
use function array_filter;
use function array_merge;
use function basename;
use function count;
use function dirname;
use function explode;
use function file_exists;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function hash;
use function hash_algos;
use function in_array;
use function is_readable;
use function is_resource;
use function is_writable;
use function ltrim;
use function pathinfo;
use function rawurlencode;
use function rename;
use function rtrim;
use function sprintf;
use function stream_get_contents;
use function strlen;
use function strpos;
use function substr;
use function sys_get_temp_dir;
use function tempnam;
use function trim;
use function unlink;
use const PATHINFO_EXTENSION;

class DatabaseDriver extends AbstractHierarchicalFilesystemDriver
{
    public const DRIVER_KEY = 'Jbaron.FalDatabase';

    public const CACHE_EXISTENCE_NAME = 'tx_jbaron_faldatabase_existencecache';

    private const ROOT_FOLDER_ID = '/';
    private const DEFAULT_FOLDER_ID = '/user_upload/';

    private const TABLENAME = 'tx_jbaron_faldatabase_entry';

    private const COLUMNNAME_ENTRY_ID = 'entry_id';
    private const COLUMNNAME_STORAGE = 'storage';
    private const COLUMNNAME_DATA = 'data';

    private const COLUMNNAMES = [
        self::COLUMNNAME_ENTRY_ID,
        self::COLUMNNAME_STORAGE,
        self::COLUMNNAME_DATA,
    ];

    private const COLUMNTYPE_ENTRY_ID = Types::STRING;
    private const COLUMNTYPE_STORAGE = Types::INTEGER;
    private const COLUMNTYPE_DATA = Types::BLOB;

    private const COLUMNTYPES = [
        self::COLUMNTYPE_ENTRY_ID,
        self::COLUMNTYPE_STORAGE,
        self::COLUMNTYPE_DATA,
    ];

    /**
     * @var string[]
     */
    private array $tempfileNames = [];

    /**
     * @var Connection|null
     */
    protected ?Connection $databaseConnection = null;

    /**
     * @var LoggerInterface
     */
    protected LoggerInterface $logger;

    /**
     * Cache in which information about file existence is stored.
     *
     * @var FrontendInterface|null
     */
    protected ?FrontendInterface $entryExistenceCache = null;

    /**
     * @param array $configuration
     */
    public function __construct(array $configuration = [])
    {
        parent::__construct($configuration);

        $this->capabilities = ResourceStorageInterface::CAPABILITY_BROWSABLE
            | ResourceStorageInterface::CAPABILITY_PUBLIC
            | ResourceStorageInterface::CAPABILITY_WRITABLE;
    }

    public function __destruct()
    {
        foreach ($this->tempfileNames as $tempfileName) {
            @unlink($tempfileName);
        }
    }

    /**
     * @return FrontendInterface
     * @throws NoSuchCacheException
     */
    private function getExistenceCache(): FrontendInterface
    {
        if (null === $this->entryExistenceCache) {
            $this->entryExistenceCache = GeneralUtility::makeInstance(CacheManager::class)
                ->getCache(self::CACHE_EXISTENCE_NAME);
        }
        return $this->entryExistenceCache;
    }

    public function isCaseSensitiveFileSystem(): bool
    {
        return true;
    }

    public function processConfiguration()
    {
        // Nothing to do.
    }

    public function initialize()
    {
        $this->logger = GeneralUtility::makeInstance(LogManager::class)->getLogger(__CLASS__);
    }

    public function mergeConfigurationCapabilities($capabilities): int
    {
        $this->capabilities &= $capabilities;
        return $this->capabilities;
    }

    /**
     * @return string
     * @throws NoSuchCacheException
     */
    public function getRootLevelFolder(): string
    {
        if (!$this->folderExists(self::ROOT_FOLDER_ID)) {
            $this->createRootFolder();
        }
        return self::ROOT_FOLDER_ID;
    }

    public function getDefaultFolder(): string
    {
        return self::DEFAULT_FOLDER_ID;
    }

    public function getPublicUrl($identifier): string
    {
        return sprintf(
            '%1$sindex.php?eID=fal_database_download&id=%2$s%%3A%3$s',
            GeneralUtility::getIndpEnv('TYPO3_SITE_URL'),
            rawurlencode((string)$this->storageUid),
            rawurlencode($identifier)
        );
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function createFolder($newFolderName, $parentFolderIdentifier = '', $recursive = false): string
    {
        if ('' === $parentFolderIdentifier) {
            $parentFolderIdentifier = $this->getRootLevelFolder();
        }

        $this->logger->debug(
            'Creating folder',
            [
                'folderName'    => $newFolderName,
                'parentFolderIdentifier' => $parentFolderIdentifier,
                'recursive' => $recursive,
            ]
        );

        if (!$this->folderExists($parentFolderIdentifier)) {
            throw new FolderDoesNotExistException(
                sprintf('Parent folder with ID "%s" does not exist', $parentFolderIdentifier),
                1578678889
            );
        }

        $pathPartsOfFolderToCreate = explode('/', $parentFolderIdentifier);
        if (!$recursive) {
            $pathPartsOfFolderToCreate[] = $newFolderName;
        } else {
            $pathPartsOfNewFoldersName = explode('/', $newFolderName);
            $pathPartsOfFolderToCreate = array_merge($pathPartsOfFolderToCreate, $pathPartsOfNewFoldersName);
        }

        $pathPartsOfFolderToCreate = array_filter(
            $pathPartsOfFolderToCreate,
            function (string $pathPart): bool
            {
                return '' !== $pathPart;
            }
        );

        $this->logger->debug('Parts of path to create.', ['pathParts' => $pathPartsOfFolderToCreate]);

        $this->getDatabaseConnection()->beginTransaction();

        $insertData = [];
        if (!$this->folderExists($this->getRootLevelFolder())) {
            $this->logger->debug('Root folder does not exist - creating');
            $insertData[] = [
                self::COLUMNNAME_ENTRY_ID   => $this->getRootLevelFolder(),
                self::COLUMNNAME_STORAGE    => $this->storageUid,
                self::COLUMNNAME_DATA       => null,
            ];
        }

        $currentFolderPath = $this->getRootLevelFolder();
        foreach ($pathPartsOfFolderToCreate as $pathPart) {
            $currentFolderPath .= $pathPart . '/';
            $this->logger->debug('Checking path.', ['path' => $currentFolderPath]);
            if (!$this->folderExists($currentFolderPath)) {
                $this->logger->debug('Folder does not exist - adding to insert.', ['path' => $currentFolderPath]);
                $insertData[] = [
                    self::COLUMNNAME_ENTRY_ID   => $currentFolderPath,
                    self::COLUMNNAME_STORAGE    => $this->storageUid,
                    self::COLUMNNAME_DATA       => null,
                ];
            }
        }

        if (count($insertData) > 0) {
            $numberInserted = $this->getDatabaseConnection()->bulkInsert(
                self::TABLENAME,
                $insertData,
                self::COLUMNNAMES,
                static::getColumnTypes()
            );

            if (count($insertData) !== $numberInserted) {
                $this->getDatabaseConnection()->rollBack();
                throw new FileOperationErrorException(
                    'Could not insert at least one folder entry.',
                    1578678583
                );
            }
        }
        $this->getDatabaseConnection()->commit();
        foreach ($insertData as $insertDatum) {
            $this->setExistenceCacheEntry($insertDatum[self::COLUMNNAME_ENTRY_ID], true);
        }

        return $currentFolderPath;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ConnectionException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function renameFolder($folderIdentifier, $newName): array
    {
        $targetFolderIdentifier = rtrim(dirname($folderIdentifier), '/') . '/';
        return $this->moveFolderWithinStorage($folderIdentifier, $targetFolderIdentifier, $newName);
    }

    /**
     * {@inheritDoc}
     *
     * @throws FolderDoesNotExistException
     * @throws ConnectionException|Exception
     * @throws NoSuchCacheException
     */
    public function deleteFolder($folderIdentifier, $deleteRecursively = false): bool
    {
        if (!$this->folderExists($folderIdentifier)) {
            throw new FolderDoesNotExistException(
                sprintf('Folder with identifier "%s" does not exist.', $folderIdentifier),
                1578231639
            );
        }

        $this->logger->debug('Deleting folder', ['folder' => $folderIdentifier]);

        $this->getDatabaseConnection()->beginTransaction();

        if (!$deleteRecursively && !$this->isFolderEmpty($folderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            return false;
        }

        $queryBuilder = $this->getDatabaseConnection()->createQueryBuilder();
        $executedStatement = $queryBuilder
            ->select(self::COLUMNTYPE_ENTRY_ID)
            ->from(self::TABLENAME)
            ->where($this->getConditionForAllSubEntries($queryBuilder, $folderIdentifier, true))
            ->execute();

        $entryIdentifiersToDelete = [];
        while (null !== ($entryIdentifier = $executedStatement->fetchOne())) {
            $entryIdentifiersToDelete[] = $entryIdentifier;
            $this->getDatabaseConnection()->delete(
                self::TABLENAME,
                [
                    self::COLUMNTYPE_ENTRY_ID => $entryIdentifier,
                ],
                static::getColumnTypes()
            );
        }
        $this->getDatabaseConnection()->commit();

        foreach ($entryIdentifiersToDelete as $entryIdentifierToDelete) {
            $this->setExistenceCacheEntry($entryIdentifierToDelete, false);
        }

        return true;
    }

    /**
     * @param $fileIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    public function fileExists($fileIdentifier): bool
    {
        if (!$this->isFileIdentifier($fileIdentifier)) {
            return false;
        }

        return $this->doesItemWithIdentifierExist($fileIdentifier);
    }

    /**
     * @param $folderIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    public function folderExists($folderIdentifier): bool
    {
        if (!$this->isFolderIdentifier($folderIdentifier)) {
            $folderIdentifier .= '/';
        }
        return $this->doesItemWithIdentifierExist($folderIdentifier);
    }

    public function isFolderEmpty($folderIdentifier): bool
    {
        $queryBuilder = $this->getDatabaseConnection()->createQueryBuilder();
        $numberFolderEntriesExcludingSelf = $queryBuilder
            ->count('*')
            ->from(self::TABLENAME)
            ->where($this->getConditionForAllSubEntries($queryBuilder, $folderIdentifier))
            ->execute()
        ;

        return 0 === $numberFolderEntriesExcludingSelf;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function addFile($localFilePath, $targetFolderIdentifier, $newFileName = '', $removeOriginal = true): string
    {
        if ('' === $newFileName) {
            $newFileName = basename($localFilePath);
        }

        $this->getDatabaseConnection()->beginTransaction();

        if (!$this->folderExists($targetFolderIdentifier)) {
            throw new FolderDoesNotExistException(
                sprintf('Folder with ID "%s" to add file to does not exist.', $targetFolderIdentifier),
                1578465395
            );
        }

        if ($this->fileExistsInFolder($newFileName, $targetFolderIdentifier)) {
            throw new FileOperationErrorException(
                sprintf(
                    'There already is a file named "%s" in folder with ID "%s".',
                    $targetFolderIdentifier,
                    $newFileName
                ),
                1578465439
            );
        }

        if (!is_readable($localFilePath)) {
            throw new FileOperationErrorException(
                sprintf('Cannot read file "%s" to add it to the file storage.', $localFilePath),
                1578465610
            );
        }
        $fileContents = file_get_contents($localFilePath);
        if (false === $fileContents) {
            throw new FileOperationErrorException(
                sprintf('Could not read file "%s" to add it to the file storage.', $localFilePath),
                1578465693
            );
        }

        $newFileId = $targetFolderIdentifier . $this->sanitizeFileName($newFileName);
        $numberInserted = $this->getDatabaseConnection()->insert(
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID   => $newFileId,
                self::COLUMNNAME_STORAGE    => $this->storageUid,
                self::COLUMNNAME_DATA       => $fileContents,
            ],
            static::getColumnTypes()
        );

        if (1 !== $numberInserted) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf('Failed to insert new file with name "%s" into the database', $newFileName),
                1578465756
            );
        }

        if ($removeOriginal) {
            if (!is_writable($localFilePath)) {
                throw new FileOperationErrorException(
                    sprintf(
                        'Cannot write file "%s" to remove it from the file system after adding it to the storage.',
                        $localFilePath
                    ),
                    1578465610
                );
            }
            unlink($localFilePath);
        }

        $this->getDatabaseConnection()->commit();

        $this->setExistenceCacheEntry($newFileId, true);

        return $newFileId;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function createFile($fileName, $parentFolderIdentifier): string
    {
        $this->getDatabaseConnection()->beginTransaction();
        if (!$this->folderExists($parentFolderIdentifier)) {
            throw new FileOperationErrorException(
                sprintf(
                    'Parent folder with ID "%s" for creating an empty file with name "%s" does not exist',
                    $parentFolderIdentifier,
                    $fileName
                ),
                1578245907
            );
        }

        $newFileIdentifier = $parentFolderIdentifier . $this->sanitizeFileName($fileName);
        $emptyFileData = [
            self::COLUMNNAME_ENTRY_ID  => $newFileIdentifier,
            self::COLUMNNAME_STORAGE   => $this->storageUid,
            self::COLUMNNAME_DATA      => '',
        ];

        $result = $this->getDatabaseConnection()->insert(
            self::TABLENAME,
            $emptyFileData,
            static::getColumnTypes()
        );
        if (1 === $result) {
            $this->getDatabaseConnection()->commit();
            $this->setExistenceCacheEntry($newFileIdentifier, true);
            return $newFileIdentifier;
        }

        $this->getDatabaseConnection()->rollBack();
        throw new FileOperationErrorException(
            sprintf(
                'Could not create file with name "%s" in folder with identifier "%s"',
                $fileName,
                $parentFolderIdentifier
            ),
            1578245824
        );
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ResourceDoesNotExistException
     * @throws ConnectionException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function copyFileWithinStorage($fileIdentifier, $targetFolderIdentifier, $fileName): string
    {
        $this->getDatabaseConnection()->beginTransaction();

        if (!$this->folderExists($targetFolderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FolderDoesNotExistException(
                sprintf(
                    'Target folder with ID "%s" to copy file with ID "%s" to does not exist',
                    $targetFolderIdentifier,
                    $fileIdentifier
                ),
                1578245907
            );
        }

        if ($this->fileExistsInFolder($targetFolderIdentifier, $fileName)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf(
                    'Target folder with ID "%s" already contains an entry with name "%s".',
                    $targetFolderIdentifier,
                    $fileName
                ),
                1578247088
            );
        }

        $row = $this->getEntryRow($fileIdentifier);

        $newFileIdentifier = $targetFolderIdentifier . $this->sanitizeFileName($fileName);
        $numberInsertedRows = $this->getDatabaseConnection()->insert(
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID  => $newFileIdentifier,
                self::COLUMNNAME_STORAGE   => $this->storageUid,
                self::COLUMNNAME_DATA      => $row[self::COLUMNNAME_DATA],
            ],
            static::getColumnTypes()
        );

        if (1 === $numberInsertedRows) {
            $this->getDatabaseConnection()->commit();
            $this->setExistenceCacheEntry($newFileIdentifier, true);
            return $newFileIdentifier;
        }

        $this->getDatabaseConnection()->rollBack();
        throw new FileOperationErrorException(
            sprintf('Could not copy file "%s".', $targetFolderIdentifier),
            1578247389
        );
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function renameFile($fileIdentifier, $newName): string
    {
        $newFileIdentifier = rtrim(dirname($fileIdentifier), '/') . '/' . $this->sanitizeFileName($newName);

        if ($fileIdentifier === $newFileIdentifier) {
            return $fileIdentifier;
        }

        $this->getDatabaseConnection()->beginTransaction();
        if ($this->fileExists($newFileIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf(
                    'Cannot rename file with ID "%s" to "%s" because that file already exists.',
                    $fileIdentifier,
                    $newName
                ),
                1578681832
            );
        }

        $numberUpdated = $this->getDatabaseConnection()->update(
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID   => $newFileIdentifier,
            ],
            [
                self::COLUMNNAME_ENTRY_ID   => $fileIdentifier,
            ]
        );

        if (1 !== $numberUpdated) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf('Failed to rename file with ID "%s"', $fileIdentifier),
                1578682088
            );
        }

        $this->getDatabaseConnection()->commit();

        $this->setExistenceCacheEntry($fileIdentifier, false);
        $this->setExistenceCacheEntry($newFileIdentifier, true);

        return $newFileIdentifier;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function replaceFile($fileIdentifier, $localFilePath): bool
    {
        $this->getDatabaseConnection()->beginTransaction();
        if (!$this->fileExists($fileIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf(
                    'File with ID "%s" to replace does not exist',
                    $fileIdentifier
                ),
                1578292350
            );
        }

        $this->getDatabaseConnection()->update(
            self::TABLENAME,
            [
                self::COLUMNNAME_DATA       => $this->getBlobDataFromLocalFile($localFilePath),
            ],
            [
                self::COLUMNNAME_ENTRY_ID   => $fileIdentifier,
            ],
            static::getColumnTypes()
        );

        $this->getDatabaseConnection()->commit();
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function deleteFile($fileIdentifier): bool
    {
        $this->getDatabaseConnection()->beginTransaction();
        if (!$this->fileExists($fileIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf(
                    'File with ID "%s" to delete does not exist',
                    $fileIdentifier
                ),
                1578292648
            );
        }

        $numberDeleted = $this->getDatabaseConnection()->delete(
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID => $fileIdentifier
            ],
            static::getColumnTypes()
        );

        if (1 === $numberDeleted) {
            $this->getDatabaseConnection()->commit();
            $this->setExistenceCacheEntry($fileIdentifier, false);
            return true;
        } else {
            $this->getDatabaseConnection()->rollBack();
            return false;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    public function hash($fileIdentifier, $hashAlgorithm): string
    {
        if (!in_array($hashAlgorithm, hash_algos(), true)) {
            throw new FileOperationErrorException(
                sprintf(
                    'Hashing file with ID "%s" failed because hash algorithm "%s" is unsupported.',
                    $fileIdentifier,
                    $hashAlgorithm
                ),
                1578292874
            );
        }

        return hash($hashAlgorithm, $this->getFileContents($fileIdentifier));
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws ConnectionException
     * @throws NoSuchCacheException
     */
    public function moveFileWithinStorage($fileIdentifier, $targetFolderIdentifier, $newFileName): string
    {
        $newFileIdentifier = $targetFolderIdentifier . $this->sanitizeFileName($newFileName, 'UTF-8');

        $this->renameFile($fileIdentifier, $newFileIdentifier);

        return $fileIdentifier;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ConnectionException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function moveFolderWithinStorage($sourceFolderIdentifier, $targetFolderIdentifier, $newFolderName): array
    {
        $this->getDatabaseConnection()->beginTransaction();
        if (!$this->folderExists($sourceFolderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FolderDoesNotExistException(
                sprintf('Folder "%s" for moving does not exist.', $sourceFolderIdentifier),
                1578682477
            );
        }

        if (!$this->folderExists($targetFolderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FolderDoesNotExistException(
                sprintf(
                    'Folder "%s" for moving folder "%s" to does not exist.',
                    $targetFolderIdentifier,
                    $sourceFolderIdentifier
                ),
                1578682539
            );
        }

        $queryBuilder = $this->getDatabaseConnection()->createQueryBuilder();
        $executedQuery = $queryBuilder
            ->select(self::COLUMNNAME_ENTRY_ID)
            ->from(self::TABLENAME)
            ->where(
                $this->getConditionForAllSubEntries($queryBuilder, $sourceFolderIdentifier, true)
            )
            ->execute();

        $identifierMap = [];
        while (false !== ($entryId = $executedQuery->fetchOne())) {

            // If for some reason the fetched identifier does not begin with the folder identifier, ignore it.
            if (!GeneralUtility::isFirstPartOfStr($entryId, $sourceFolderIdentifier)) {
                continue;
            }

            $newEntryIdentifier = $targetFolderIdentifier . $this->sanitizeFileName($newFolderName) . '/'
                . substr($entryId, strlen($sourceFolderIdentifier));
            $numberUpdated = $this->getDatabaseConnection()->update(
                self::TABLENAME,
                [
                    self::COLUMNNAME_ENTRY_ID => $newEntryIdentifier,
                ],
                [
                    self::COLUMNNAME_ENTRY_ID => $entryId,
                ]
            );

            if (1 !== $numberUpdated) {
                $executedQuery->free();
                $this->getDatabaseConnection()->rollBack();
                throw new FileOperationErrorException(
                    sprintf(
                        'Moving folder failed because entry with ID "%s" in storage %d could not be moved',
                        $entryId,
                        $this->storageUid
                    ),
                    1578679807
                );
            }

            $identifierMap[$entryId] = $newEntryIdentifier;
        }
        $executedQuery->free();
        $this->getDatabaseConnection()->commit();

        foreach ($identifierMap as $oldIdentifier => $newEntryIdentifier) {
            $this->setExistenceCacheEntry($oldIdentifier, false);
            $this->setExistenceCacheEntry($newEntryIdentifier, true);
        }

        return $identifierMap;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws FolderDoesNotExistException
     * @throws ConnectionException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function copyFolderWithinStorage($sourceFolderIdentifier, $targetFolderIdentifier, $newFolderName): bool
    {
        $this->getDatabaseConnection()->beginTransaction();
        if (!$this->folderExists($sourceFolderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FolderDoesNotExistException(
                sprintf(
                    'Folder with ID "%s" to copy to folder "%s" does not exist.',
                    $sourceFolderIdentifier,
                    $targetFolderIdentifier
                ),
                1578464961
            );
        }

        if ($this->folderExistsInFolder($newFolderName, $targetFolderIdentifier)) {
            $this->getDatabaseConnection()->rollBack();
            throw new FileOperationErrorException(
                sprintf(
                    'There already is a folder named "%s" in folder with ID "%s".',
                    $newFolderName,
                    $targetFolderIdentifier
                ),
                1578463693
            );
        }

        $copiedFolderIdentifier = $this->createFolder($newFolderName, $targetFolderIdentifier);

        $queryBuilder = $this->getDatabaseConnection()->createQueryBuilder();
        $executedStatement = $queryBuilder
            ->select(self::COLUMNNAME_ENTRY_ID, self::COLUMNNAME_DATA)
            ->from(self::TABLENAME)
            ->where($this->getConditionForAllSubEntries($queryBuilder, $sourceFolderIdentifier))
            ->execute();

        $dataToInsert = [];
        while (false !== ($row = $executedStatement->fetchAssociative())) {
            $oldIdentifier = $row[self::COLUMNNAME_ENTRY_ID];
            $newIdentifier = $copiedFolderIdentifier . substr($oldIdentifier, strlen($sourceFolderIdentifier));

            $this->getExistenceCache()->set($newIdentifier, true);

            $dataToInsert[] = [
                self::COLUMNNAME_ENTRY_ID   => $newIdentifier,
                self::COLUMNNAME_STORAGE    => $this->storageUid,
                self::COLUMNNAME_DATA       => $row[self::COLUMNNAME_DATA],
            ];
        }

        $numberInserted = $this->getDatabaseConnection()->bulkInsert(
            self::TABLENAME,
            $dataToInsert,
            self::COLUMNNAMES,
            static::getColumnTypes()
        );

        if (count($dataToInsert) !== $numberInserted) {
            $this->getDatabaseConnection()->rollBack();
            return false;
        }

        $this->getDatabaseConnection()->commit();

        $this->getExistenceCache()->set($copiedFolderIdentifier, true);
        foreach ($dataToInsert as $item) {
            $this->getExistenceCache()->set($item[self::COLUMNNAME_ENTRY_ID], true);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    public function getFileContents($fileIdentifier)
    {
        if (!$this->fileExists($fileIdentifier)) {
            throw new FileOperationErrorException(
                sprintf(
                    'File with ID "%s" to get contents for does not exist',
                    $fileIdentifier
                ),
                1578293805
            );
        }

        /** @var ResultStatement $statement */
        $statement = $this->getDatabaseConnection()->select(
            [
                self::COLUMNNAME_DATA
            ],
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID => $fileIdentifier,
            ]
        );
        $result = $statement->fetchOne();
        $statement->free();

        if (is_resource($result)) {
            return stream_get_contents($result);
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    public function setFileContents($fileIdentifier, $contents): int
    {
        if (!$this->fileExists($fileIdentifier)) {
            throw new FileOperationErrorException(
                sprintf(
                    'File with ID "%s" to get contents for does not exist',
                    $fileIdentifier
                ),
                1578293805
            );
        }

        $numberRowsChanged = $this->getDatabaseConnection()->update(
            self::TABLENAME,
            [
                self::COLUMNNAME_DATA => $contents,
            ],
            [
                self::COLUMNNAME_ENTRY_ID => $fileIdentifier,
            ],
            static::getColumnTypes()
        );

        return 1 === $numberRowsChanged ? strlen($contents) : 0;
    }

    /**
     * @param $fileName
     * @param $folderIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    public function fileExistsInFolder($fileName, $folderIdentifier): bool
    {
        $fileIdentifier = $folderIdentifier . $this->sanitizeFileName($fileName, 'UTF-8');
        return $this->doesItemWithIdentifierExist($fileIdentifier);
    }

    /**
     * @param $folderName
     * @param $folderIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    public function folderExistsInFolder($folderName, $folderIdentifier): bool
    {
        $queriedFolderIdentifier = $folderIdentifier . $this->sanitizeFileName($folderName, 'UTF-8') . '/';
        return $this->doesItemWithIdentifierExist($queriedFolderIdentifier);
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    public function getFileForLocalProcessing($fileIdentifier, $writable = true): string
    {
        $fileContents = $this->getFileContents($fileIdentifier);
        $temporaryFileName = $this->getTemporaryFileName($fileIdentifier);
        $result = file_put_contents($temporaryFileName, $fileContents);
        if (false === $result) {
            $this->logger->error(
                'Failed to write file to local file for processing.',
                [
                    'temporaryFileName' => $temporaryFileName,
                ]
            );
            throw new FileOperationErrorException(
                'Failed to write file to local file for processing.',
                1643140786
            );
        }

        $this->logger->debug(
            'Got file for local processing',
            [
                'file' => $fileIdentifier,
                'tempPath' => $temporaryFileName,
                'downloadedSize' => filesize($temporaryFileName),
            ]
        );

        return $temporaryFileName;
    }

    public function getPermissions($identifier): array
    {
        return [
            'r' => true,
            'w' => true,
        ];
    }

    /**
     * {@inheritDoc}
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    public function dumpFileContents($identifier)
    {
        echo $this->getFileContents($identifier);
    }

    public function isWithin($folderIdentifier, $identifier): bool
    {
        $folderIdentifier = $this->canonicalizeAndCheckFolderIdentifier($folderIdentifier);

        return str_starts_with($identifier, $folderIdentifier);
    }

    /**
     * {@inheritDoc}
     *
     * @throws ResourceDoesNotExistException
     * @throws FileOperationErrorException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function getFileInfoByIdentifier($fileIdentifier, array $propertiesToExtract = []): array
    {
        $fileInfo = $this->extractFileInformation($fileIdentifier, $propertiesToExtract);
        $this->logger->debug(
            'File info',
            [
                'file' => $fileIdentifier,
                'info' => $fileInfo,
                'requestedProperties' => $propertiesToExtract,
            ]
        );
        return $fileInfo;
    }

    public function getFolderInfoByIdentifier($folderIdentifier): array
    {
        $folderInfo = [
            'identifier' => $folderIdentifier,
            'name' => basename($folderIdentifier),
            'storage' => $this->storageUid
        ];

        $this->logger->debug('Folder info', ['folderId' => $folderIdentifier, 'info' => $folderInfo]);

        return $folderInfo;
    }

    public function getFileInFolder($fileName, $folderIdentifier): string
    {
        return $folderIdentifier . $this->sanitizeFileName($fileName, 'UTF-8');
    }

    /**
     * {@inheritDoc}
     *
     * @throws FolderDoesNotExistException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function getFilesInFolder(
        $folderIdentifier,
        $start = 0,
        $numberOfItems = 0,
        $recursive = false,
        array $filenameFilterCallbacks = [],
        $sort = '',
        $sortRev = false
    ): array {
        $allFolderEntries = $this->getFolderEntries(
            $folderIdentifier,
            $start,
            $numberOfItems,
            $recursive,
            $filenameFilterCallbacks,
            $sort,
            $sortRev
        );

        $this->logger->debug('All entries in folder.', ['folder' => $folderIdentifier, 'entries' => $allFolderEntries]);

        $fileFolderEntries = array_filter(
            $allFolderEntries,
            function (string $entry): bool
            {
                return $this->isFileIdentifier($entry);
            }
        );
        $this->logger->debug(
            'Only file entries in folder.',
            [
                'folder' => $folderIdentifier,
                'files' => $fileFolderEntries,
            ]
        );

        return $fileFolderEntries;
    }

    public function getFolderInFolder($folderName, $folderIdentifier): string
    {
        return $folderIdentifier . $this->sanitizeFileName($folderName, 'UTF-8');
    }

    /**
     * {@inheritDoc}
     *
     * @throws FolderDoesNotExistException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function getFoldersInFolder(
        $folderIdentifier,
        $start = 0,
        $numberOfItems = 0,
        $recursive = false,
        array $folderNameFilterCallbacks = [],
        $sort = '',
        $sortRev = false
    ): array {
        $allFolderEntries = $this->getFolderEntries(
            $folderIdentifier,
            $start,
            $numberOfItems,
            $recursive,
            $folderNameFilterCallbacks,
            $sort,
            $sortRev
        );

        $this->logger->debug('All entries in folder.', ['folder' => $folderIdentifier, 'entries' => $allFolderEntries]);

        $folderFolderEntries = array_filter(
            $allFolderEntries,
            function (string $entry): bool
            {
                return $this->isFolderIdentifier($entry);
            }
        );
        $this->logger->debug(
            'Only folder entries in folder.',
            [
                'folder' => $folderIdentifier,
                'folders' => $folderFolderEntries,
            ]
        );

        return $folderFolderEntries;
    }

    /**
     * {@inheritDoc}
     *
     * @throws FolderDoesNotExistException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function countFilesInFolder(
        $folderIdentifier,
        $recursive = false,
        array $filenameFilterCallbacks = []
    ): int {
        return count(
            $this->getFilesInFolder(
                $folderIdentifier,
                0,
                0,
                $recursive,
                $filenameFilterCallbacks
            )
        );
    }

    /**
     * {@inheritDoc}
     *
     * @throws FolderDoesNotExistException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    public function countFoldersInFolder(
        $folderIdentifier,
        $recursive = false,
        array $folderNameFilterCallbacks = []
    ): int {
        return count(
            $this->getFoldersInFolder(
                $folderIdentifier,
                0,
                0,
                $recursive,
                $folderNameFilterCallbacks
            )
        );
    }

    /**
     * @param string $identifier
     * @return bool
     * @throws NoSuchCacheException
     */
    private function doesItemWithIdentifierExist(string $identifier): bool
    {
        if ('' === $identifier) {
            return true;
        }

        $identifier = '/' . ltrim($identifier, '/');

        if ($this->hasExistenceCacheEntry($identifier)) {
            return $this->getExistenceCacheEntry($identifier);
        }

        $numberEntries = $this->getDatabaseConnection()->count(
            '*',
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID     => $identifier,
            ]
        );

        $itemExists = $numberEntries > 0;
        $this->setExistenceCacheEntry($identifier, $itemExists);
        return $itemExists;
    }

    /**
     * @param string $itemIdentifier
     * @param bool $exists
     * @return void
     * @throws NoSuchCacheException
     */
    private function setExistenceCacheEntry(string $itemIdentifier, bool $exists): void
    {
        $this->getExistenceCache()->set($this->getCacheKey($itemIdentifier), $exists);
    }

    /**
     * @param string $itemIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    private function hasExistenceCacheEntry(string $itemIdentifier): bool
    {
        return $this->getExistenceCache()->has($this->getCacheKey($itemIdentifier));
    }

    /**
     * @param string $itemIdentifier
     * @return bool
     * @throws NoSuchCacheException
     */
    private function getExistenceCacheEntry(string $itemIdentifier): bool
    {
        return $this->getExistenceCache()->get($this->getCacheKey($itemIdentifier));
    }

    private function getCacheKey(string $itemIdentifier): string
    {
        return hash('sha256', $this->storageUid . ':' . $itemIdentifier);
    }

    private function createRootFolder()
    {
        $this->getDatabaseConnection()->insert(
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID   => self::ROOT_FOLDER_ID,
                self::COLUMNNAME_STORAGE    => $this->storageUid,
                self::COLUMNNAME_DATA       => null,
            ],
            static::getColumnTypes()
        );
    }

    /**
     * @throws ResourceDoesNotExistException
     * @throws Exception
     */
    private function getEntryRow(string $entryIdentifier): array
    {
        $result = $this->getDatabaseConnection()->select(
            self::COLUMNNAMES,
            self::TABLENAME,
            [
                self::COLUMNNAME_ENTRY_ID => $entryIdentifier,
            ]
        );

        $row = $result->fetchAssociative();
        $result->free();
        if (false === $row) {
            throw new ResourceDoesNotExistException(
                sprintf('There is no resource with ID "%s".', $entryIdentifier),
                1578665811
            );
        }
        return $row;
    }

    private static function getColumnTypes(): array
    {
        return static::COLUMNTYPES;
    }

    private function getBlobDataFromLocalFile(string $localFilePath)
    {
        if (!is_readable($localFilePath)) {
            return null;
        }

        return file_get_contents($localFilePath);
    }

    /**
     * Extracts information about a file from the filesystem.
     *
     * @param string $identifier
     * @param string[] $propertiesToExtract array of properties which should be returned, if empty all will be extracted
     *
     * @return array
     *
     * @throws ResourceDoesNotExistException
     * @throws FileOperationErrorException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    private function extractFileInformation(string $identifier, array $propertiesToExtract = []): array
    {
        if (empty($propertiesToExtract)) {
            $propertiesToExtract = [
                'size', 'mimetype', 'name', 'extension', 'identifier', 'identifier_hash', 'storage', 'folder_hash'
            ];
        }
        $fileRow = $this->getEntryRow($identifier);

        $fileInformation = [];
        foreach ($propertiesToExtract as $property) {
            $fileInformation[$property] = $this->getSpecificFileInformation($fileRow, $property);
        }
        $this->logger->debug(
            'Got file information',
            [
                'entryId' => $fileRow[self::COLUMNNAME_ENTRY_ID],
                'fileInformation' => $fileInformation,
            ]
        );
        return $fileInformation;
    }

    /**
     * Extracts a specific FileInformation from the FileSystems.
     *
     * @param array $fileRow
     * @param string $property
     *
     * @return bool|int|string
     *
     * @throws FileOperationErrorException
     * @throws NoSuchCacheException
     */
    private function getSpecificFileInformation(array $fileRow, string $property)
    {
        switch ($property) {
            case 'size':
                return strlen($fileRow[self::COLUMNNAME_DATA]);
            case 'name':
                return basename($fileRow[self::COLUMNNAME_ENTRY_ID]);
            case 'extension':
                return PathUtility::pathinfo($fileRow[self::COLUMNNAME_ENTRY_ID], PATHINFO_EXTENSION);
            case 'mimetype':
                $localFilePath = $this->getFileForLocalProcessing($fileRow[self::COLUMNNAME_ENTRY_ID]);
                /** @var FileInfo $fileInfo */
                $fileInfo = GeneralUtility::makeInstance(FileInfo::class, $localFilePath);
                $mimeType = (string)$fileInfo->getMimeType();
                $fileInfo = null;
                unlink($localFilePath);
                return $mimeType;
            case 'identifier':
                return $fileRow[self::COLUMNNAME_ENTRY_ID];
            case 'storage':
                return $this->storageUid;
            case 'identifier_hash':
                return $this->hashIdentifier($fileRow[self::COLUMNNAME_ENTRY_ID]);
            case 'folder_hash':
                $folderIdentifier = rtrim(dirname($fileRow[self::COLUMNNAME_ENTRY_ID])) . '/';
                return $this->hashIdentifier($folderIdentifier);
            default:
                throw new InvalidArgumentException(
                    sprintf('The information "%s" is not available for files from the database storage.', $property),
                    1578378410
                );
        }
    }

    /**
     * @throws FolderDoesNotExistException
     * @throws Exception
     * @throws NoSuchCacheException
     */
    private function getFolderEntries(
        $folderIdentifier,
        $start = 0,
        $numberOfItems = 0,
        $recursive = false,
        array $nameFilterCallbacks = [],
        $sort = '',
        $sortRev = false
    ): array {
        if (!$this->folderExists($folderIdentifier)) {
            throw new FolderDoesNotExistException(
                sprintf('Folder with ID "%s" to get entries for does not exist.', $folderIdentifier),
                1578463218
            );
        }

        $queryBuilder = $this->getDatabaseConnection()->createQueryBuilder();
        $queryBuilder
            ->select(self::COLUMNNAME_ENTRY_ID)
            ->from(self::TABLENAME)
            ->where(
                $this->getConditionForAllSubEntries($queryBuilder, $folderIdentifier)
            );
        if ($start > 0) {
            $queryBuilder->setFirstResult($start);
        }
        if ($numberOfItems > 0) {
            $queryBuilder->setMaxResults($numberOfItems);
        }
        if ('' !== $sort) {
            $queryBuilder->orderBy('entry_id', $sortRev ? 'DESC' : 'ASC');
        }

        $entries = [];
        $executedStatement = $queryBuilder->execute();
        while (false !== ($entryId = $executedStatement->fetchOne())) {
            $identifierPartAfterFolderId = substr($entryId, strlen($folderIdentifier));
            $this->logger->debug(
                'Got entry',
                [
                    'id' => $entryId,
                    'folderId' => $folderIdentifier,
                    'partAfterFolderId' => $identifierPartAfterFolderId,
                ]
            );

            // Skip entries which are in subfolders if entries should not be fetched recursively
            $isDirectChildOfFolder = false === strpos(rtrim($identifierPartAfterFolderId, '/'), '/');
            if (!$recursive && !$isDirectChildOfFolder) {
                $this->logger->debug('Not fetching recursively, and entry is no direct child - skipping');
                continue;
            }

            $entryFolderIdentifier = rtrim(dirname($entryId)) . '/';
            $entryName = trim(basename($entryId), '/');
            foreach ($nameFilterCallbacks as $nameFilterCallback) {
                $this->logger->debug(
                    'Checking name filter',
                    [
                        'filter' => $nameFilterCallback,
                        'entryId' => $entryId,
                        'folderId' => $entryFolderIdentifier,
                        'name' => $entryName,
                    ]
                );
                if (-1 === $nameFilterCallback($entryName, $entryId, $entryFolderIdentifier, [], $this)) {
                    $this->logger->debug(
                        'Filtering entry out.',
                        [
                            'filter' => $nameFilterCallback,
                            'entryId' => $entryId,
                            'folderId' => $entryFolderIdentifier,
                            'name' => $entryName,
                        ]
                    );
                    continue 2;
                }
            }

            $entries[] = $entryId;
        }

        return $entries;
    }

    private function isFileIdentifier(string $identifier): bool
    {
        return '/' !== substr($identifier, -1);
    }

    private function isFolderIdentifier(string $identifier): bool
    {
        return '/' === substr($identifier, -1);
    }

    private function getConditionForAllSubEntries(
        QueryBuilder $queryBuilder,
        string $folderIdentifier,
        bool $includeFolder = false
    ): CompositeExpression {
        $expression = $queryBuilder->expr()->andX(
            $queryBuilder->expr()->eq(
                self::COLUMNNAME_STORAGE,
                $queryBuilder->expr()->literal($this->storageUid)
            ),
            $queryBuilder->expr()->like(
                self::COLUMNNAME_ENTRY_ID,
                $queryBuilder->expr()->literal($folderIdentifier . '%')
            )
        );
        if (!$includeFolder) {
            $expression = $queryBuilder->expr()->andX(
                $expression,
                $queryBuilder->expr()->neq(
                    self::COLUMNNAME_ENTRY_ID,
                    $queryBuilder->expr()->literal($folderIdentifier)
                )
            );
        }

        return $expression;
    }

    /**
     * Creates a new temporary file and returns the path to it. Adds it to the list of files to be
     * deleted when the driver is destructed.
     *
     * @param string $fileIdentifier
     *
     * @return string
     *
     * @throws FileOperationErrorException
     */
    private function getTemporaryFileName(string $fileIdentifier): string
    {
        $temporaryFileName = tempnam(sys_get_temp_dir(), 'typo3_fal_database');
        if (false === $temporaryFileName) {
            throw new FileOperationErrorException(
                'Could not create temporary file in database FAL driver.',
                1580127165
            );
        }

        // Add file extension, otherwise the file will not be processed because the extension
        // is not in the list of whitelisted extensions.
        $newTemporaryFilename = $temporaryFileName . '.' . pathinfo($fileIdentifier, PATHINFO_EXTENSION);
        if (file_exists($newTemporaryFilename)) {
            throw new FileOperationErrorException(
                'Could not rename temporary file in database FAL driver to contain the correct extension, '
                . 'because a file with the correct extension already exists.',
                1580145965
            );
        }
        $renameResult = rename($temporaryFileName, $newTemporaryFilename);
        if (false === $renameResult) {
            throw new FileOperationErrorException(
                'Could not rename temporary file in database FAL driver to contain the correct extension.',
                1580145901
            );
        }

        $this->tempfileNames[] = $newTemporaryFilename;

        return $newTemporaryFilename;
    }

    private function getDatabaseConnection(): Connection
    {
        if (null !== $this->databaseConnection) {
            return $this->databaseConnection;
        }

        /** @var ConnectionPool $connectionPool */
        $connectionPool = GeneralUtility::makeInstance(ConnectionPool::class);
        return $connectionPool->getConnectionForTable(self::TABLENAME);
    }

    public function __sleep()
    {
        $this->databaseConnection = null;
    }

    public function __wakeup()
    {
        $this->databaseConnection = $this->getDatabaseConnection();
    }
}
