<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Service;

use Psr\Log\LoggerInterface;
use TYPO3\CMS\Core\Database\Connection;
use TYPO3\CMS\Core\Database\ConnectionPool;
use TYPO3\CMS\Core\Log\LogManager;
use TYPO3\CMS\Core\Resource\Driver\DriverInterface;
use TYPO3\CMS\Core\Resource\Driver\LocalDriver;
use TYPO3\CMS\Core\Resource\Exception\ExistingTargetFolderException;
use TYPO3\CMS\Core\Resource\Exception\InsufficientFolderAccessPermissionsException;
use TYPO3\CMS\Core\Resource\Exception\InsufficientFolderWritePermissionsException;
use TYPO3\CMS\Core\Resource\Folder;
use TYPO3\CMS\Core\Resource\ResourceFactory;
use TYPO3\CMS\Core\Resource\ResourceStorage;
use TYPO3\CMS\Core\Resource\StorageRepository;
use TYPO3\CMS\Core\SingletonInterface;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Extbase\Object\ObjectManager;

class MigrationService implements SingletonInterface
{
    private const TABLENAME = 'sys_file';

    private StorageRepository $storageRepository;
    private Connection $databaseConnection;
    private LoggerInterface $logger;

    /**
     * IDs of folders to delete from old storage directly.
     *
     * @var string[]
     */
    private array $foldersToDeleteInDriver;

    /**
     * IDs of files to delete from old storage directly.
     *
     * @var string[]
     */
    private array $filesToDeleteInDriver;

    public function __construct(StorageRepository $storageRepository)
    {
        $this->storageRepository = $storageRepository;

        /** @var ConnectionPool $connectionPool */
        $connectionPool = GeneralUtility::makeInstance(ConnectionPool::class);
        $this->databaseConnection = $connectionPool->getConnectionForTable(self::TABLENAME);

        $this->logger = GeneralUtility::makeInstance(LogManager::class)->getLogger(__CLASS__);
    }

    /**
     * @param int $sourceStorageId ID of the source file storage.
     * @param int $targetStorageId ID of the target file storage.
     * @param string|null $sourceFolderIdentifier Identifier of the folder whose contents to move,
     * WITHOUT storage ID. If null the root folder of the storage is used.
     * @param string|null $targetFolderIdentifier Identifier of the target folder, WITOUT storage ID.
     * The moved folders contents will be directly inserted into this folder, so make sure no naming
     * conflicts are possible. If null the root folder of the storage is used.
     *
     * @return string[] An array of error messages. Empty in case of no errors.
     *
     * @throws \Exception
     */
    public function migrateFolderToDatabaseStorage(
        int $sourceStorageId,
        int $targetStorageId,
        ?string $sourceFolderIdentifier = null,
        ?string $targetFolderIdentifier = null
    ): array {
        $errorMessages = [];

        $sourceStorage = $this->storageRepository->findByUid($sourceStorageId);
        if (null === $sourceStorage) {
            $errorMessages[] = 'The source storage does not exist.';
        }

        $targetStorage = $this->storageRepository->findByUid($targetStorageId);
        if (null === $targetStorage) {
            $errorMessages[] = 'The target storage does not exist.';
        }

        if (null === $sourceStorage || null === $targetStorage) {
            return $errorMessages;
        }

        if (null === $sourceFolderIdentifier) {
            $sourceFolderIdentifier = $sourceStorage->getRootLevelFolder(false)->getIdentifier();
            if (null === $sourceFolderIdentifier) {
                $errorMessages[] = 'The source folder does not exist.';
            }
        }
        if (null === $targetFolderIdentifier) {
            $targetFolderIdentifier = $targetStorage->getRootLevelFolder(false)->getIdentifier();
            $errorMessages[] = 'The target folder does not exist.';
        }

        try {
            $sourceFolder = $sourceStorage->getFolder($sourceFolderIdentifier);
        } catch (InsufficientFolderAccessPermissionsException $exception) {
            $sourceFolder = null;
            $errorMessages[] = 'Source folder is not accessible.';
        }

        try {
            $targetFolder = $targetStorage->getFolder($targetFolderIdentifier);
        } catch (InsufficientFolderAccessPermissionsException $exception) {
            $targetFolder = null;
            $errorMessages[] = 'Target folder is not accessible.';
        }

        if (null === $sourceFolder || null === $targetFolder) {
            return $errorMessages;
        }

        $sourceDriver = $this->getSourceStorageDriverInstance($sourceStorage);

        $this->logger->info(
            'Starting migration.',
            [
                'sourceStorageId' => $sourceStorage->getUid(),
                'sourceFolderId' => $sourceFolder->getIdentifier(),
                'targetStorageId' => $targetStorage->getUid(),
                'targetFolderId' => $targetFolder->getIdentifier(),
            ]
        );

        $this->foldersToDeleteInDriver = [];
        $this->filesToDeleteInDriver = [];
        $this->databaseConnection->beginTransaction();
        $errorMessages = $this->moveFolderContents([$sourceFolder->getIdentifier()], $sourceFolder, $targetFolder);

        if ([] !== $errorMessages) {
            $this->databaseConnection->rollBack();
            $errorMessage = 'Rolled back changes because errors occurred.';
            $this->logger->error($errorMessage);
            $errorMessages[] = $errorMessage;
            return $errorMessages;
        }

        $this->databaseConnection->commit();

        $this->logger->info('Deleting files from source storage', ['oldFileIds' => $this->filesToDeleteInDriver]);
        foreach ($this->filesToDeleteInDriver as $fileId) {
            $this->logger->debug('Deleting file from source storage', ['fileId' => $fileId]);
            try {
                $sourceDriver->deleteFile($fileId);
            } catch (\Throwable $throwable) {
                $this->logger->error(
                    'Deletion of source file directly in the driver failed.',
                    [
                        'exception' => $throwable,
                    ]
                );
            }
        }

        $this->logger->info('Deleting folders from source storage', ['oldFileIds' => $this->foldersToDeleteInDriver]);
        foreach ($this->foldersToDeleteInDriver as $folderId) {
            $this->logger->debug('Deleting folder from source storage', ['folderId' => $folderId]);
            try {
                $sourceDriver->deleteFolder($folderId, true);
            } catch (\Throwable $throwable) {
                $this->logger->error(
                    'Deletion of source folder directly in the driver failed.',
                    [
                        'exception' => $throwable,
                    ]
                );
            }
        }

        $this->logger->info('Done!');

        return [];
    }

    private function moveFolderContents(array $currentSubPath, Folder $sourceFolder, Folder $targetFolder): array
    {
        $errorMessages = [];

        // First, we need to create the folders in the target storage - no database
        // updates are needed, because they are not stored by the TYPO3 core.
        $subFolders = $sourceFolder->getSubfolders(0, 0, Folder::FILTER_MODE_NO_FILTERS);
        $this->logger->info('Moving subfolders.', ['numberSubfolders' => \count($subFolders)]);
        foreach ($subFolders as $subFolder) {
            try {
                $this->logger->info(
                    'Moving subfolder.',
                    [
                        'subfolderId' => $subFolder->getIdentifier(),
                        'targetFolderId' => $targetFolder->getIdentifier(),
                    ]
                );
                $targetSubfolder = $targetFolder->createFolder($subFolder->getName());
                $this->logger->debug(
                    'Created target subfolder. Moving recursively now.',
                    [
                        'createdFolderId' => $targetSubfolder->getIdentifier(),
                    ]
                );

                $subErrors = $this->moveFolderContents(
                    \array_merge($currentSubPath, [$subFolder->getIdentifier()]),
                    $subFolder,
                    $targetSubfolder
                );

                $this->foldersToDeleteInDriver[] = $subFolder->getIdentifier();

                $errorMessages = \array_merge($errorMessages, $subErrors);

            } catch (InsufficientFolderWritePermissionsException $writePermissionsException) {
                $errorMessage = \sprintf(
                    'The target subfolder named "%s" cannot be created due '
                    . 'to missing permissions. Current source subpath: [%s]',
                    $subFolder->getName(),
                    \implode(', ', $currentSubPath)
                );
                $this->logger->error($errorMessage, ['exception' => $writePermissionsException]);
                $errorMessages[] = $errorMessage;
            } catch (ExistingTargetFolderException $existingTargetFolderException) {
                $errorMessage = \sprintf(
                    'The target subfolder named "%s" cannot be created because it already exists. '
                    . 'Current source subpath: [%s]',
                    $subFolder->getName(),
                    \implode(', ', $currentSubPath)
                );
                $this->logger->error($errorMessage, ['exception' => $existingTargetFolderException]);
                $errorMessages[] = $errorMessage;
            }
        }

        $filesInFolder = $sourceFolder->getFiles(0, 0, Folder::FILTER_MODE_NO_FILTERS);
        $this->logger->info('Moving files.', ['numberFiles' => \count($filesInFolder)]);
        foreach ($filesInFolder as $fileInFolder) {
            $localFilePath = $fileInFolder->getForLocalProcessing(false);
            try {
                $targetFolderIdentifier = $this->normalizeFolderIdentifier($targetFolder->getIdentifier());
                $oldFileIdentifier = $fileInFolder->getIdentifier();
                $newFileIdentifier = $targetFolderIdentifier . str_replace('/', '_', $fileInFolder->getName());
                $fileContents = \file_get_contents($localFilePath);
                $targetStorageId = $targetFolder->getStorage()->getUid();

                $this->logger->info(
                    'Inserting file into database storage',
                    [
                        'entry_id' => $newFileIdentifier,
                        'storageId' => $targetStorageId
                    ]
                );

                $numberInserted = $this->databaseConnection->insert(
                    'tx_jbaron_faldatabase_entry',
                    [
                        'entry_id'      => $newFileIdentifier,
                        'storage'       => $targetStorageId,
                        'data'          => $fileContents,
                    ]
                );

                $this->logger->info(
                    'Inserted file into database storage',
                    [
                        'numberInsertedRows' => $numberInserted,
                    ]
                );

                $this->logger->info('Updating sys_file record');
                $numUpdated = $this->databaseConnection->update(
                    self::TABLENAME,
                    [
                        'tstamp' => \time(),
                        'storage' => $targetStorageId,
                        'identifier' => $newFileIdentifier,
                        'identifier_hash' => \hash('sha1', $newFileIdentifier),
                        'folder_hash' => \hash('sha1', $targetFolderIdentifier),
                    ],
                    [
                        'uid' => $fileInFolder->getUid(),
                    ]
                );

                if ($numberInserted !== 1) {
                    $errorMessage = \sprintf(
                        'Could not create database file system entry for file with old ID "%s" and new ID "%s".',
                        $fileInFolder->getIdentifier(),
                        $newFileIdentifier
                    );
                    $this->logger->error($errorMessage);
                    $errorMessages[] = $errorMessage;
                }

                if ($numUpdated !== 1) {
                    $errorMessage = \sprintf(
                        'Could not update sys_file entry for moved file with ID "%s" and new ID "%s".',
                        $fileInFolder->getIdentifier(),
                        $newFileIdentifier
                    );
                    $this->logger->error($errorMessage);
                    $errorMessages[] = $errorMessage;
                }

                $this->filesToDeleteInDriver[] = $oldFileIdentifier;
            } catch (\Throwable $throwable) {
                $errorMessage = \sprintf(
                    'The file with ID "%s" could not be moved. Reason: "%s".',
                    $fileInFolder->getIdentifier(),
                    substr($throwable->getMessage(), 0, 100)
                );
                $this->logger->error($errorMessage, ['exception' => $throwable]);
                $errorMessages[] = $errorMessage;
            }
        }

        return $errorMessages;
    }

    private function normalizeFolderIdentifier(string $folderIdentifier): string
    {
        return \rtrim($folderIdentifier, '/') . '/';
    }

    private function getSourceStorageDriverInstance(ResourceStorage $sourceStorage): DriverInterface
    {
        $objectManager = GeneralUtility::makeInstance(ObjectManager::class);
        /** @var ResourceFactory $resourceFactory */
        $resourceFactory = $objectManager->get(ResourceFactory::class);
        $storageRow = $this->databaseConnection->select(
            [
                'driver',
                'configuration',
            ],
            'sys_file_storage',
            [
                'uid' => $sourceStorage->getUid(),
            ]
        )->fetch();

        if (false === $storageRow) {
            throw new \RuntimeException(
                \sprintf(
                    'Could not retrieve source storage data for storage %d from database.',
                    $sourceStorage->getUid()
                ),
                1579621183
            );
        }

        $sourceDriverKey = $storageRow['driver'];
        $sourceDriverConfiguration = $resourceFactory->convertFlexFormDataToConfigurationArray(
            $storageRow['configuration']
        );

        switch ($sourceDriverKey) {
            case 'Local':
                if (!\array_key_exists('pathType', $sourceDriverConfiguration)
                    || 'absolute' !== $sourceDriverConfiguration['pathType']) {
                    throw new \RuntimeException(
                        \sprintf(
                            'Storage with ID %d uses the "Local" driver, but does not have an absolute '
                            . 'path configured as base path. This is necessary in order to remove the moved files '
                            . 'from the file system after moving.',
                            $sourceStorage->getUid()
                        ),
                        1579621628
                    );
                }
            // Add checks for other drivers here later.
        }

        $sourceDriver = $resourceFactory->getDriverObject(
            $sourceDriverKey,
            $sourceDriverConfiguration
        );
        $sourceDriver->processConfiguration();
        $sourceDriver->initialize();

        return $sourceDriver;
    }
}
