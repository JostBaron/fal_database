<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Service;

use TYPO3\CMS\Core\Resource\Exception\ExistingTargetFileNameException;
use TYPO3\CMS\Core\Resource\Exception\ExistingTargetFolderException;
use TYPO3\CMS\Core\Resource\Exception\InsufficientFolderAccessPermissionsException;
use TYPO3\CMS\Core\Resource\Exception\InsufficientFolderWritePermissionsException;
use TYPO3\CMS\Core\Resource\File;
use TYPO3\CMS\Core\Resource\Folder;
use TYPO3\CMS\Core\SingletonInterface;

class MigrationService implements SingletonInterface
{
    /**
     * @var \TYPO3\CMS\Core\Resource\StorageRepository
     * @inject
     */
    protected $storageRepository;

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
    public function migrateFolderToDifferentStorage(
        int $sourceStorageId,
        int $targetStorageId,
        string $sourceFolderIdentifier = null,
        string $targetFolderIdentifier = null
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
            $sourceFolderIdentifier = $sourceStorage->getRootLevelFolder(false);
            if (null === $sourceFolderIdentifier) {
                $errorMessages[] = 'The source folder does not exist.';
            }
        }
        if (null === $targetFolderIdentifier) {
            $targetFolderIdentifier = $targetStorage->getRootLevelFolder(false);
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

        return $this->moveFolderContents([$sourceFolder->getIdentifier()], $sourceFolder, $targetFolder);
    }

    private function moveFolderContents(array $currentSubPath, Folder $sourceFolder, Folder $targetFolder): array
    {
        $errorMessages = [];

        $subFolders = $sourceFolder->getSubfolders(0, 0, Folder::FILTER_MODE_NO_FILTERS);
        foreach ($subFolders as $subFolder) {

            $targetFolderCreationSuccessful = false;
            try {
                $targetSubfolder = $targetFolder->createFolder($subFolder->getName());
                $targetFolderCreationSuccessful = true;
            } catch (InsufficientFolderWritePermissionsException $writePermissionsException) {
                $errorMessages[] = \sprintf(
                    'The target subfolder named "%s" cannot be created due '
                        . 'to missing permissions. Current source subpath: [%s]',
                    $subFolder->getName(),
                    \implode(', ', $currentSubPath)
                );
            } catch (ExistingTargetFolderException $existingTargetFolderException) {
                $errorMessages[] = \sprintf(
                    'The target subfolder named "%s" cannot be created because it already exists. '
                        . 'Current source subpath: [%s]',
                    $subFolder->getName(),
                    \implode(', ', $currentSubPath)
                );
            }

            if ($targetFolderCreationSuccessful) {
                $subErrors = $this->moveFolderContents(
                    \array_merge($currentSubPath, [$subFolder->getIdentifier()]),
                    $subFolder,
                    $targetSubfolder
                );
            } else {
                $errorMessages[] = 'Not copying subfolder because it could not be created.';
            }

            $errorMessages = \array_merge($errorMessages, $subErrors);
        }

        $filesInFolder = $sourceFolder->getFiles(0, 0, Folder::FILTER_MODE_NO_FILTERS);
        foreach ($filesInFolder as $fileInFolder) {
            $localFilePath = $fileInFolder->getForLocalProcessing(false);
            try {
                $targetFolder->addFile($localFilePath, $fileInFolder->getName());
            } catch (ExistingTargetFileNameException $exception) {
                $errorMessages[] = \sprintf(
                    'The target file named "%s" cannot be created because there already exists a file with '
                        . 'that name. Current source subpath: [%s]',
                    $fileInFolder->getName(),
                    \implode(', ', $currentSubPath)
                );
            }
        }

        return $errorMessages;
    }
}
