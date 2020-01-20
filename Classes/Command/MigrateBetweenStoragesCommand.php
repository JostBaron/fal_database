<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Command;

use Jbaron\FalDatabase\Service\MigrationService;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use TYPO3\CMS\Core\Utility\GeneralUtility;
use TYPO3\CMS\Extbase\Object\ObjectManager;
use TYPO3\CMS\Extbase\Object\ObjectManagerInterface;

/**
 * Command for migrating files and folders between FAL storages.
 *
 * @package Jbaron\FalDatabase\Command
 */
class MigrateBetweenStoragesCommand extends Command
{
    const ARGUMENT_NAME_STORAGEFROM = 'storage-id-from';
    const ARGUMENT_NAME_STORAGETO = 'storage-id-to';

    const OPTION_NAME_SOURCEFOLDER_IDENTIFIER = 'sourcefolder-identifier';
    const OPTION_NAME_TARGETFOLDER_IDENTIFIER = 'targetfolder-identifier';

    /**
     * @var \Jbaron\FalDatabase\Service\MigrationService
     */
    protected $migrationService;

    protected function configure()
    {
        $this->setDescription(
            'Migrate folders between storages.'
        );

        $this->addArgument(
            self::ARGUMENT_NAME_STORAGEFROM,
            InputArgument::REQUIRED,
            'ID of the source storage',
            null
        );
        $this->addArgument(
            self::ARGUMENT_NAME_STORAGETO,
            InputArgument::REQUIRED,
            'ID of the target storage',
            null
        );

        $this->addOption(
            self::OPTION_NAME_SOURCEFOLDER_IDENTIFIER,
            's',
            InputOption::VALUE_OPTIONAL,
            'The folder identifier of the source folder. If not given, the root folder of the storage is used.',
            null
        );

        $this->addOption(
            self::OPTION_NAME_TARGETFOLDER_IDENTIFIER,
            't',
            InputOption::VALUE_OPTIONAL,
            'The folder identifier of the target folder. If not given, the root folder of the storage is used.',
            null
        );
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     *
     * @return int
     *
     * @throws \Exception
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        /** @var ObjectManagerInterface $objectManager */
        $objectManager = GeneralUtility::makeInstance(ObjectManager::class);
        $this->migrationService = $objectManager->get(MigrationService::class);

        $errorMessages = $this->migrationService->migrateFolderToDifferentStorage(
            (int)$input->getArgument(self::ARGUMENT_NAME_STORAGEFROM),
            (int)$input->getArgument(self::ARGUMENT_NAME_STORAGETO),
            $input->getOption(self::OPTION_NAME_SOURCEFOLDER_IDENTIFIER),
            $input->getOption(self::OPTION_NAME_TARGETFOLDER_IDENTIFIER)
        );

        if ([] === $errorMessages) {
            return 0;
        }

        $output->writeln($errorMessages);

        return 1;
    }
}
