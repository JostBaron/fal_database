<?php

declare(strict_types=1);

namespace Jbaron\FalDatabase\Tests\Functional\Command;

use Jbaron\FalDatabase\Command\MigrateToDatabaseStorageCommand;
use Jbaron\FalDatabase\Tests\Functional\AbstractFunctionalTestCase;
use Symfony\Component\Console\Tester\CommandTester;
use TYPO3\CMS\Core\Core\Environment;
use TYPO3\CMS\Core\Utility\GeneralUtility;

class MigrateToDatabaseStorageCommandTest extends AbstractFunctionalTestCase
{
    /**
     * @test
     */
    public function commandIsRegisteredUnderItsName(): void
    {
        $command = GeneralUtility::makeInstance(MigrateToDatabaseStorageCommand::class);

        self::assertSame('faldatabase:migrate-to-database-storage', $command->getName());
    }

    /**
     * @test
     */
    public function migratesFilesAndExitsWithZero(): void
    {
        $sourcePath = Environment::getPublicPath() . '/migration-source/';
        $this->createLocalStorage(self::LOCAL_STORAGE_UID, $sourcePath);
        file_put_contents($sourcePath . 'a.txt', 'contents');
        $this->getStorage(self::LOCAL_STORAGE_UID)->getFile('/a.txt');
        $this->getStorage(self::DATABASE_STORAGE_UID)->getRootLevelFolder();

        $tester = new CommandTester(GeneralUtility::makeInstance(MigrateToDatabaseStorageCommand::class));
        $exitCode = $tester->execute([
            'storage-id-from' => (string)self::LOCAL_STORAGE_UID,
            'storage-id-to' => (string)self::DATABASE_STORAGE_UID,
        ]);

        self::assertSame(0, $exitCode, $tester->getDisplay());
        self::assertSame('contents', $this->getEntryData('/a.txt'));
    }

    /**
     * @test
     */
    public function printsErrorsAndExitsWithOneForMissingStorages(): void
    {
        $tester = new CommandTester(GeneralUtility::makeInstance(MigrateToDatabaseStorageCommand::class));
        $exitCode = $tester->execute([
            'storage-id-from' => '998',
            'storage-id-to' => '999',
        ]);

        self::assertSame(1, $exitCode);
        self::assertStringContainsString('The source storage does not exist.', $tester->getDisplay());
        self::assertStringContainsString('The target storage does not exist.', $tester->getDisplay());
    }
}
