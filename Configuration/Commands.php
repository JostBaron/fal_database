<?php

declare(strict_types=1);

use Jbaron\FalDatabase\Command\MigrateToDatabaseStorageCommand;

return [
    'faldatabase:migrate-to-database-storage' => [
        'class' => MigrateToDatabaseStorageCommand::class,
    ]
];
