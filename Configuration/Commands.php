<?php

declare(strict_types=1);

use Jbaron\FalDatabase\Command\MigrateBetweenStoragesCommand;

return [
    'faldatabase:migratestorage' => [
        'class' => MigrateBetweenStoragesCommand::class,
    ]
];
