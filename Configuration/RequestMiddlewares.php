<?php
declare(strict_types=1);

return [
    'frontend' => [
        'jbaron/fal_database/filedelivery' => [
            'target' => \Jbaron\FalDatabase\Middleware\FileDeliveryMiddleware::class,
            'after' => [
                'typo3/cms-core/normalized-params-attribute',
            ],
            'before' => [
                'typo3/cms-frontend/eid',
            ],
        ],
    ],
];
