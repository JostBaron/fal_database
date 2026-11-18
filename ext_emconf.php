<?php
$EM_CONF[$_EXTKEY] = [
    'title' => 'Database driver for FAL',
    'description' => 'TYPO3 FAL storage driver for storing files in the database',
    'category' => 'misc',
    'author' => 'Jost Baron',
    'author_email' => 'j.baron@netzkoenig.de',
    'author_company' => 'Mein Bauernhof GbR',
    'state' => 'stable',
    'version' => '1.3.0',
    'constraints' => [
        'depends' => [
            'php' => '7.4.0-8.99.99',
            'typo3' => '11.5.0-12.4.99',
        ],
    ],
];
