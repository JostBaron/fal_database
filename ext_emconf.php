<?php
$EM_CONF[$_EXTKEY] = [
    'title' => 'Database driver for FAL',
    'description' => 'TYPO3 FAL storage driver for storing files in the database',
    'category' => 'misc',
    'author' => 'Jost Baron',
    'author_email' => 'j.baron@netzkoenig.de',
    'author_company' => 'Mein Bauernhof GbR',
    'state' => 'alpha',
    'version' => '0.0.1',
    'constraints' => [
        'depends' => [
            'typo3' => '8.7.0-10.4.999',
        ]
    ]
];
