<?php
if (!defined('TYPO3_MODE')) {
    die ('Access denied.');
}

$GLOBALS['TYPO3_CONF_VARS']['SYS']['fal']['registeredDrivers'][] = [
    'class' => \Jbaron\FalDatabase\Driver\DatabaseDriver::class,
    'shortName' => \Jbaron\FalDatabase\Driver\DatabaseDriver::DRIVER_KEY,
    'label' => 'Database driver for FAL',
    'flexFormDS' => 'FILE:EXT:fal_database/Configuration/FlexForm/DriverFlexForm.xml',
];

if (!\is_array($GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations']['tx_jbaron_faldatabase_existencecache'])) {
    $GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations']['tx_jbaron_faldatabase_existencecache'] = [];
}
