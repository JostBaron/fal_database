<?php
if (!defined('TYPO3_MODE')) {
    die ('Access denied.');
}

$GLOBALS['TYPO3_CONF_VARS']['SYS']['fal']['registeredDrivers'][\Jbaron\FalDatabase\Driver\DatabaseDriver::DRIVER_KEY] = [
    'class' => \Jbaron\FalDatabase\Driver\DatabaseDriver::class,
    'shortName' => \Jbaron\FalDatabase\Driver\DatabaseDriver::DRIVER_KEY,
    'label' => 'Database driver for FAL',
    'flexFormDS' => 'FILE:EXT:fal_database/Configuration/FlexForm/DriverFlexForm.xml',
];

if (!array_key_exists(
    \Jbaron\FalDatabase\Driver\DatabaseDriver::CACHE_EXISTENCE_NAME,
    $GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations']
)) {
    $GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations'][\Jbaron\FalDatabase\Driver\DatabaseDriver::CACHE_EXISTENCE_NAME] = [];
}

$GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['ext/install']['update']['falDatabaseEntryTable']
    = \Jbaron\FalDatabase\Upgrades\EntryTableUpgradeWizard::class;
