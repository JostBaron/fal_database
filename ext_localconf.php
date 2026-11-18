<?php

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

// TYPO3 12 registers the upgrade wizard by its #[UpgradeWizard] attribute.
if ((new \TYPO3\CMS\Core\Information\Typo3Version())->getMajorVersion() < 12) {
    $GLOBALS['TYPO3_CONF_VARS']['SC_OPTIONS']['ext/install']['update']['falDatabaseEntryTable']
        = \Jbaron\FalDatabase\Upgrades\EntryTableUpgradeWizard::class;
}
