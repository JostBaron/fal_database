<?php
if (!defined('TYPO3_MODE')) {
    die ('Access denied.');
}

/** @var \TYPO3\CMS\Core\Resource\Driver\DriverRegistry $driverRegistry */
$driverRegistry = \TYPO3\CMS\Core\Utility\GeneralUtility::makeInstance(
    \TYPO3\CMS\Core\Resource\Driver\DriverRegistry::class
);
$driverRegistry->registerDriverClass(
    \Jbaron\FalDatabase\Driver\DatabaseDriver::class,
    \Jbaron\FalDatabase\Driver\DatabaseDriver::DRIVER_KEY,
    'Database driver for FAL',
    'FILE:EXT:fal_database/Configuration/FlexForm/DriverFlexForm.xml'
);

$GLOBALS['TYPO3_CONF_VARS']['FE']['eID_include']['fal_database_download']
    = \Jbaron\FalDatabase\Controller\PublicUrlController::class . '::dumpFile';

if (!\is_array($GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations']['tx_jbaron_faldatabase_existencecache'])) {
    $GLOBALS['TYPO3_CONF_VARS']['SYS']['caching']['cacheConfigurations']['tx_jbaron_faldatabase_existencecache'] = [];
}
