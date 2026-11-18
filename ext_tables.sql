#
# One row per file or folder. Folder identifiers end with "/".
#
# Identifiers are only unique per storage. The binary collation makes them case-sensitive
# on MySQL/MariaDB, like the driver claims to be. It is a table option because TYPO3 ignores
# column collations; other DBMS ignore it and compare case-sensitively anyway.
#
CREATE TABLE tx_jbaron_faldatabase_entry (
    storage int(11) DEFAULT '0' NOT NULL,
    entry_id varchar(255) DEFAULT '' NOT NULL,

    data longblob,

    PRIMARY KEY (storage, entry_id)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
