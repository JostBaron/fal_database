
CREATE TABLE tx_jbaron_faldatabase_entry (
    entry_id VARCHAR(255),
    storage int(11) NOT NULL,

    data longblob,

    PRIMARY KEY (entry_id),
    FOREIGN KEY storage(storage) REFERENCES sys_file_storage(uid) ON DELETE CASCADE,
    UNIQUE KEY uniqueEntriesInFolder(storage, entry_id(255)),
);

