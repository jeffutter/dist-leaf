use std::sync::Arc;

use rocksdb::DB;
use tempfile::TempDir;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("database error")]
    RocksDB(#[from] rocksdb::Error),
    #[error("unknown database error")]
    Unknown,
}

#[derive(Clone)]
pub struct Database {
    db: Arc<DB>,
}

impl Database {
    pub fn new(path: &std::path::Path) -> Self {
        let db = DB::open_default(path).unwrap();
        Self { db: Arc::new(db) }
    }

    pub fn new_tmp() -> Self {
        let tmp_dir = TempDir::new().unwrap();

        Self::new(tmp_dir.path())
    }

    pub fn put(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        self.db.put(key.as_bytes(), value.as_bytes())?;

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        match self.db.get(key.as_bytes())? {
            Some(v) => {
                let result = String::from_utf8(v).unwrap();
                log::debug!("Finding '{}' returns '{}'", key, result);
                Ok(Some(result))
            }
            None => {
                log::debug!("Finding '{}' returns None", key);
                Ok(None)
            }
        }
    }

    pub fn delete(&self, key: &str) -> Result<(), DatabaseError> {
        self.db.delete(key.as_bytes())?;
        Ok(())
    }
}
