use std::sync::Arc;

use rocksdb::DB;
use tempfile::TempDir;
use thiserror::Error;
use tracing::instrument;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("database error")]
    RocksDB(#[from] rocksdb::Error),
    #[error("unknown database error")]
    Unknown,
}

#[derive(Clone, Debug)]
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

    #[instrument]
    pub fn put(&self, key: &str, value: &str) -> Result<(), DatabaseError> {
        self.db.put(key.as_bytes(), value.as_bytes())?;

        Ok(())
    }

    #[instrument]
    pub fn get(&self, key: &str) -> Result<Option<String>, DatabaseError> {
        match self.db.get(key.as_bytes())? {
            Some(v) => {
                let result = String::from_utf8(v).unwrap();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    #[instrument]
    pub fn delete(&self, key: &str) -> Result<(), DatabaseError> {
        self.db.delete(key.as_bytes())?;
        Ok(())
    }
}
