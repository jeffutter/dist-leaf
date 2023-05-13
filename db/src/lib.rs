use std::{borrow::Cow, sync::Arc};

use rocksdb::DB;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use thiserror::Error;
use tracing::instrument;
use xxhash_rust::xxh3::Xxh3;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("database error")]
    RocksDB(#[from] rocksdb::Error),
    #[error("unknown database error")]
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DBValue<'data> {
    pub ts: uhlc::Timestamp,
    pub data: Cow<'data, str>,
    pub digest: u64,
}

impl<'a> DBValue<'a> {
    pub fn new(data: &'a str, ts: uhlc::Timestamp) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(ts.get_id().as_slice());
        hasher.update(&ts.get_time().as_u64().to_le_bytes());
        hasher.update(data.as_bytes());
        Self {
            ts,
            data: Cow::Borrowed(data),
            digest: hasher.digest(),
        }
    }
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
    pub fn put(&self, key: &str, value: &DBValue) -> Result<(), DatabaseError> {
        let encoded: Vec<u8> = bincode::serialize(&value).unwrap();
        self.db.put(key.as_bytes(), encoded)?;

        Ok(())
    }

    #[instrument]
    pub fn get(&self, key: &str) -> Result<Option<DBValue>, DatabaseError> {
        match self.db.get(key.as_bytes())? {
            Some(v) => {
                let decoded: DBValue = bincode::deserialize(&v).unwrap();
                Ok(Some(decoded.clone()))
            }
            None => Ok(None),
        }
    }

    #[instrument]
    pub fn list(
        &self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Result<(String, DBValue), DatabaseError>> + '_> {
        let res = self.db.prefix_iterator(prefix).map(|e| {
            e.map(|(k, v)| {
                let decoded: DBValue = bincode::deserialize(&v).unwrap();
                let key = String::from_utf8(k.to_vec()).unwrap();
                (key, decoded)
            })
            .map_err(|e| e.into())
        });

        Box::new(res)
    }

    #[instrument]
    pub fn delete(&self, key: &str) -> Result<(), DatabaseError> {
        self.db.delete(key.as_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn round_trip() {
        let db = Database::new_tmp();
        let hlc = uhlc::HLC::default();
        let key = "key";
        let value = "value";
        let db_value = DBValue::new(value, hlc.new_timestamp());

        db.put(key, &db_value).unwrap();

        let res = db.get(key).unwrap().unwrap();

        assert_eq!(db_value.ts, res.ts);
        assert_eq!(db_value.data, res.data);
    }
}
