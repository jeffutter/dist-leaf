use std::{fmt::Debug, sync::Arc};

use rocksdb::TransactionDB;
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
pub struct DBValue {
    pub ts: uhlc::Timestamp,
    pub data: String,
    pub digest: u64,
}

impl DBValue {
    pub fn new(data: &str, ts: uhlc::Timestamp) -> Self {
        let mut hasher = Xxh3::new();
        hasher.update(&ts.get_id().to_le_bytes());
        hasher.update(&ts.get_time().as_u64().to_le_bytes());
        hasher.update(data.as_bytes());
        Self {
            ts,
            data: data.to_string(),
            digest: hasher.digest(),
        }
    }
}

#[derive(Clone)]
pub struct Database {
    db: Arc<TransactionDB>,
    path: std::path::PathBuf,
}

impl Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.path)
            .finish()
    }
}

impl Database {
    pub fn new(path: &std::path::Path) -> Self {
        let db = TransactionDB::open_default(path).unwrap();

        Self {
            db: Arc::new(db),
            path: path.to_path_buf(),
        }
    }

    pub fn new_tmp() -> Self {
        let tmp_dir = TempDir::new().unwrap();

        Self::new(tmp_dir.path())
    }

    #[instrument]
    pub fn put(&self, key: &str, value: &DBValue) -> Result<(), DatabaseError> {
        let key = key.as_bytes();
        let tx = self.db.transaction();
        let old_val = tx.get_for_update(key, true)?;

        if let Some(encoded) = old_val {
            let decoded: DBValue = bincode::deserialize(&encoded).unwrap();

            if value.ts < decoded.ts {
                tx.rollback()?;
                return Ok(());
            }
        }

        let encoded: Vec<u8> = bincode::serialize(value).unwrap();
        tx.put(key, encoded)?;
        tx.commit()?;
        Ok(())
    }

    #[instrument]
    pub fn get(&self, key: &str) -> Result<Option<DBValue>, DatabaseError> {
        match self.db.get(key.as_bytes())? {
            Some(v) => {
                let decoded: DBValue = bincode::deserialize(&v).unwrap();
                Ok(Some(decoded))
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
    pub fn delete(&self, key: &str, ts: uhlc::Timestamp) -> Result<(), DatabaseError> {
        let key = key.as_bytes();
        let tx = self.db.transaction();
        let old_val = tx.get_for_update(key, true)?;
        match old_val {
            None => {
                tx.rollback()?;
                Ok(())
            }
            Some(encoded) => {
                let decoded: DBValue = bincode::deserialize(&encoded).unwrap();
                match decoded {
                    DBValue { ts: old_ts, .. } if old_ts >= ts => {
                        tx.rollback()?;
                        Ok(())
                    }
                    _ => {
                        tx.delete(key)?;
                        tx.commit()?;
                        Ok(())
                    }
                }
            }
        }
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

    #[test]
    fn writing_new_ts_updates() {
        let db = Database::new_tmp();
        let hlc = uhlc::HLC::default();

        let old_ts = hlc.new_timestamp();
        let new_ts = hlc.new_timestamp();

        let key = "key";

        let old_value = "old";
        let new_value = "new";

        let old_db_value = DBValue::new(old_value, old_ts);
        let new_db_value = DBValue::new(new_value, new_ts);

        db.put(key, &old_db_value).unwrap();
        db.put(key, &new_db_value).unwrap();

        let res = db.get(key).unwrap().unwrap();

        assert_eq!(new_ts, res.ts);
        assert_eq!(new_value, res.data);
    }

    #[test]
    fn writing_old_ts_doesnt_update() {
        let db = Database::new_tmp();
        let hlc = uhlc::HLC::default();

        let old_ts = hlc.new_timestamp();
        let new_ts = hlc.new_timestamp();

        let key = "key";

        let old_value = "old";
        let new_value = "new";

        let old_db_value = DBValue::new(old_value, old_ts);
        let new_db_value = DBValue::new(new_value, new_ts);

        db.put(key, &new_db_value).unwrap();
        db.put(key, &old_db_value).unwrap();

        let res = db.get(key).unwrap().unwrap();

        assert_eq!(new_ts, res.ts);
        assert_eq!(new_value, res.data);
    }

    #[test]
    fn deleting_merge() {
        let db = Database::new_tmp();
        let hlc = uhlc::HLC::default();

        let old_ts = hlc.new_timestamp();
        let delete_ts = hlc.new_timestamp();
        let new_ts = hlc.new_timestamp();

        let key = "key";

        let old_value = "old";
        let new_value = "new";

        let old_db_value = DBValue::new(old_value, old_ts);
        let new_db_value = DBValue::new(new_value, new_ts);

        db.put(key, &old_db_value).unwrap();
        db.delete(key, delete_ts).unwrap();
        db.put(key, &new_db_value).unwrap();

        let res = db.get(key).unwrap().unwrap();

        assert_eq!(new_ts, res.ts);
        assert_eq!(new_value, res.data);
    }

    #[test]
    fn delete_with_old_timestamp_doesnt_delete() {
        let db = Database::new_tmp();
        let hlc = uhlc::HLC::default();

        let old_ts = hlc.new_timestamp();
        let delete_ts = hlc.new_timestamp();
        let new_ts = hlc.new_timestamp();

        let key = "key";

        let old_value = "old";
        let new_value = "new";

        let old_db_value = DBValue::new(old_value, old_ts);
        let new_db_value = DBValue::new(new_value, new_ts);

        db.put(key, &old_db_value).unwrap();
        db.put(key, &new_db_value).unwrap();
        db.delete(key, delete_ts).unwrap();

        let res = db.get(key).unwrap().unwrap();

        assert_eq!(new_ts, res.ts);
        assert_eq!(new_value, res.data);
    }
}
