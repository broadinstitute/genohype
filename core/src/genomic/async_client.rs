use crate::genomic::{LocusTable, VariantAssociation};
use crate::error::Result;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe async client for querying multiple Hail Tables
pub struct HailClient {
    tables: RwLock<LruCache<String, Arc<LocusTable>>>,
}

impl HailClient {
    pub fn new(cache_capacity: usize) -> Self {
        Self {
            tables: RwLock::new(LruCache::new(NonZeroUsize::new(cache_capacity).unwrap())),
        }
    }

    async fn get_or_open(&self, path: &str) -> Result<Arc<LocusTable>> {
        {
            let mut cache = self.tables.write().await;
            if let Some(table) = cache.get(path) {
                return Ok(Arc::clone(table));
            }
        }

        let path_owned = path.to_string();
        let table =
            tokio::task::spawn_blocking(move || LocusTable::open(&path_owned)).await.unwrap()?;

        let table = Arc::new(table);

        {
            let mut cache = self.tables.write().await;
            cache.put(path.to_string(), Arc::clone(&table));
        }

        Ok(table)
    }

    pub async fn query_interval_typed(
        &self,
        table_path: &str,
        contig: &str,
        start: i32,
        end: i32,
    ) -> Result<Vec<VariantAssociation>> {
        let table = self.get_or_open(table_path).await?;
        let contig_owned = contig.to_string();

        let rows = tokio::task::spawn_blocking(move || table.query_interval(&contig_owned, start, end))
            .await
            .unwrap()?;

        Ok(rows.iter().filter_map(VariantAssociation::from_encoded).collect())
    }

    pub async fn query_variant_typed(
        &self,
        table_path: &str,
        contig: &str,
        position: i32,
        ref_allele: Option<&str>,
        alt_allele: Option<&str>,
    ) -> Result<Vec<VariantAssociation>> {
        let table = self.get_or_open(table_path).await?;
        let contig_owned = contig.to_string();
        let ref_owned = ref_allele.map(String::from);
        let alt_owned = alt_allele.map(String::from);

        let rows = tokio::task::spawn_blocking(move || {
            table.query_variant(&contig_owned, position, ref_owned.as_deref(), alt_owned.as_deref())
        })
        .await
        .unwrap()?;

        Ok(rows.iter().filter_map(VariantAssociation::from_encoded).collect())
    }
}
