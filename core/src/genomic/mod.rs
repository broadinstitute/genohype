pub mod async_client;
pub mod extract;

pub use async_client::*;
pub use extract::*;

use crate::codec::EncodedValue;
use crate::query::{IntervalList, KeyRange, KeyValue, QueryBound, QueryEngine};
use crate::error::Result;
use std::sync::Arc;

/// A client for querying locus-keyed Hail Tables
pub struct LocusTable {
    engine: QueryEngine,
    contig_path: Vec<String>,
    position_path: Vec<String>,
}

impl LocusTable {
    pub fn open(path: &str) -> Result<Self> {
        let engine = QueryEngine::open_path(path)?;
        Ok(Self {
            engine,
            contig_path: vec!["locus".to_string(), "contig".to_string()],
            position_path: vec!["locus".to_string(), "position".to_string()],
        })
    }

    pub fn query_interval(
        &self,
        contig: &str,
        start: i32,
        end: i32,
    ) -> Result<Vec<EncodedValue>> {
        let ranges = vec![
            KeyRange::point_nested(
                self.contig_path.clone(),
                KeyValue::String(contig.to_string()),
            ),
            KeyRange {
                field_path: self.position_path.clone(),
                start: QueryBound::Included(KeyValue::Int32(start)),
                end: QueryBound::Included(KeyValue::Int32(end)),
            },
        ];

        self.engine
            .query_iter(&ranges)?
            .collect::<Result<Vec<_>>>()
    }

    pub fn query_variant(
        &self,
        contig: &str,
        position: i32,
        ref_allele: Option<&str>,
        alt_allele: Option<&str>,
    ) -> Result<Vec<EncodedValue>> {
        let ranges = vec![
            KeyRange::point_nested(
                self.contig_path.clone(),
                KeyValue::String(contig.to_string()),
            ),
            KeyRange::point_nested(self.position_path.clone(), KeyValue::Int32(position)),
        ];

        let results: Vec<EncodedValue> =
            self.engine.query_iter(&ranges)?.collect::<Result<Vec<_>>>()?;

        if ref_allele.is_some() || alt_allele.is_some() {
            Ok(results
                .into_iter()
                .filter(|row| self.matches_alleles(row, ref_allele, alt_allele))
                .collect())
        } else {
            Ok(results)
        }
    }

    pub fn query_intervals(&self, contig: &str, intervals: &[(i32, i32)]) -> Result<Vec<EncodedValue>> {
        let mut interval_list = IntervalList::new();
        for (start, end) in intervals {
            interval_list.add(contig.to_string(), *start, *end);
        }
        interval_list.optimize();

        let min_start = intervals.iter().map(|(s, _)| *s).min().unwrap_or(0);
        let max_end = intervals.iter().map(|(_, e)| *e).max().unwrap_or(i32::MAX);

        let ranges = vec![
            KeyRange::point_nested(
                self.contig_path.clone(),
                KeyValue::String(contig.to_string()),
            ),
            KeyRange {
                field_path: self.position_path.clone(),
                start: QueryBound::Included(KeyValue::Int32(min_start)),
                end: QueryBound::Included(KeyValue::Int32(max_end)),
            },
        ];

        self.engine
            .query_iter_with_intervals(&ranges, Some(Arc::new(interval_list)))?
            .collect::<Result<Vec<_>>>()
    }

    fn matches_alleles(
        &self,
        row: &EncodedValue,
        ref_allele: Option<&str>,
        alt_allele: Option<&str>,
    ) -> bool {
        if let Some(EncodedValue::Array(alleles)) = get_field(row, "alleles") {
            let ref_match = ref_allele.map_or(true, |r| {
                alleles
                    .first()
                    .map_or(false, |a| matches!(a, EncodedValue::Binary(b) if b == r.as_bytes()))
            });
            let alt_match = alt_allele.map_or(true, |a| {
                alleles
                    .get(1)
                    .map_or(false, |al| matches!(al, EncodedValue::Binary(b) if b == a.as_bytes()))
            });
            return ref_match && alt_match;
        }
        false
    }
}
