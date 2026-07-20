//! Sorted merge join iterator for combining two tables.
//!
//! This module provides an iterator that performs an efficient O(n) merge join
//! between two sorted iterators of EncodedValues, enabling annotation lookups
//! without random access.

use crate::codec::EncodedValue;
use crate::query::compare::compare_rows;
use crate::Result;
use std::cmp::Ordering;
use std::iter::Peekable;

/// A row resulting from a merge join.
#[derive(Debug)]
pub struct JoinedRow {
    /// The row from the primary table (e.g., variant results).
    pub left: EncodedValue,
    /// The row from the secondary table (e.g., annotations), if found.
    pub right: Option<EncodedValue>,
}

/// An iterator that performs a sorted merge join between two iterators of EncodedValues.
///
/// Assumes both inputs are sorted by `key_fields`. This performs a left join:
/// all rows from the primary iterator are yielded, with matching rows from
/// secondary attached when available.
pub struct SortedMergeIterator<I, J>
where
    I: Iterator<Item = Result<EncodedValue>>,
    J: Iterator<Item = Result<EncodedValue>>,
{
    primary: Peekable<I>,
    secondary: Peekable<J>,
    key_fields: Vec<String>,
}

impl<I, J> SortedMergeIterator<I, J>
where
    I: Iterator<Item = Result<EncodedValue>>,
    J: Iterator<Item = Result<EncodedValue>>,
{
    /// Create a new sorted merge iterator.
    ///
    /// # Arguments
    /// * `primary` - The primary (left) iterator
    /// * `secondary` - The secondary (right) iterator
    /// * `key_fields` - Field names to use for comparison
    pub fn new(primary: I, secondary: J, key_fields: Vec<String>) -> Self {
        Self {
            primary: primary.peekable(),
            secondary: secondary.peekable(),
            key_fields,
        }
    }
}

impl<I, J> Iterator for SortedMergeIterator<I, J>
where
    I: Iterator<Item = Result<EncodedValue>>,
    J: Iterator<Item = Result<EncodedValue>>,
{
    type Item = Result<JoinedRow>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if primary is exhausted
            let left_res = match self.primary.peek() {
                Some(res) => res,
                None => return None, // Primary exhausted, stop (left join behavior)
            };

            // Propagate errors from primary
            if let Err(_) = left_res {
                return self.primary.next().map(|r| {
                    r.map(|_| unreachable!())
                        .map_err(|e| e)
                        .map(|_: EncodedValue| unreachable!())
                });
            }
            let left_row = left_res.as_ref().unwrap();

            // Check secondary
            let right_res = self.secondary.peek();

            if let Some(res) = right_res {
                if let Err(_) = res {
                    // Propagate errors from secondary
                    return self.secondary.next().map(|r| {
                        r.map(|_| unreachable!())
                            .map_err(|e| e)
                            .map(|_: EncodedValue| unreachable!())
                    });
                }
                let right_row = res.as_ref().unwrap();

                // Compare keys
                match compare_rows(left_row, right_row, &self.key_fields) {
                    Ordering::Equal => {
                        // Match found
                        let left = self.primary.next().unwrap().unwrap();
                        let right = self.secondary.next().unwrap().unwrap();
                        return Some(Ok(JoinedRow {
                            left,
                            right: Some(right),
                        }));
                    }
                    Ordering::Less => {
                        // Left is behind Right -> yield Left (no match)
                        let left = self.primary.next().unwrap().unwrap();
                        return Some(Ok(JoinedRow { left, right: None }));
                    }
                    Ordering::Greater => {
                        // Right is behind Left -> skip Right
                        self.secondary.next();
                        continue;
                    }
                }
            } else {
                // Secondary exhausted -> yield remaining Lefts
                let left = self.primary.next().unwrap().unwrap();
                return Some(Ok(JoinedRow { left, right: None }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_row(chrom: &str, pos: i32) -> EncodedValue {
        EncodedValue::Struct(vec![
            (
                "chrom".to_string(),
                EncodedValue::Binary(chrom.as_bytes().to_vec()),
            ),
            ("pos".to_string(), EncodedValue::Int32(pos)),
        ])
    }

    #[test]
    fn test_merge_join_basic() {
        let left: Vec<Result<EncodedValue>> = vec![
            Ok(make_row("chr1", 100)),
            Ok(make_row("chr1", 200)),
            Ok(make_row("chr1", 300)),
        ];

        let right: Vec<Result<EncodedValue>> =
            vec![Ok(make_row("chr1", 100)), Ok(make_row("chr1", 300))];

        let keys = vec!["chrom".to_string(), "pos".to_string()];
        let iter = SortedMergeIterator::new(left.into_iter(), right.into_iter(), keys);

        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 3);

        // First row matches
        assert!(results[0].as_ref().unwrap().right.is_some());
        // Second row no match
        assert!(results[1].as_ref().unwrap().right.is_none());
        // Third row matches
        assert!(results[2].as_ref().unwrap().right.is_some());
    }

    #[test]
    fn test_merge_join_empty_right() {
        let left: Vec<Result<EncodedValue>> =
            vec![Ok(make_row("chr1", 100)), Ok(make_row("chr1", 200))];

        let right: Vec<Result<EncodedValue>> = vec![];

        let keys = vec!["chrom".to_string(), "pos".to_string()];
        let iter = SortedMergeIterator::new(left.into_iter(), right.into_iter(), keys);

        let results: Vec<_> = iter.collect();
        assert_eq!(results.len(), 2);
        assert!(results[0].as_ref().unwrap().right.is_none());
        assert!(results[1].as_ref().unwrap().right.is_none());
    }
}
