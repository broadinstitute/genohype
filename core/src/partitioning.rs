//! Partition allocation for distributed processing.
//!
//! This module provides logic for distributing work items (partitions or shards)
//! across multiple workers in a distributed environment.
//!
//! ## Example
//!
//! ```rust
//! use genohype_core::partitioning::PartitionAllocator;
//!
//! // Worker 0 of 4 total workers, processing 100 total shards
//! let allocator = PartitionAllocator::new(0, 4);
//! let my_shards = allocator.get_owned_indices(100);
//! // Worker 0 gets: [0, 4, 8, 12, ..., 96]
//! assert_eq!(my_shards.len(), 25);
//! ```

/// Logic for distributing work items across multiple workers.
///
/// Uses modulo arithmetic to evenly distribute indices across workers,
/// ensuring each worker gets a non-overlapping subset of the work.
#[derive(Debug, Clone, Copy)]
pub struct PartitionAllocator {
    /// The ID of this worker (0-based)
    pub worker_id: usize,
    /// Total number of workers in the pool
    pub total_workers: usize,
}

impl PartitionAllocator {
    /// Create a new partition allocator.
    ///
    /// # Arguments
    ///
    /// * `worker_id` - The ID of this worker (0-based)
    /// * `total_workers` - Total number of workers in the pool
    ///
    /// # Panics
    ///
    /// Panics if `total_workers` is 0 or if `worker_id >= total_workers`.
    pub fn new(worker_id: usize, total_workers: usize) -> Self {
        assert!(total_workers > 0, "Total workers must be > 0");
        assert!(
            worker_id < total_workers,
            "Worker ID ({}) must be < total workers ({})",
            worker_id,
            total_workers
        );
        Self {
            worker_id,
            total_workers,
        }
    }

    /// Returns the indices of items this worker is responsible for.
    ///
    /// Uses modulo arithmetic to distribute items evenly: item `i` is owned
    /// by worker `i % total_workers`.
    ///
    /// # Arguments
    ///
    /// * `total_items` - Total number of items to distribute
    ///
    /// # Returns
    ///
    /// A vector of indices that this worker should process.
    pub fn get_owned_indices(&self, total_items: usize) -> Vec<usize> {
        (0..total_items)
            .filter(|i| i % self.total_workers == self.worker_id)
            .collect()
    }

    /// Returns true if this worker owns the specific item index.
    ///
    /// # Arguments
    ///
    /// * `index` - The item index to check
    pub fn owns(&self, index: usize) -> bool {
        index % self.total_workers == self.worker_id
    }

    /// Returns the number of items this worker will own out of `total_items`.
    ///
    /// This is useful for pre-allocating buffers or progress tracking.
    pub fn count_owned(&self, total_items: usize) -> usize {
        // Items are distributed as: worker 0 gets 0, n, 2n, ...
        // Worker i gets items where item % n == i
        // Count = ceil((total - worker_id) / total_workers) for worker_id < total
        if total_items == 0 {
            return 0;
        }
        // Number of complete "rounds" of distribution
        let full_rounds = total_items / self.total_workers;
        // Remaining items after full rounds
        let remainder = total_items % self.total_workers;
        // This worker gets one item per full round, plus one more if worker_id < remainder
        full_rounds + if self.worker_id < remainder { 1 } else { 0 }
    }
}

impl Default for PartitionAllocator {
    /// Default allocator: single worker (worker 0 of 1), processes everything.
    fn default() -> Self {
        Self {
            worker_id: 0,
            total_workers: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_worker_owns_all() {
        let alloc = PartitionAllocator::new(0, 1);
        let indices = alloc.get_owned_indices(10);
        assert_eq!(indices, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(alloc.count_owned(10), 10);
    }

    #[test]
    fn test_two_workers_split_evenly() {
        let alloc0 = PartitionAllocator::new(0, 2);
        let alloc1 = PartitionAllocator::new(1, 2);

        let indices0 = alloc0.get_owned_indices(10);
        let indices1 = alloc1.get_owned_indices(10);

        assert_eq!(indices0, vec![0, 2, 4, 6, 8]);
        assert_eq!(indices1, vec![1, 3, 5, 7, 9]);

        assert_eq!(alloc0.count_owned(10), 5);
        assert_eq!(alloc1.count_owned(10), 5);
    }

    #[test]
    fn test_three_workers_uneven() {
        // 10 items, 3 workers: worker 0 gets 4, workers 1 and 2 get 3 each
        let alloc0 = PartitionAllocator::new(0, 3);
        let alloc1 = PartitionAllocator::new(1, 3);
        let alloc2 = PartitionAllocator::new(2, 3);

        let indices0 = alloc0.get_owned_indices(10);
        let indices1 = alloc1.get_owned_indices(10);
        let indices2 = alloc2.get_owned_indices(10);

        assert_eq!(indices0, vec![0, 3, 6, 9]);
        assert_eq!(indices1, vec![1, 4, 7]);
        assert_eq!(indices2, vec![2, 5, 8]);

        assert_eq!(alloc0.count_owned(10), 4);
        assert_eq!(alloc1.count_owned(10), 3);
        assert_eq!(alloc2.count_owned(10), 3);

        // Verify all items are covered exactly once
        let mut all: Vec<usize> = indices0
            .into_iter()
            .chain(indices1)
            .chain(indices2)
            .collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_owns() {
        let alloc = PartitionAllocator::new(1, 3);
        assert!(!alloc.owns(0)); // 0 % 3 == 0
        assert!(alloc.owns(1)); // 1 % 3 == 1
        assert!(!alloc.owns(2)); // 2 % 3 == 2
        assert!(!alloc.owns(3)); // 3 % 3 == 0
        assert!(alloc.owns(4)); // 4 % 3 == 1
    }

    #[test]
    fn test_empty_items() {
        let alloc = PartitionAllocator::new(0, 4);
        let indices = alloc.get_owned_indices(0);
        assert!(indices.is_empty());
        assert_eq!(alloc.count_owned(0), 0);
    }

    #[test]
    fn test_more_workers_than_items() {
        // 3 items, 10 workers: workers 0, 1, 2 each get 1 item
        let alloc0 = PartitionAllocator::new(0, 10);
        let alloc1 = PartitionAllocator::new(1, 10);
        let alloc2 = PartitionAllocator::new(2, 10);
        let alloc9 = PartitionAllocator::new(9, 10);

        assert_eq!(alloc0.get_owned_indices(3), vec![0]);
        assert_eq!(alloc1.get_owned_indices(3), vec![1]);
        assert_eq!(alloc2.get_owned_indices(3), vec![2]);
        assert!(alloc9.get_owned_indices(3).is_empty());

        assert_eq!(alloc0.count_owned(3), 1);
        assert_eq!(alloc1.count_owned(3), 1);
        assert_eq!(alloc2.count_owned(3), 1);
        assert_eq!(alloc9.count_owned(3), 0);
    }

    #[test]
    fn test_default_is_single_worker() {
        let alloc = PartitionAllocator::default();
        assert_eq!(alloc.worker_id, 0);
        assert_eq!(alloc.total_workers, 1);
        assert_eq!(alloc.get_owned_indices(100).len(), 100);
    }

    #[test]
    #[should_panic(expected = "Total workers must be > 0")]
    fn test_zero_workers_panics() {
        PartitionAllocator::new(0, 0);
    }

    #[test]
    #[should_panic(expected = "Worker ID (5) must be < total workers (3)")]
    fn test_invalid_worker_id_panics() {
        PartitionAllocator::new(5, 3);
    }
}
