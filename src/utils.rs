pub mod RoundRobin {
    use std::collections::{HashMap, VecDeque};

    use linked_hash_set::LinkedHashSet;
    use uuid::Uuid;

    use crate::upstream::UpstreamPeerSinkChannelTx;

    pub struct RoundRobinContext<T> {
        available_candidat: Vec<T>,
        available_cursor: usize,
    }

    impl<T: Ord + std::fmt::Debug> RoundRobinContext<T> {
        pub fn new() -> Self {
            return Self {
                available_candidat: vec![],
                available_cursor: 0,
            };
        }

        pub fn add(&mut self, candidat: T) {
            trace!("Add round robin candidat [{:?}]", candidat);
            self.available_candidat.push(candidat);
        }

        pub fn delete(&mut self, candidat: &T) -> Option<T> {
            trace!("Delete round robin candidat [{:?}]", candidat);
            let mut removed = Option::None;

            if let Ok(candidat_index) = self.available_candidat.binary_search(candidat) {
                trace!(
                    "Found the candidat [{:?}] for deletion at index [{:?}]",
                    candidat,
                    candidat_index
                );

                removed = Option::Some(self.available_candidat.remove(candidat_index));

                if self.available_cursor > 0 {
                    self.available_cursor -= 1;
                }
            } else {
                error!("Failed to delete the candidat [{:?}]", candidat);
            }

            return removed;
        }

        pub fn next(&mut self) -> Option<&T> {
            trace!("Next round robin candidat");
            let mut candidat = Option::None;

            if !self.available_candidat.is_empty() {
                let index = self.available_cursor % self.available_candidat.len();

                candidat = Some(&self.available_candidat[index]);
                self.available_cursor += 1;
            }

            return candidat;
        }
    }

    // /// Only handle upstream sink tx for the moment
    // pub struct RoundRobinContext<T> {
    //     since_added_tx: VecDeque<T>,
    //     since_deleted_tx: VecDeque<T>,
    //     current_txs: LinkedHashSet<T>,
    // }

    // /// Round robin for Upstream peers (sink channel)
    // pub struct RoundRobinContextIterator<T> {
    //     curr_cursor: usize,
    //     inner: Vec<T>,
    // }

    // impl<T> RoundRobinContext<T> {
    //     pub fn new() -> Self {
    //         return Self {
    //             since_added_tx: VecDeque::default(),
    //             since_deleted_tx: VecDeque::default(),
    //             current_txs: LinkedHashSet::new(),
    //         };
    //     }

    //     fn update<'b>(&mut self, from: &'b HashMap<T, UpstreamPeerSinkChannelTx>) {
    //         trace!("Updating upstream peers sink tx (round robin)");
    //         //consume since_added
    //         //consume since_deleted
    //         //return the new iterator (as an option)
    //         // self.inner = from.keys().cloned().collect();
    //     }

    //     pub fn new_candidat(candidat: T) {}

    //     pub fn delete_candidat(candidat: T) {}

    //     pub fn iter() -> RoundRobinContextIterator<T> {
    //         return RoundRobinContextIterator::new();
    //     }
    // }

    // impl<T> RoundRobinContextIterator<T> {
    //     pub fn new() -> Self {
    //         trace!("Creating Round Robin for upstreams peer sink tx");
    //         Self {
    //             curr_cursor: 0,
    //             inner: vec![],
    //         }
    //     }
    // }

    // impl<T> Iterator for RoundRobinContextIterator<T> {
    //     type Item = T;

    //     fn next(&mut self) -> Option<Self::Item> {
    //         if !self.inner.is_empty() {
    //             return Some(self.inner.get(self.inner.len() % self.curr_cursor)?.clone());
    //         }

    //         return None;
    //     }
    // }

    // /// Task with notifying job to the given channel
    // /// Will notify every X ms about the new round robin context iterator
    // pub fn create_task(tokio::) {

    // }
}
