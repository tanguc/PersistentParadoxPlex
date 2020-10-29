/// Round robin implementation
/// NOTE for the sake of simplicity this implementation
/// is backed with a vec and a cursor, T has to be Ord
/// in order to keep sorting always present and keep O(log(n))
/// complexity for CRUD operations
/// There could be some undefined behaviour about the ordering
/// when using `next` function, so please make sure to be aware.
pub mod round_robin {
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

        /// NOTE: before to add, we search for an index with binary search
        /// make sure to keep the sorting in the vec and insert it at this index
        /// so at the added index, all items at the left are shitfted
        pub fn add(&mut self, candidat: T) {
            trace!("Add round robin candidat [{:?}]", candidat);

            match self.available_candidat.binary_search(&candidat) {
                Ok(_) => {
                    warn!(
                        "Failed to insert the candidat [{:?}], it already exists",
                        &candidat
                    );
                }
                Err(insert_index) => {
                    trace!(
                        "Inserting candidat [{:?}] at index [{:?}]",
                        &candidat,
                        insert_index
                    );
                    self.available_candidat.insert(insert_index, candidat);
                }
            }
        }

        pub fn delete(&mut self, candidat: &T) -> Option<T> {
            debug!("Delete round robin candidat [{:?}]", candidat);
            trace!(
                "Before deletion = {:?} && cursor [{:?}]",
                self.available_candidat,
                self.available_cursor
            );

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
                error!("Failed to find the candidat [{:?}] for deletion", candidat);
            }

            return removed;
        }

        /// Retrieve the next candidat which is determined by the round robin
        /// algorithm
        /// NOTE: due to technical reasons and to avoid too much complexity
        /// there is a special use case when the a new candidat will be added to the  
        /// current cursor, the next one will be the new one and not old first (shifted one)
        pub fn next(&mut self) -> Option<&T> {
            let mut candidat = Option::None;

            if !self.available_candidat.is_empty() {
                let index = self.available_cursor % self.available_candidat.len();

                candidat = Some(&self.available_candidat[index]);
                trace!("Next round robin candidat [{:?}]", &candidat);

                self.available_cursor += 1;
            }

            return candidat;
        }
    }

    #[cfg(test)]
    mod tests {

        use super::*;
        use test_env_log::test;
        use uuid::Uuid;

        #[test]
        fn candidat_is_added() {
            let mut rr: RoundRobinContext<Uuid> = RoundRobinContext::new();

            let candidat_one = Uuid::new_v4();
            let candidat_two = Uuid::new_v4();
            let candidat_three = Uuid::new_v4();
            info!("candidat one = [{:?}]", &candidat_one);
            info!("candidat two = [{:?}]", &candidat_two);
            info!("candidat three = [{:?}]", &candidat_three);

            rr.add(candidat_one.clone());
            rr.add(candidat_two.clone());
            rr.add(candidat_three.clone());

            let mut candidat_added = vec![];
            for _ in 1..4 {
                candidat_added.push(rr.next().unwrap().clone());
            }
            info!("Candidat added {:?}", &candidat_added);

            assert!(candidat_added
                .iter()
                .find(|&c| c == &candidat_one)
                .is_some());
            assert!(candidat_added
                .iter()
                .find(|&c| c == &candidat_two)
                .is_some());
            assert!(candidat_added
                .iter()
                .find(|&c| c == &candidat_three)
                .is_some());
        }

        #[test]
        fn candidat_is_deleted() {
            let mut rr: RoundRobinContext<Uuid> = RoundRobinContext::new();

            let candidat_one = Uuid::new_v4();
            let candidat_two = Uuid::new_v4();
            let candidat_three = Uuid::new_v4();

            let candidat_to_delete = &candidat_two;

            rr.add(candidat_one.clone());
            rr.add(candidat_two.clone());
            rr.add(candidat_three.clone());

            let deleted = rr.delete(candidat_to_delete);
            trace!("Deleted candidat [{:?}]", &deleted);
            assert!(&deleted.unwrap() == candidat_to_delete);

            let mut candidat_remaining = vec![];
            for _ in 1..3 {
                candidat_remaining.push(rr.next().unwrap().clone());
            }
            info!("Candidat remaining {:?}", &candidat_remaining);

            assert!(candidat_remaining
                .iter()
                .find(|&c| c == &candidat_one)
                .is_some());
            assert!(candidat_remaining
                .iter()
                .find(|&c| c == &candidat_three)
                .is_some());
        }

        #[test]
        fn candidats_next() {
            let mut rr = RoundRobinContext::new();

            let candidat_one = Uuid::new_v4();
            let candidat_two = Uuid::new_v4();
            let candidat_three = Uuid::new_v4();

            rr.add(candidat_one.clone());
            rr.add(candidat_two.clone());
            rr.add(candidat_three.clone());

            let mut candidats_added = vec![];
            for _ in 1..4 {
                candidats_added.push(rr.next().unwrap().clone());
            }

            assert!(candidats_added
                .iter()
                .find(|&c| c == &candidat_one)
                .is_some());
            assert!(candidats_added
                .iter()
                .find(|&c| c == &candidat_two)
                .is_some());

            assert!(candidats_added
                .iter()
                .find(|&c| c == &candidat_three)
                .is_some());
        }
    }
}
