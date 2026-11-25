use crate::lease::{Generation, Lease, LeaseEntry, Revision};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RevisionConflict {
    pub current: Revision,
}

#[derive(Clone, Debug)]
pub struct SimulatedKeyValue<V> {
    value: Option<V>,
    revision: Revision,
}

impl<V> Default for SimulatedKeyValue<V> {
    fn default() -> Self {
        Self {
            value: None,
            revision: Revision(0),
        }
    }
}

impl<V> SimulatedKeyValue<V> {
    pub fn get(&self) -> Option<&V> {
        self.value.as_ref()
    }

    pub fn revision(&self) -> Revision {
        self.revision
    }

    pub fn put(
        &mut self,
        expected: Revision,
        value: Option<V>,
    ) -> Result<Revision, RevisionConflict> {
        if expected != self.revision {
            return Err(RevisionConflict {
                current: self.revision,
            });
        }
        self.revision = Revision(self.revision.0 + 1);
        self.value = value;
        Ok(self.revision)
    }

    pub fn wipe(&mut self) {
        self.value = None;
        self.revision = Revision(0);
    }
}

impl SimulatedKeyValue<Lease> {
    pub fn entry(&self, generation: Option<Generation>) -> LeaseEntry {
        LeaseEntry {
            lease: self.value.clone(),
            revision: self.revision,
            generation,
        }
    }
}
