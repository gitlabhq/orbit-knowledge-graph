//! Shared observability primitives.
//!
//! `MultiObserver<T>` composes multiple trait objects of the same
//! observer trait into one. The trait-specific forwarding `impl`
//! lives in the consuming crate so this module stays trait-agnostic
//! and dependency-free.

pub struct MultiObserver<T: ?Sized> {
    observers: Vec<Box<T>>,
}

impl<T: ?Sized> MultiObserver<T> {
    pub fn new(observers: Vec<Box<T>>) -> Self {
        Self { observers }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.observers.iter().map(|b| b.as_ref())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> + '_ {
        self.observers.iter_mut().map(|b| b.as_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Counter {
        fn tick(&mut self);
        fn value(&self) -> usize;
    }

    #[derive(Default)]
    struct Impl {
        n: usize,
    }

    impl Counter for Impl {
        fn tick(&mut self) {
            self.n += 1;
        }
        fn value(&self) -> usize {
            self.n
        }
    }

    #[test]
    fn iter_mut_forwards_to_each() {
        let mut m: MultiObserver<dyn Counter> =
            MultiObserver::new(vec![Box::new(Impl::default()), Box::new(Impl::default())]);
        for o in m.iter_mut() {
            o.tick();
            o.tick();
        }
        let values: Vec<usize> = m.iter().map(|o| o.value()).collect();
        assert_eq!(values, vec![2, 2]);
    }

    #[test]
    fn empty_new_is_valid() {
        let mut m: MultiObserver<dyn Counter> = MultiObserver::new(vec![]);
        assert_eq!(m.iter_mut().count(), 0);
        assert_eq!(m.iter().count(), 0);
    }
}
