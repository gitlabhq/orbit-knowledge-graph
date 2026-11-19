use std::time::Duration;

use indexing_engine_next::protocol::{Fence, Revision, TaskId};
use indexing_engine_next::testkit::simulator::{
    CopyState, RevisionConflict, SimulatedClock, SimulatedKeyValue, SimulatedStore,
    SimulatedStream, StoreWrite,
};
use uuid::Uuid;

fn task(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

#[test]
fn a_key_value_write_with_a_stale_revision_is_a_conflict() {
    let mut key_value = SimulatedKeyValue::default();
    let first = key_value.put(Revision(0), Some(task(1))).unwrap();

    assert_eq!(
        key_value.put(Revision(0), Some(task(2))),
        Err(RevisionConflict { current: first })
    );
    assert_eq!(key_value.get(), Some(&task(1)));
    assert_eq!(key_value.put(first, None), Ok(Revision(2)));
    assert_eq!(key_value.get(), None);
}

#[test]
fn a_publish_of_a_task_with_a_live_copy_adds_no_copy() {
    let mut stream = SimulatedStream::new(2);
    stream.publish(task(1));
    stream.publish(task(1));
    assert_eq!(stream.copies().len(), 1);

    stream.acknowledge(0);
    stream.publish(task(1));
    assert_eq!(stream.copies().len(), 2);
}

#[test]
fn a_copy_comes_back_after_the_acknowledge_wait_unless_its_consumer_acks_progress() {
    let mut stream = SimulatedStream::new(2);
    stream.publish(task(1));
    stream.deliver(0, 0, 10);

    assert_eq!(stream.expired_copies(11).count(), 0);
    stream.acknowledge_progress(0, 1, 11);
    assert_eq!(stream.expired_copies(12).collect::<Vec<_>>(), [0]);

    stream.acknowledge_progress(0, 0, 11);
    assert_eq!(stream.expired_copies(12).count(), 0);

    stream.redeliver(0);
    assert_eq!(stream.copies()[0].state, CopyState::Ready);
    assert_eq!(stream.copies()[0].consumer, None);
}

#[test]
fn the_store_rejects_a_write_with_a_lower_fence_than_one_it_kept() {
    let mut store = SimulatedStore::default();

    assert_eq!(store.write(Fence(4)), StoreWrite::Kept);
    assert_eq!(store.write(Fence(3)), StoreWrite::Rejected);
    assert_eq!(store.write(Fence(4)), StoreWrite::Kept);
    assert_eq!(store.highest_fence(), Fence(4));
}

#[test]
fn a_skewed_holder_reads_a_later_time_from_the_same_clock() {
    let mut clock = SimulatedClock::new(Duration::from_secs(20));
    clock.advance();

    assert_eq!(clock.ticks_at(clock.local_now(0)), 1);
    assert_eq!(clock.ticks_at(clock.local_now(1)), 2);
}
