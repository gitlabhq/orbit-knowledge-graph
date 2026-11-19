use std::time::Duration;

use indexing_engine_next::protocol::{Fence, Revision, TaskId};
use indexing_engine_next::testkit::simulator::{
    MessageState, RevisionConflict, SimulatedBroker, SimulatedClock, SimulatedKeyValue,
    SimulatedStore, StoreWrite,
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
fn a_publish_of_a_task_with_a_live_message_adds_no_message() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.publish(task(1));
    assert_eq!(broker.messages().len(), 1);

    broker.acknowledge(0);
    broker.publish(task(1));
    assert_eq!(broker.messages().len(), 2);
}

#[test]
fn a_message_comes_back_after_the_acknowledge_wait_unless_its_consumer_acknowledges_progress() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.deliver(0, 0, 10);

    assert_eq!(broker.expired_messages(11).count(), 0);
    broker.acknowledge_progress(0, 1, 11);
    assert_eq!(broker.expired_messages(12).collect::<Vec<_>>(), [0]);

    broker.acknowledge_progress(0, 0, 11);
    assert_eq!(broker.expired_messages(12).count(), 0);

    broker.redeliver(0);
    assert_eq!(broker.messages()[0].state, MessageState::Ready);
    assert_eq!(broker.messages()[0].consumer, None);
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
