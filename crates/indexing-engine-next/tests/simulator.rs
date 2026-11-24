use std::time::Duration;

use indexing_engine_next::protocol::{Fence, Generation, Revision, TaskId};
use indexing_engine_next::testkit::simulator::{
    DatabaseWrite, MessageState, RevisionConflict, SimulatedBroker, SimulatedClock,
    SimulatedDatabase, SimulatedKeyValue,
};
use uuid::Uuid;

fn task(n: u128) -> TaskId {
    TaskId::new(Uuid::from_u128(n))
}

fn fence(generation: u64, revision: u64) -> Fence {
    Fence {
        generation: Generation(generation),
        revision: Revision(revision),
    }
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
fn the_database_rejects_a_write_with_a_lower_fence_than_one_it_kept() {
    let mut database = SimulatedDatabase::default();

    assert_eq!(database.write(fence(1, 4)), DatabaseWrite::Kept);
    assert_eq!(database.write(fence(1, 3)), DatabaseWrite::Rejected);
    assert_eq!(database.write(fence(1, 4)), DatabaseWrite::Kept);
    assert_eq!(database.highest_fence(), fence(1, 4));
}

#[test]
fn a_new_generation_survives_in_the_database_and_beats_older_fences() {
    let mut database = SimulatedDatabase::default();
    assert_eq!(database.write(fence(1, 4_000)), DatabaseWrite::Kept);

    let next = database.start_next_generation();

    assert_eq!(next, Generation(2));
    assert_eq!(database.write(fence(next.0, 1)), DatabaseWrite::Kept);
    assert_eq!(database.write(fence(1, 4_001)), DatabaseWrite::Rejected);
}

#[test]
fn a_wiped_key_value_entry_starts_its_revisions_again() {
    let mut key_value = SimulatedKeyValue::default();
    let first = key_value.put(Revision(0), Some(task(1))).unwrap();
    key_value.put(first, Some(task(2))).unwrap();

    key_value.wipe();

    assert_eq!(key_value.get(), None);
    assert_eq!(key_value.put(Revision(0), Some(task(3))), Ok(Revision(1)));
}

#[test]
fn a_wipe_loses_every_live_message_and_an_acknowledge_cannot_bring_one_back() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.publish(task(2));
    broker.deliver(1, 0, 0);
    broker.acknowledge(1);
    broker.publish(task(3));

    broker.wipe();
    broker.acknowledge(0);

    let states: Vec<MessageState> = broker.messages().iter().map(|m| m.state).collect();
    assert_eq!(
        states,
        [
            MessageState::Lost,
            MessageState::Acknowledged,
            MessageState::Lost
        ]
    );
    assert!(!broker.has_live_message(task(1)));
}

#[test]
fn a_duplicate_is_a_second_ready_message_of_the_same_task() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));
    broker.deliver(0, 0, 0);

    broker.duplicate(0);

    assert_eq!(broker.messages().len(), 2);
    assert_eq!(broker.messages()[1].task, task(1));
    assert_eq!(broker.ready_messages().collect::<Vec<_>>(), [1]);
}

#[test]
fn a_lost_message_is_no_longer_live() {
    let mut broker = SimulatedBroker::new(2);
    broker.publish(task(1));

    broker.lose(0);

    assert!(!broker.has_live_message(task(1)));
    assert_eq!(broker.ready_messages().count(), 0);
}

#[test]
fn a_skewed_holder_reads_a_later_time_from_the_same_clock() {
    let mut clock = SimulatedClock::new(Duration::from_secs(20));
    clock.advance();

    assert_eq!(clock.ticks_at(clock.local_now(0)), 1);
    assert_eq!(clock.ticks_at(clock.local_now(1)), 2);
}
