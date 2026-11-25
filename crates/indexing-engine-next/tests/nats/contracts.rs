use std::fmt::Debug;
use std::time::{Duration, UNIX_EPOCH};

use testcontainers::ContainerAsync;
use testcontainers_modules::nats::Nats;
use uuid::Uuid;

use crate::server::{connect_to_empty_nats, delete_every_stream};
use indexing_engine_next::lease::{HolderId, Lease};
use indexing_engine_next::nats::{NatsSettings, NatsTransport};
use indexing_engine_next::ports::Ports;
use indexing_engine_next::task::{Delivery, TaskId, TaskMessage, WorkerKey};
use indexing_engine_next::testkit::simulation::SimulatedNats;
use indexing_engine_next::versioned::{
    DeleteOutcome, Revision, Versioned, VersionedStore, WriteOutcome,
};
use indexing_engine_next::{Key, Tenant};
use indexing_engine_next::{Lane, LaneId};

const WORKER: &str = "contract.worker";
const SETTINGS: NatsSettings = NatsSettings {
    replicas: 1,
    ack_wait: Duration::from_secs(2),
    idle_poll: Duration::from_millis(50),
    stream_check: Duration::from_millis(200),
};

enum Harness {
    Simulated(SimulatedNats),
    Nats(Box<NatsHarness>),
}

struct NatsHarness {
    client: async_nats::Client,
    transport: NatsTransport,
    _container: Option<ContainerAsync<Nats>>,
}

impl Harness {
    async fn start_nats() -> Self {
        let (client, container) = connect_to_empty_nats().await;
        let transport = NatsTransport::open(client.clone(), SETTINGS)
            .await
            .expect("the first pod creates the NATS resources");
        Self::Nats(Box::new(NatsHarness {
            client,
            transport,
            _container: container,
        }))
    }

    fn ports(&self) -> Ports {
        match self {
            Self::Simulated(world) => world.ports(),
            Self::Nats(nats) => nats.transport.ports().clone(),
        }
    }

    fn ack_wait(&self) -> Duration {
        match self {
            Self::Simulated(world) => world.ack_wait(),
            Self::Nats(_) => SETTINGS.ack_wait,
        }
    }

    async fn wait(&self, duration: Duration) {
        match self {
            Self::Simulated(world) => world.wait_whole_ticks_in(duration).await,
            Self::Nats(_) => tokio::time::sleep(duration).await,
        }
    }

    async fn wipe(&mut self) {
        match self {
            Self::Simulated(world) => world.wipe(),
            Self::Nats(nats) => delete_every_stream(&nats.client).await,
        }
    }

    async fn restart_after_wipe(&mut self) {
        match self {
            Self::Simulated(_) => {}
            Self::Nats(nats) => {
                nats.transport = NatsTransport::open(nats.client.clone(), SETTINGS)
                    .await
                    .expect("a restarted pod creates the NATS resources again");
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn the_simulated_ports_keep_every_contract() {
    check_every_contract(Harness::Simulated(SimulatedNats::default())).await;
}

#[tokio::test]
#[ignore = "starts a NATS container; mise run test:engine:nats runs it"]
async fn the_nats_ports_keep_every_contract() {
    check_every_contract(Harness::start_nats().await).await;
}

async fn check_every_contract(mut harness: Harness) {
    let ports = harness.ports();
    a_write_or_delete_needs_the_version_of_the_last_read(
        ports.leases.as_ref(),
        lease("p1"),
        lease("p2"),
    )
    .await;
    a_write_or_delete_needs_the_version_of_the_last_read(
        ports.pending.as_ref(),
        task_message(1),
        task_message(2),
    )
    .await;
    a_task_with_a_live_copy_is_published_once(&ports).await;
    a_delivered_task_waits_for_its_ack_and_stays_gone_after_the_ack_wait(&harness).await;
    an_unacked_task_comes_back_after_the_ack_wait(&harness).await;
    a_negative_ack_brings_the_task_back_after_its_delay(&harness).await;
    a_progress_ack_restarts_the_ack_wait(&harness).await;
    a_restart_after_a_wipe_starts_from_empty_state(&mut harness).await;
}

async fn a_write_or_delete_needs_the_version_of_the_last_read<V>(
    store: &dyn VersionedStore<V>,
    first: V,
    second: V,
) where
    V: Clone + Debug + PartialEq,
{
    let key = worker_key("compare-and-set");
    let empty = store.read(&key).await.expect("the store reads a new key");
    assert_eq!(empty.value, None);
    let empty_revision = empty.revision;

    let written = expect_written(store.write(&key, empty_revision, &first).await);
    assert!(written > empty_revision);
    let stale = store.write(&key, empty_revision, &second).await;
    assert!(matches!(stale, Ok(WriteOutcome::Conflict)));
    assert!(matches!(
        store.delete(&key, empty_revision).await,
        Ok(DeleteOutcome::Conflict)
    ));
    assert_eq!(
        store.read(&key).await.expect("the store reads the key"),
        Versioned {
            value: Some(first),
            revision: written
        }
    );

    assert!(matches!(
        store.delete(&key, written).await,
        Ok(DeleteOutcome::Deleted)
    ));
    let deleted = store.read(&key).await.expect("the store reads the key");
    assert_eq!(deleted.value, None);
    assert!(deleted.revision > written);
    expect_written(store.write(&key, deleted.revision, &second).await);
}

async fn a_task_with_a_live_copy_is_published_once(ports: &Ports) {
    let message = task_message(10);
    let lane = lane_of(&message);
    ports
        .queue
        .publish(message.clone())
        .await
        .expect("the queue takes the task");
    ports
        .queue
        .publish(message.clone())
        .await
        .expect("the queue takes the copy");

    let deliveries = pull_within(ports, &lane, Duration::from_secs(1)).await;
    let tasks: Vec<TaskId> = deliveries
        .iter()
        .map(|delivery| delivery.message.task)
        .collect();
    assert_eq!(tasks, [message.task]);
    assert_eq!(deliveries[0].message, message);
    deliveries[0]
        .in_flight
        .ack()
        .await
        .expect("the queue takes the ack");
}

async fn a_delivered_task_waits_for_its_ack_and_stays_gone_after_the_ack_wait(harness: &Harness) {
    let (ports, lane, delivered) = publish_and_deliver(harness, 11).await;
    let in_flight = pull_within(&ports, &lane, harness.ack_wait() / 10).await;
    assert!(
        in_flight.is_empty(),
        "a task in flight is not delivered twice"
    );

    delivered
        .in_flight
        .ack()
        .await
        .expect("the queue takes the ack");
    harness.wait(harness.ack_wait() * 2).await;
    let acked = pull_within(&ports, &lane, harness.ack_wait() / 10).await;
    assert!(acked.is_empty(), "an acked task is gone");
}

async fn an_unacked_task_comes_back_after_the_ack_wait(harness: &Harness) {
    let (ports, lane, delivered) = publish_and_deliver(harness, 12).await;

    harness.wait(harness.ack_wait() / 2).await;
    let early = pull_within(&ports, &lane, harness.ack_wait() / 10).await;
    assert!(
        early.is_empty(),
        "a task comes back only after the ack wait"
    );

    harness.wait(harness.ack_wait()).await;
    let again = pull_within(&ports, &lane, harness.ack_wait()).await;
    assert_eq!(task_ids(&again), [delivered.message.task]);
    again[0]
        .in_flight
        .ack()
        .await
        .expect("the queue takes the ack");
}

async fn a_negative_ack_brings_the_task_back_after_its_delay(harness: &Harness) {
    let (ports, lane, delivered) = publish_and_deliver(harness, 13).await;

    delivered
        .in_flight
        .nack(harness.ack_wait() / 2)
        .await
        .expect("the queue takes the negative ack");
    let early = pull_within(&ports, &lane, harness.ack_wait() / 10).await;
    assert!(early.is_empty(), "a task comes back only after its delay");

    harness.wait(harness.ack_wait()).await;
    let again = pull_within(&ports, &lane, harness.ack_wait()).await;
    assert_eq!(task_ids(&again), [delivered.message.task]);
    again[0]
        .in_flight
        .ack()
        .await
        .expect("the queue takes the ack");
}

async fn a_progress_ack_restarts_the_ack_wait(harness: &Harness) {
    let (ports, lane, delivered) = publish_and_deliver(harness, 14).await;

    harness.wait(harness.ack_wait() * 6 / 10).await;
    delivered
        .in_flight
        .ack_progress()
        .await
        .expect("the queue takes the progress ack");
    harness.wait(harness.ack_wait() * 6 / 10).await;
    let kept = pull_within(&ports, &lane, harness.ack_wait() / 10).await;
    assert!(
        kept.is_empty(),
        "a progress ack keeps the task in flight past the first ack wait"
    );

    harness.wait(harness.ack_wait()).await;
    let again = pull_within(&ports, &lane, harness.ack_wait()).await;
    assert_eq!(task_ids(&again), [delivered.message.task]);
    again[0]
        .in_flight
        .ack()
        .await
        .expect("the queue takes the ack");
}

async fn publish_and_deliver(harness: &Harness, id: u128) -> (Ports, LaneId, Delivery) {
    let ports = harness.ports();
    let message = task_message(id);
    let lane = lane_of(&message);
    ports
        .queue
        .publish(message.clone())
        .await
        .expect("the queue takes the task");
    let mut delivered = pull_within(&ports, &lane, Duration::from_secs(1)).await;
    assert_eq!(task_ids(&delivered), [message.task]);
    (ports, lane, delivered.remove(0))
}

fn task_ids(deliveries: &[Delivery]) -> Vec<TaskId> {
    deliveries
        .iter()
        .map(|delivery| delivery.message.task)
        .collect()
}

async fn a_restart_after_a_wipe_starts_from_empty_state(harness: &mut Harness) {
    let ports = harness.ports();
    let key = worker_key("wipe");
    let entry = ports
        .leases
        .read(&key)
        .await
        .expect("the store reads the key");
    expect_written(ports.leases.write(&key, entry.revision, &lease("p1")).await);
    let message = task_message(20);
    ports
        .queue
        .publish(message.clone())
        .await
        .expect("the queue takes the task");

    harness.wipe().await;
    harness.restart_after_wipe().await;

    let ports = harness.ports();
    let after_wipe = ports
        .leases
        .read(&key)
        .await
        .expect("the store reads the key");
    assert_eq!(after_wipe.value, None);
    let lost = pull_within(&ports, &lane_of(&message), Duration::from_secs(1)).await;
    assert!(lost.is_empty(), "a wipe loses every task");
}

async fn pull_within(ports: &Ports, lane: &LaneId, window: Duration) -> Vec<Delivery> {
    let started = tokio::time::Instant::now();
    loop {
        let deliveries = ports.queue.pull(lane, 10).await.expect("the lane pulls");
        if !deliveries.is_empty() || started.elapsed() >= window {
            return deliveries;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn expect_written(outcome: anyhow::Result<WriteOutcome>) -> Revision {
    match outcome {
        Ok(WriteOutcome::Written(version)) => version,
        Ok(WriteOutcome::Conflict) => panic!("the write conflicted"),
        Err(error) => panic!("the write failed: {error:#}"),
    }
}

fn worker_key(name: &str) -> WorkerKey {
    WorkerKey {
        worker: WORKER.to_owned(),
        key: Key::new("contract", name),
    }
}

fn lease(holder: &str) -> Lease {
    Lease {
        holder: HolderId::new(holder),
        expires_at: UNIX_EPOCH + Duration::from_secs(60),
    }
}

fn task_message(id: u128) -> TaskMessage {
    TaskMessage {
        task: TaskId::new(Uuid::from_u128(id)),
        tenant: Tenant::new("contract"),
        worker: WORKER.to_owned(),
        key: Key::new("contract", id),
        lane: Lane::DEFAULT,
        input: b"{\"id\":1}".to_vec(),
    }
}

fn lane_of(message: &TaskMessage) -> LaneId {
    LaneId::new(message.worker.clone(), message.lane.clone())
}
