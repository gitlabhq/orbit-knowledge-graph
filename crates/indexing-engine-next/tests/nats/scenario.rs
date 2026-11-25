use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::server::{connect_to_empty_nats, delete_every_stream};
use crate::spec_trace::{ActorObserver, Journal};
use indexing_engine_next::lease::{HolderId, LeaseRules};
use indexing_engine_next::nats::{NatsSettings, NatsTransport};
use indexing_engine_next::{Engine, Outcome, TaskContext, TaskError, Worker};
use indexing_engine_next::{EngineConfig, LaneConfig};
use indexing_engine_next::{Key, Tenant};
use indexing_engine_next::{Lane, LaneId};

const KEYS: u32 = 6;
const RULES: LeaseRules = LeaseRules {
    ttl: Duration::from_secs(2),
    renew_every: Duration::from_millis(500),
    skew_margin: Duration::from_millis(200),
};
const SETTINGS: NatsSettings = NatsSettings {
    replicas: 1,
    ack_wait: Duration::from_secs(1),
    idle_poll: Duration::from_millis(50),
    stream_check: Duration::from_millis(200),
};

#[derive(Serialize, Deserialize)]
struct WriteInput {
    key: u32,
}

struct SlowWrite;

struct Pod {
    serving: JoinHandle<anyhow::Result<()>>,
}

#[async_trait]
impl Worker for SlowWrite {
    const NAME: &'static str = "scenario.write";
    const EXCLUSIVE: bool = true;
    type Input = WriteInput;
    type Output = ();

    fn key(input: &WriteInput) -> Key {
        Key::new("scenario", input.key)
    }

    async fn run(&self, _ctx: &TaskContext, _input: WriteInput) -> Result<Outcome<()>, TaskError> {
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(Outcome::done(()))
    }
}

impl Pod {
    async fn start(
        client: &async_nats::Client,
        name: &'static str,
        journal: &Journal,
    ) -> (Self, Arc<NatsTransport>) {
        let transport = Arc::new(
            NatsTransport::open(client.clone(), SETTINGS)
                .await
                .expect("the pod creates or joins the NATS resources"),
        );
        let engine = build_engine(journal.observer_for(name));
        let ports = transport.ports().clone();
        let holder = HolderId::new(name);
        let serving = tokio::spawn(async move { engine.serve(ports, holder, RULES).await });
        (Self { serving }, transport)
    }

    fn crash(self) {
        self.serving.abort();
    }

    async fn wait_until_it_stops(self) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(5), self.serving)
            .await
            .expect("the pod stops after its task stream is gone")
            .expect("the pod does not panic")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts a NATS container; mise run test:engine:nats runs it"]
async fn every_task_finishes_after_a_pod_crash_and_after_a_nats_wipe() {
    let (client, _container) = connect_to_empty_nats().await;
    let journal = Journal::default();
    let planner = build_engine(journal.observer_for("planner"));
    let keys = usize::try_from(KEYS).unwrap_or(usize::MAX);

    let (first, transport) = Pod::start(&client, "p1", &journal).await;
    let (second, _) = Pod::start(&client, "p2", &journal).await;
    submit_every_key(&planner, &transport).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    first.crash();
    journal.record_crash("p1");
    journal.wait_until_keys_finish(keys, 0).await;

    journal.record_wipe();
    delete_every_stream(&client).await;
    let stopped = second.wait_until_it_stops().await;
    assert!(stopped.is_err(), "a pod stops once its task stream is gone");
    journal.record_crash("p2");

    let after_wipe = journal.mark();
    let (_third, transport) = Pod::start(&client, "p3", &journal).await;
    submit_every_key(&planner, &transport).await;
    journal.wait_until_keys_finish(keys, after_wipe).await;

    if let Ok(folder) = std::env::var("ENGINE_SPEC_NATS_TRACES") {
        journal
            .write_tla_traces(&PathBuf::from(folder))
            .expect("the traces of each key are written");
    }
}

fn build_engine(observer: ActorObserver) -> Engine {
    let lane = LaneId::new(SlowWrite::NAME, Lane::DEFAULT);
    let slots = NonZeroU32::new(4).unwrap_or(NonZeroU32::MIN);
    let config =
        EngineConfig::default().with_lane(lane, LaneConfig::new(slots, Duration::from_secs(60)));
    Engine::builder()
        .config(config)
        .module(|module| {
            module.worker(SlowWrite);
            module.observer(observer);
        })
        .build()
        .expect("the scenario engine builds")
}

async fn submit_every_key(planner: &Engine, transport: &NatsTransport) {
    for key in 0..KEYS {
        planner
            .submit::<SlowWrite>(
                transport.ports(),
                Tenant::new("scenario"),
                &WriteInput { key },
                Lane::DEFAULT,
            )
            .await
            .expect("the submit reaches NATS");
    }
}
