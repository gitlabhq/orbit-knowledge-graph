use crate::protocol::TaskId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyState {
    Ready,
    Inflight,
    Acked,
}

#[derive(Clone, Debug)]
pub struct StreamCopy {
    pub task: TaskId,
    pub state: CopyState,
    pub deadline: u64,
    pub consumer: Option<usize>,
}

impl StreamCopy {
    fn is_live(&self) -> bool {
        self.state != CopyState::Acked
    }
}

#[derive(Clone, Debug)]
pub struct SimulatedStream {
    acknowledge_wait: u64,
    copies: Vec<StreamCopy>,
}

impl SimulatedStream {
    pub fn new(acknowledge_wait: u64) -> Self {
        Self {
            acknowledge_wait,
            copies: Vec::new(),
        }
    }

    pub fn copies(&self) -> &[StreamCopy] {
        &self.copies
    }

    pub fn has_live_copy(&self, task: TaskId) -> bool {
        self.copies
            .iter()
            .any(|copy| copy.task == task && copy.is_live())
    }

    pub fn live_tasks(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.copies
            .iter()
            .filter(|copy| copy.is_live())
            .map(|copy| copy.task)
    }

    pub fn ready_copies(&self) -> impl Iterator<Item = usize> + '_ {
        self.copies
            .iter()
            .enumerate()
            .filter(|(_, copy)| copy.state == CopyState::Ready)
            .map(|(index, _)| index)
    }

    pub fn expired_copies(&self, now: u64) -> impl Iterator<Item = usize> + '_ {
        self.copies
            .iter()
            .enumerate()
            .filter(move |(_, copy)| copy.state == CopyState::Inflight && now >= copy.deadline)
            .map(|(index, _)| index)
    }

    pub fn publish(&mut self, task: TaskId) {
        if self.has_live_copy(task) {
            return;
        }
        self.copies.push(StreamCopy {
            task,
            state: CopyState::Ready,
            deadline: 0,
            consumer: None,
        });
    }

    pub fn deliver(&mut self, copy: usize, consumer: usize, now: u64) -> TaskId {
        let delivered = &mut self.copies[copy];
        delivered.state = CopyState::Inflight;
        delivered.consumer = Some(consumer);
        delivered.deadline = now + self.acknowledge_wait;
        delivered.task
    }

    pub fn redeliver(&mut self, copy: usize) {
        self.copies[copy].state = CopyState::Ready;
        self.copies[copy].consumer = None;
    }

    pub fn acknowledge_progress(&mut self, copy: usize, consumer: usize, now: u64) {
        let held = &mut self.copies[copy];
        if held.state == CopyState::Inflight && held.consumer == Some(consumer) {
            held.deadline = now + self.acknowledge_wait;
        }
    }

    pub fn acknowledge(&mut self, copy: usize) {
        self.copies[copy].state = CopyState::Acked;
    }
}
