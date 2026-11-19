use crate::protocol::TaskId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageState {
    Ready,
    Inflight,
    Acknowledged,
    Lost,
}

#[derive(Clone, Debug)]
pub struct BrokerMessage {
    pub task: TaskId,
    pub state: MessageState,
    pub deadline: u64,
    pub consumer: Option<usize>,
}

impl BrokerMessage {
    fn is_live(&self) -> bool {
        matches!(self.state, MessageState::Ready | MessageState::Inflight)
    }
}

#[derive(Clone, Debug)]
pub struct SimulatedBroker {
    acknowledge_wait: u64,
    messages: Vec<BrokerMessage>,
}

impl SimulatedBroker {
    pub fn new(acknowledge_wait: u64) -> Self {
        Self {
            acknowledge_wait,
            messages: Vec::new(),
        }
    }

    pub fn messages(&self) -> &[BrokerMessage] {
        &self.messages
    }

    pub fn has_live_message(&self, task: TaskId) -> bool {
        self.messages
            .iter()
            .any(|message| message.task == task && message.is_live())
    }

    pub fn is_done(&self, task: TaskId) -> bool {
        self.messages
            .iter()
            .any(|message| message.task == task && message.state == MessageState::Acknowledged)
    }

    pub fn live_tasks(&self) -> impl Iterator<Item = TaskId> + '_ {
        self.messages
            .iter()
            .filter(|message| message.is_live())
            .map(|message| message.task)
    }

    pub fn ready_messages(&self) -> impl Iterator<Item = usize> + '_ {
        self.messages
            .iter()
            .enumerate()
            .filter(|(_, message)| message.state == MessageState::Ready)
            .map(|(index, _)| index)
    }

    pub fn expired_messages(&self, now: u64) -> impl Iterator<Item = usize> + '_ {
        self.messages
            .iter()
            .enumerate()
            .filter(move |(_, message)| {
                message.state == MessageState::Inflight && now >= message.deadline
            })
            .map(|(index, _)| index)
    }

    pub fn publish(&mut self, task: TaskId) {
        if self.has_live_message(task) {
            return;
        }
        self.messages.push(BrokerMessage {
            task,
            state: MessageState::Ready,
            deadline: 0,
            consumer: None,
        });
    }

    pub fn deliver(&mut self, message: usize, consumer: usize, now: u64) -> TaskId {
        let delivered = &mut self.messages[message];
        delivered.state = MessageState::Inflight;
        delivered.consumer = Some(consumer);
        delivered.deadline = now + self.acknowledge_wait;
        delivered.task
    }

    pub fn redeliver(&mut self, message: usize) {
        self.messages[message].state = MessageState::Ready;
        self.messages[message].consumer = None;
    }

    pub fn acknowledge_progress(&mut self, message: usize, consumer: usize, now: u64) {
        let held = &mut self.messages[message];
        if held.state == MessageState::Inflight && held.consumer == Some(consumer) {
            held.deadline = now + self.acknowledge_wait;
        }
    }

    pub fn acknowledge(&mut self, message: usize) {
        if self.messages[message].state != MessageState::Lost {
            self.messages[message].state = MessageState::Acknowledged;
        }
    }

    pub fn duplicate(&mut self, message: usize) {
        let task = self.messages[message].task;
        self.messages.push(BrokerMessage {
            task,
            state: MessageState::Ready,
            deadline: 0,
            consumer: None,
        });
    }

    pub fn lose(&mut self, message: usize) {
        if self.messages[message].is_live() {
            self.messages[message].state = MessageState::Lost;
        }
    }

    pub fn wipe(&mut self) {
        for message in 0..self.messages.len() {
            self.lose(message);
        }
    }
}
