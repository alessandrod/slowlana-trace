use {crate::TraceQueuePaths, std::path::Path};

pub const DEFAULT_TRACE_DIR: &str = "/dev/shm";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgaveTracePaths {
    pub events: TraceQueuePaths,
    pub tx: TraceQueuePaths,
    pub svm: TraceQueuePaths,
}

#[derive(Debug, Copy, Clone)]
#[repr(u32)]
pub enum EventKind {
    ReplaySlotComplete = 1,
    TurbineSlotComplete = 2,
    RetransmitStats = 3,
    RepairStats = 4,
    SchedulingDetails = 5,
    PohSlot = 6,
    ShredRecvRange = 7,
    ShredFrontier = 8,
    ShredGap = 9,
    ShredTurbineLayer = 10,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct RetransmitStatsEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub num_nodes: u64,
    pub num_shreds: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct RepairStatsEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub num_repairs: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct SchedulingDetailsEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub blocked: u64,
    pub queue_size: u64,
    pub buffer_size: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ReplaySlotCompleteEvent {
    pub pid: u32,
    pub tid: u32,
    pub slot: u64,
    pub start_ts: u64,
    pub end_ts: u64,
    pub num_shreds: u64,
    pub num_entries: u64,
    pub num_txs: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct TurbineSlotCompleteEvent {
    pub pid: u32,
    pub tid: u32,
    pub slot: u64,
    pub start_ts: u64,
    pub end_ts: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u64)]
pub enum ShredSource {
    Turbine = 1,
    Repair = 2,
    Recovered = 3,
}

impl ShredSource {
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            1 => Some(Self::Turbine),
            2 => Some(Self::Repair),
            3 => Some(Self::Recovered),
            _ => None,
        }
    }
}

pub const UNKNOWN_TURBINE_LAYER: u64 = u64::MAX;

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ShredRecvRangeEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub slot: u64,
    pub start_index: u64,
    pub end_index: u64,
    pub source: u64,
    pub turbine_layer: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ShredTurbineLayerEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub slot: u64,
    pub index: u64,
    pub turbine_layer: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ShredFrontierEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub slot: u64,
    pub highest_received: u64,
    pub consumed: u64,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ShredGapEvent {
    pub pid: u32,
    pub tid: u32,
    pub start_ts: u64,
    pub end_ts: u64,
    pub slot: u64,
    pub start_index: u64,
    pub end_index: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u64)]
pub enum PohSlotTag {
    ControllerSetBank = 1,
    ControllerReset = 2,
    ServiceSetBank = 3,
    ServiceReset = 4,
}

impl PohSlotTag {
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            1 => Some(Self::ControllerSetBank),
            2 => Some(Self::ControllerReset),
            3 => Some(Self::ServiceSetBank),
            4 => Some(Self::ServiceReset),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct PohSlotEvent {
    pub pid: u32,
    pub tid: u32,
    pub ts: u64,
    pub slot: u64,
    pub tag: u64,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u64)]
pub enum TransactionState {
    Received = 1,
    Deduped = 2,
    Buffered = 3,
    Scheduled = 4,
    Executed = 5,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct TransactionEvent {
    pub flow_id: u64,
    pub sig: [u8; 64],
    pub ts: u64,
    pub state: TransactionState,
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct SvmEvent {
    pub sig: [u8; 64],
    pub start: u64,
    pub end: u64,
    pub tid: u64,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union EventPayload {
    pub replay_slot_complete: ReplaySlotCompleteEvent,
    pub turbine_slot_complete: TurbineSlotCompleteEvent,
    pub retransmit_stats: RetransmitStatsEvent,
    pub repair_stats: RepairStatsEvent,
    pub scheduling_details: SchedulingDetailsEvent,
    pub poh_slot: PohSlotEvent,
    pub shred_recv_range: ShredRecvRangeEvent,
    pub shred_turbine_layer: ShredTurbineLayerEvent,
    pub shred_frontier: ShredFrontierEvent,
    pub shred_gap: ShredGapEvent,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct Event {
    pub kind: EventKind,
    pub payload: EventPayload,
}

impl Event {
    pub fn replay_slot_complete(event: ReplaySlotCompleteEvent) -> Self {
        Self {
            kind: EventKind::ReplaySlotComplete,
            payload: EventPayload {
                replay_slot_complete: event,
            },
        }
    }

    pub fn turbine_slot_complete(event: TurbineSlotCompleteEvent) -> Self {
        Self {
            kind: EventKind::TurbineSlotComplete,
            payload: EventPayload {
                turbine_slot_complete: event,
            },
        }
    }

    pub fn retransmit_stats(event: RetransmitStatsEvent) -> Self {
        Self {
            kind: EventKind::RetransmitStats,
            payload: EventPayload {
                retransmit_stats: event,
            },
        }
    }

    pub fn repair_stats(event: RepairStatsEvent) -> Self {
        Self {
            kind: EventKind::RepairStats,
            payload: EventPayload {
                repair_stats: event,
            },
        }
    }

    pub fn scheduling_details(event: SchedulingDetailsEvent) -> Self {
        Self {
            kind: EventKind::SchedulingDetails,
            payload: EventPayload {
                scheduling_details: event,
            },
        }
    }

    pub fn poh_slot(event: PohSlotEvent) -> Self {
        Self {
            kind: EventKind::PohSlot,
            payload: EventPayload { poh_slot: event },
        }
    }

    pub fn shred_recv_range(event: ShredRecvRangeEvent) -> Self {
        Self {
            kind: EventKind::ShredRecvRange,
            payload: EventPayload {
                shred_recv_range: event,
            },
        }
    }

    pub fn shred_turbine_layer(event: ShredTurbineLayerEvent) -> Self {
        Self {
            kind: EventKind::ShredTurbineLayer,
            payload: EventPayload {
                shred_turbine_layer: event,
            },
        }
    }

    pub fn shred_frontier(event: ShredFrontierEvent) -> Self {
        Self {
            kind: EventKind::ShredFrontier,
            payload: EventPayload {
                shred_frontier: event,
            },
        }
    }

    pub fn shred_gap(event: ShredGapEvent) -> Self {
        Self {
            kind: EventKind::ShredGap,
            payload: EventPayload { shred_gap: event },
        }
    }
}

pub fn trace_paths(pid: u32, dir: impl AsRef<Path>) -> AgaveTracePaths {
    let dir = dir.as_ref();
    AgaveTracePaths {
        events: queue_paths(dir, pid, "events"),
        tx: queue_paths(dir, pid, "tx"),
        svm: queue_paths(dir, pid, "svm"),
    }
}

fn queue_paths(dir: &Path, pid: u32, stream: &str) -> TraceQueuePaths {
    let prefix = format!("agave-trace-v1-{pid}.{stream}");
    TraceQueuePaths::new(
        dir.join(format!("{prefix}.q")),
        dir.join(format!("{prefix}.meta")),
    )
}

#[cfg(test)]
mod tests {
    use {
        crate::agave::{
            trace_paths, PohSlotEvent, ReplaySlotCompleteEvent, ShredFrontierEvent, ShredGapEvent,
            ShredRecvRangeEvent, ShredTurbineLayerEvent, TransactionEvent, DEFAULT_TRACE_DIR,
        },
        std::{mem, path::Path},
    };

    #[test]
    fn agave_event_layouts_are_stable() {
        assert_eq!(mem::size_of::<PohSlotEvent>(), 32);
        assert_eq!(mem::size_of::<ShredRecvRangeEvent>(), 56);
        assert_eq!(mem::size_of::<ShredTurbineLayerEvent>(), 40);
        assert_eq!(mem::size_of::<ShredFrontierEvent>(), 40);
        assert_eq!(mem::size_of::<ShredGapEvent>(), 48);
        assert_eq!(mem::size_of::<TransactionEvent>(), 88);
        assert!(mem::size_of::<crate::agave::Event>() >= mem::size_of::<ReplaySlotCompleteEvent>());
    }

    #[test]
    fn trace_paths_use_expected_file_names() {
        let paths = trace_paths(42, Path::new(DEFAULT_TRACE_DIR));
        assert_eq!(
            paths.events.queue_path,
            Path::new(DEFAULT_TRACE_DIR).join("agave-trace-v1-42.events.q")
        );
        assert_eq!(
            paths.events.meta_path,
            Path::new(DEFAULT_TRACE_DIR).join("agave-trace-v1-42.events.meta")
        );
        assert_eq!(
            paths.tx.queue_path,
            Path::new(DEFAULT_TRACE_DIR).join("agave-trace-v1-42.tx.q")
        );
        assert_eq!(
            paths.svm.meta_path,
            Path::new(DEFAULT_TRACE_DIR).join("agave-trace-v1-42.svm.meta")
        );
    }
}
