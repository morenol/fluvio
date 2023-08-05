//!
//! # Streaming Coordinator Metadata
//!
//! Metadata stores a copy of the data from KV store in local memory.
//!
use std::marker::PhantomData;
use std::sync::Arc;

use fluvio_stream_model::core::MetadataItem;

use crate::config::ScConfig;
use crate::stores::spu::*;
use crate::stores::partition::*;
use crate::stores::topic::*;
use crate::stores::spg::*;
use crate::stores::smartmodule::*;
use crate::stores::tableformat::*;
use crate::stores::*;

pub type SharedContext<
    C,
    SpuStore,
    PartitionStore,
    TopicStore,
    SpgStore,
    SmartModuleStore,
    TableFormatStore,
> = Arc<
    Context<C, SpuStore, PartitionStore, TopicStore, SpgStore, SmartModuleStore, TableFormatStore>,
>;

pub type K8Context = Context<
    K8MetaItem,
    StoreContext<SpuSpec>,
    StoreContext<PartitionSpec>,
    StoreContext<TopicSpec>,
    StoreContext<SpuGroupSpec>,
    StoreContext<SmartModuleSpec>,
    StoreContext<TableFormatSpec>,
>;
pub type K8SharedContext = SharedContext<
    K8MetaItem,
    StoreContext<SpuSpec>,
    StoreContext<PartitionSpec>,
    StoreContext<TopicSpec>,
    StoreContext<SpuGroupSpec>,
    StoreContext<SmartModuleSpec>,
    StoreContext<TableFormatSpec>,
>;
/// Global Context for SC
/// This is where we store globally accessible data
#[derive(Debug)]
pub struct Context<
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
> {
    spus: SpuStore,
    partitions: PartitionStore,
    topics: TopicStore,
    spgs: SpgStore,
    smartmodules: SmartModuleStore,
    tableformats: TableFormatStore,
    health: SharedHealthCheck,
    config: ScConfig,
    phantom: std::marker::PhantomData<C>,
}

// -----------------------------------
// ScMetadata - Implementation
// -----------------------------------

impl<
        C: MetadataItem,
        SpuStore: Store<SpuSpec, C>,
        PartitionStore: Store<PartitionSpec, C>,
        TopicStore: Store<TopicSpec, C>,
        SpgStore: Store<SpuGroupSpec, C>,
        SmartModuleStore: Store<SmartModuleSpec, C>,
        TableFormatStore: Store<TableFormatSpec, C>,
    >
    Context<C, SpuStore, PartitionStore, TopicStore, SpgStore, SmartModuleStore, TableFormatStore>
{
    pub fn shared_metadata(config: ScConfig) -> Arc<Self> {
        Arc::new(Self::new(config))
    }

    /// private function to provision metadata
    fn new(config: ScConfig) -> Self {
        Self {
            spus: SpuStore::new(),
            partitions: PartitionStore::new(),
            topics: TopicStore::new(),
            spgs: SpgStore::new(),
            smartmodules: SmartModuleStore::new(),
            tableformats: TableFormatStore::new(),
            health: HealthCheck::shared(),
            config,
            phantom: PhantomData,
        }
    }

    /// reference to spus
    pub fn spus(&self) -> &SpuStore {
        &self.spus
    }

    /// reference to partitions
    pub fn partitions(&self) -> &PartitionStore {
        &self.partitions
    }

    /// reference to topics
    pub fn topics(&self) -> &TopicStore {
        &self.topics
    }

    pub fn spgs(&self) -> &SpgStore {
        &self.spgs
    }

    pub fn smartmodules(&self) -> &SmartModuleStore {
        &self.smartmodules
    }

    pub fn tableformats(&self) -> &TableFormatStore {
        &self.tableformats
    }

    /// spu health channel
    pub fn health(&self) -> &SharedHealthCheck {
        &self.health
    }

    /// reference to config
    pub fn config(&self) -> &ScConfig {
        &self.config
    }

    #[cfg(feature = "k8")]
    pub fn namespace(&self) -> &str {
        &self.config.namespace
    }
}
