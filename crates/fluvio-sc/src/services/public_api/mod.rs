mod public_server;
mod spg;
mod smartmodule;
mod spu;
mod topic;
mod partition;
mod api_version;
mod create;
mod delete;
mod list;
mod watch;
mod tableformat;
mod derivedstream;

pub use server::start_public_server;

mod server {

    use std::fmt::Debug;

    use fluvio_controlplane_metadata::{
        partition::PartitionSpec, spu::SpuSpec, topic::TopicSpec, smartmodule::SmartModuleSpec,
        tableformat::TableFormatSpec, spg::SpuGroupSpec,
    };
    use fluvio_stream_model::core::MetadataItem;
    use tracing::debug;

    use fluvio_service::FluvioApiServer;
    use fluvio_auth::Authorization;

    use crate::{services::auth::AuthGlobalContext, stores::Store};
    use super::public_server::PublicService;

    /// create public server
    pub fn start_public_server<
        A,
        C,
        SpuStore,
        PartitionStore,
        TopicStore,
        SpgStore,
        SmartModuleStore,
        TableFormatStore,
    >(
        ctx: AuthGlobalContext<
            A,
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >,
    ) where
        A: Authorization + Sync + Send + Debug + 'static,
        C: MetadataItem + 'static,
        C::UId: Send + Sync,
        SpuStore: Store<SpuSpec, C> + Sync + Send + 'static,
        PartitionStore: Store<PartitionSpec, C> + Sync + Send + 'static,
        TopicStore: Store<TopicSpec, C> + Sync + Send + 'static,
        SpgStore: Store<SpuGroupSpec, C> + Sync + Send + 'static,
        SmartModuleStore: Store<SmartModuleSpec, C> + Sync + Send + 'static,
        TableFormatStore: Store<TableFormatSpec, C> + Sync + Send + 'static,
        AuthGlobalContext<
            A,
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >: Clone + Debug,
        <A as Authorization>::Context: Send + Sync,
    {
        let addr = ctx.global_ctx.config().public_endpoint.clone();
        debug!("starting public api service");
        let server = FluvioApiServer::new(addr, ctx, PublicService::new());
        server.run();
    }
}
