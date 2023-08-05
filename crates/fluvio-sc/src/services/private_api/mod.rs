mod private_server;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::info;
use tracing::instrument;

use private_server::ScInternalService;
use fluvio_service::FluvioApiServer;

use crate::core::SharedContext;
use crate::stores::Store;

// start server
#[instrument(
    name = "sc_private_server"
    skip(ctx),
    fields(address = &*ctx.config().private_endpoint)
)]
pub fn start_internal_server<
    C: MetadataItem + 'static,
    SpuStore: Store<SpuSpec, C> + Send + Sync + 'static,
    PartitionStore: Store<PartitionSpec, C> + Send + Sync + 'static,
    TopicStore: Store<TopicSpec, C> + Send + Sync + 'static,
    SpgStore: Store<SpuGroupSpec, C> + Send + Sync + 'static,
    SmartModuleStore: Store<SmartModuleSpec, C> + Send + Sync + 'static,
    TableFormatStore: Store<TableFormatSpec, C> + Send + Sync + 'static,
>(
    ctx: SharedContext<
        C,
        SpuStore,
        PartitionStore,
        TopicStore,
        SpgStore,
        SmartModuleStore,
        TableFormatStore,
    >,
) {
    info!("starting internal services");

    let addr = ctx.config().private_endpoint.clone();
    let server = FluvioApiServer::new(addr, ctx, ScInternalService::new());
    server.run();
}
