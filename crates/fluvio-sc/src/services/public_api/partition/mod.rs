use std::io::{Error, ErrorKind};

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{trace, debug, instrument};
use anyhow::Result;

use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::partition::PartitionSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};

use crate::services::auth::AuthServiceContext;
use crate::stores::Store;

#[instrument(skip(_filters, auth_ctx))]
pub async fn handle_fetch_request<
    AC: AuthContext,
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    _filters: ListFilters,
    auth_ctx: &AuthServiceContext<
        AC,
        C,
        SpuStore,
        PartitionStore,
        TopicStore,
        SpgStore,
        SmartModuleStore,
        TableFormatStore,
    >,
) -> Result<ListResponse<PartitionSpec>> {
    debug!("fetching custom spu list");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(PartitionSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error").into());
    }

    let partitions: Vec<Metadata<PartitionSpec>> = auth_ctx
        .global_ctx
        .partitions()
        .store()
        .read()
        .await
        .values()
        .map(|value| value.inner().clone().into())
        .collect();

    debug!("flv fetch partitions resp: {} items", partitions.len());
    trace!("flv fetch partitions resp {:#?}", partitions);

    Ok(ListResponse::new(partitions))
}
