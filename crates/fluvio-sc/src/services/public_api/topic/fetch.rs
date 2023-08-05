use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{trace, debug, instrument};
use anyhow::{anyhow, Result};

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::topic::TopicSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;
use crate::stores::Store;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_topics_request<
    AC: AuthContext,
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    filters: ListFilters,
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
) -> Result<ListResponse<TopicSpec>> {
    debug!("retrieving topic list: {:#?}", filters);

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TopicSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(anyhow!("authorization error"));
    }

    let topics: Vec<Metadata<TopicSpec>> = auth_ctx
        .global_ctx
        .topics()
        .store()
        .read()
        .await
        .values()
        .filter_map(|value| {
            if filters.filter(value.key()) {
                Some(value.inner().clone().into())
            } else {
                None
            }
        })
        .collect();

    debug!("flv fetch topics resp: {} items", topics.len());
    trace!("flv fetch topics resp {:#?}", topics);

    Ok(ListResponse::new(topics))
}
