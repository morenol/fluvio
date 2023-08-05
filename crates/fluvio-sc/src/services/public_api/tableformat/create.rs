//!
//! # Create TableFormat Request
//!
//! Converts TableFormat API request into KV request and sends to KV store for processing.
//!

use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{debug, info, trace, instrument};
use anyhow::{anyhow, Result};

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::Status;
use fluvio_sc_schema::objects::CreateRequest;
use fluvio_sc_schema::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::extended::SpecExt;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;

use crate::core::Context;
use crate::services::auth::AuthServiceContext;

use crate::stores::Store;

/// Handler for tableformat request
#[instrument(skip(req, auth_ctx))]
pub async fn handle_create_tableformat_request<
    AC: AuthContext,
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    req: CreateRequest<TableFormatSpec>,
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
) -> Result<Status> {
    let (create, spec) = req.parts();
    let name = create.name;

    info!(%name,"creating tableformat");

    if auth_ctx
        .global_ctx
        .tableformats()
        .store()
        .contains_key(&name)
        .await
    {
        debug!("tableformat already exists");
        return Ok(Status::new(
            name.to_string(),
            ErrorCode::TableFormatAlreadyExists,
            Some(format!("tableformat '{name}' already defined")),
        ));
    }

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TableFormatSpec::OBJECT_TYPE, TypeAction::Create)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(Status::new(
                name.clone(),
                ErrorCode::PermissionDenied,
                Some(String::from("permission denied")),
            ));
        }
    } else {
        return Err(anyhow!("authorization io error"));
    }

    let status = process_tableformat_request(&auth_ctx.global_ctx, name, spec).await;
    trace!("create tableformat response {:#?}", status);

    Ok(status)
}

/// Process custom tableformat, converts tableformat spec to K8 and sends to KV store
#[instrument(skip(ctx, name, tableformat_spec))]
async fn process_tableformat_request<
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    ctx: &Context<
        C,
        SpuStore,
        PartitionStore,
        TopicStore,
        SpgStore,
        SmartModuleStore,
        TableFormatStore,
    >,
    name: String,
    tableformat_spec: TableFormatSpec,
) -> Status {
    if let Err(err) = ctx
        .tableformats()
        .create_spec(name.clone(), tableformat_spec)
        .await
    {
        let error = Some(err.to_string());
        Status::new(name, ErrorCode::TableFormatError, error) // TODO: create error type
    } else {
        info!(%name,"tableformat created");
        Status::new_ok(name.clone())
    }
}
