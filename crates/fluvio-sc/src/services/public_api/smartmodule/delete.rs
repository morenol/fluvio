use std::io::{Error, ErrorKind};

use fluvio_stream_model::core::MetadataItem;
use tracing::{debug, trace, instrument, info};
use anyhow::Result;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModulePackageKey};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;
use crate::stores::Store;

/// Handler for delete smartmodule request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_smartmodule<
    AC: AuthContext,
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    name: String,
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
    use fluvio_protocol::link::ErrorCode;

    debug!(%name,"deleting smartmodule");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(SmartModuleSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error").into());
    }

    let sm_fqdn = SmartModulePackageKey::from_qualified_name(&name)?.store_id();

    info!(%sm_fqdn,"deleting smartmodule");

    let status = if auth_ctx
        .global_ctx
        .smartmodules()
        .store()
        .value(&sm_fqdn)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx.global_ctx.smartmodules().delete(sm_fqdn).await {
            Status::new(
                name.clone(),
                ErrorCode::SmartModuleError,
                Some(err.to_string()),
            )
        } else {
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name.clone(),
            ErrorCode::SmartModuleNotFound { name },
            Some("not found".to_owned()),
        )
    };

    trace!("smartmodule deleting resp {:#?}", status);

    Ok(status)
}
