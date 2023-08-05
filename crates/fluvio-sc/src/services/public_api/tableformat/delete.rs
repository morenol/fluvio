use std::io::{Error, ErrorKind};

use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{info, trace, instrument};

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;

use fluvio_sc_schema::Status;
use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;
use crate::stores::Store;

/// Handler for delete tableformat request
#[instrument(skip(name, auth_ctx))]
pub async fn handle_delete_tableformat<
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
) -> Result<Status, Error> {
    use fluvio_protocol::link::ErrorCode;

    info!(%name, "deleting tableformat");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_instance_action(TableFormatSpec::OBJECT_TYPE, InstanceAction::Delete, &name)
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
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let status = if auth_ctx
        .global_ctx
        .tableformats()
        .store()
        .value(&name)
        .await
        .is_some()
    {
        if let Err(err) = auth_ctx
            .global_ctx
            .tableformats()
            .delete(name.clone())
            .await
        {
            Status::new(
                name.clone(),
                ErrorCode::TableFormatError,
                Some(err.to_string()),
            )
        } else {
            info!(%name, "tableformat deleted");
            Status::new_ok(name)
        }
    } else {
        Status::new(
            name,
            ErrorCode::TableFormatNotFound,
            Some("not found".to_owned()),
        )
    };

    trace!("flv delete tableformat resp {:#?}", status);

    Ok(status)
}
