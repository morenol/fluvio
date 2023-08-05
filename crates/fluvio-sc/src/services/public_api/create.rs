use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_protocol::link::ErrorCode;
use fluvio_stream_model::core::MetadataItem;
use tracing::{instrument, debug, error};
use anyhow::Result;

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::{CustomSpuSpec, SpuSpec};
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status, TryEncodableFrom};
use fluvio_sc_schema::objects::{ObjectApiCreateRequest, CreateRequest};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;
use crate::stores::Store;

/// Handler for create topic request
#[instrument(skip(request, auth_context))]
pub async fn handle_create_request<
    AC: AuthContext,
    C: MetadataItem,
    SpuStore: Store<SpuSpec, C>,
    PartitionStore: Store<PartitionSpec, C>,
    TopicStore: Store<TopicSpec, C>,
    SpgStore: Store<SpuGroupSpec, C>,
    SmartModuleStore: Store<SmartModuleSpec, C>,
    TableFormatStore: Store<TableFormatSpec, C>,
>(
    request: Box<RequestMessage<ObjectApiCreateRequest>>,
    auth_context: &AuthServiceContext<
        AC,
        C,
        SpuStore,
        PartitionStore,
        TopicStore,
        SpgStore,
        SmartModuleStore,
        TableFormatStore,
    >,
) -> Result<ResponseMessage<Status>> {
    let (header, req) = request.get_header_request();

    debug!(?req, "create request");
    let status = if let Some(create) = req.downcast()? as Option<CreateRequest<TopicSpec>> {
        super::topic::handle_create_topics_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<SpuGroupSpec>> {
        super::spg::handle_create_spu_group_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<CustomSpuSpec>> {
        super::spu::RegisterCustomSpu::handle_register_custom_spu_request(create, auth_context)
            .await
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<SmartModuleSpec>> {
        super::smartmodule::handle_create_smartmodule_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<TableFormatSpec>> {
        super::tableformat::handle_create_tableformat_request(create, auth_context).await?
    } else {
        error!("unknown create request: {:#?}", req);
        Status::new(
            "create error".to_owned(),
            ErrorCode::Other("unknown admin object type".to_owned()),
            None,
        )
    };

    Ok(ResponseMessage::from_header(&header, status))
}

mod create_handler {
    use std::convert::{TryFrom, TryInto};
    use std::fmt::Display;
    use std::io::{Error, ErrorKind};

    use fluvio_controlplane_metadata::core::Spec;
    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
    use fluvio_controlplane_metadata::spg::SpuGroupSpec;
    use fluvio_controlplane_metadata::spu::SpuSpec;
    use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
    use fluvio_controlplane_metadata::topic::TopicSpec;
    use fluvio_stream_model::core::MetadataItem;
    use tracing::{info, trace, instrument};

    use fluvio_protocol::link::ErrorCode;
    use fluvio_sc_schema::{AdminSpec, Status};
    use fluvio_sc_schema::objects::CommonCreateRequest;
    use fluvio_controlplane_metadata::extended::SpecExt;
    use fluvio_auth::{AuthContext, TypeAction};

    use crate::services::auth::AuthServiceContext;
    use crate::stores::Store;

    #[instrument(skip(create, spec, auth_ctx, object_ctx, error_code))]
    pub async fn process<
        AC: AuthContext,
        S,
        F,
        C: MetadataItem,
        SpuStore: Store<SpuSpec, C>,
        PartitionStore: Store<PartitionSpec, C>,
        TopicStore: Store<TopicSpec, C>,
        SpgStore: Store<SpuGroupSpec, C>,
        SmartModuleStore: Store<SmartModuleSpec, C>,
        TableFormatStore: Store<TableFormatSpec, C>,
        SS: Store<S, C>,
    >(
        create: CommonCreateRequest,
        spec: S,
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
        object_ctx: &SS,
        error_code: F,
    ) -> Result<Status, Error>
    where
        S: AdminSpec + SpecExt,
        <S as Spec>::IndexKey: TryFrom<String> + Display,
        F: FnOnce(Error) -> ErrorCode,
    {
        let name = create.name;

        info!(%name, ty = %S::LABEL,"creating");

        if let Ok(authorized) = auth_ctx
            .auth
            .allow_type_action(S::OBJECT_TYPE, TypeAction::Create)
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

        Ok(
            if let Err(err) = object_ctx
                .create_spec(
                    name.clone()
                        .try_into()
                        .map_err(|_err| Error::new(ErrorKind::InvalidData, "not convertible"))?,
                    spec,
                )
                .await
            {
                let error = Some(err.to_string());
                Status::new(name, error_code(err), error)
            } else {
                info!(%name, ty = %S::LABEL,"created");

                Status::new_ok(name.clone())
            },
        )
    }
}
