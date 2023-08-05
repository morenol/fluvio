//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!
use std::sync::Arc;

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_stream_model::core::MetadataItem;
#[cfg(feature = "k8")]
use k8_metadata_client::{MetadataClient, SharedClient};

use crate::core::Context;
use crate::core::SharedContext;
use crate::core::K8SharedContext;
use crate::controllers::spus::SpuController;
use crate::controllers::topics::TopicController;
use crate::controllers::partitions::PartitionController;
#[cfg(feature = "k8")]
use crate::config::ScConfig;
use crate::services::start_internal_server;
#[cfg(feature = "k8")]
use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
use crate::services::auth::basic::BasicRbacPolicy;
use crate::stores::Store;

#[cfg(feature = "k8")]
pub async fn start_main_loop_with_k8<C>(
    sc_config_policy: (ScConfig, Option<BasicRbacPolicy>),
    metadata_client: SharedClient<C>,
) -> K8SharedContext
where
    C: MetadataClient + 'static,
{
    use crate::core::K8Context;

    let (sc_config, auth_policy) = sc_config_policy;

    let namespace = sc_config.namespace.clone();
    let ctx = K8Context::shared_metadata(sc_config);

    K8ClusterStateDispatcher::<SpuSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spus().clone(),
    );

    K8ClusterStateDispatcher::<TopicSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.topics().clone(),
    );

    K8ClusterStateDispatcher::<PartitionSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.partitions().clone(),
    );

    K8ClusterStateDispatcher::<SpuGroupSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spgs().clone(),
    );

    K8ClusterStateDispatcher::<TableFormatSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.tableformats().clone(),
    );

    K8ClusterStateDispatcher::<SmartModuleSpec, C>::start(
        namespace,
        metadata_client,
        ctx.smartmodules().clone(),
    );

    start_main_loop(ctx, auth_policy).await
}

/// start the main loop
pub async fn start_main_loop<
    C,
    SpuStore,
    PartitionStore,
    TopicStore,
    SpgStore,
    SmartModuleStore,
    TableFormatStore,
>(
    ctx: Arc<
        Context<
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >,
    >,
    auth_policy: Option<BasicRbacPolicy>,
) -> SharedContext<
    C,
    SpuStore,
    PartitionStore,
    TopicStore,
    SpgStore,
    SmartModuleStore,
    TableFormatStore,
>
where
    C: MetadataItem + 'static,
    C::UId: Send + Sync,
    SpuStore: Store<SpuSpec, C> + Sync + Send + 'static,
    PartitionStore: Store<PartitionSpec, C> + Sync + Send + 'static,
    TopicStore: Store<TopicSpec, C> + Sync + Send + 'static,
    SmartModuleStore: Store<SmartModuleSpec, C> + Sync + Send + 'static,
    SpgStore: Store<SpuGroupSpec, C> + Sync + Send + 'static,
    TableFormatStore: Store<TableFormatSpec, C> + Sync + Send + 'static,
{
    let config = ctx.config();
    whitelist!(config, "spu", SpuController::start(ctx.clone()));
    whitelist!(config, "topic", TopicController::start(ctx.clone()));
    whitelist!(
        config,
        "partition",
        PartitionController::<SpuStore, PartitionStore, C>::start(
            ctx.partitions().clone(),
            ctx.spus().clone()
        )
    );

    whitelist!(config, "internal", start_internal_server(ctx.clone()));
    whitelist!(
        config,
        "public",
        pub_server::start(ctx.clone(), auth_policy)
    );

    mod pub_server {
        use super::*;

        use std::sync::Arc;
        use tracing::info;

        use crate::services::start_public_server;
        use crate::core::SharedContext;

        use fluvio_controlplane_metadata::core::MetadataItem;
        use crate::services::auth::{AuthGlobalContext, RootAuthorization};
        use crate::services::auth::basic::{BasicAuthorization, BasicRbacPolicy};

        pub fn start<
            C: MetadataItem + 'static,
            SpuStore: Store<SpuSpec, C> + Sync + Send + 'static,
            PartitionStore: Store<PartitionSpec, C> + Sync + Send + 'static,
            TopicStore: Store<TopicSpec, C> + Sync + Send + 'static,
            SmartModuleStore: Store<SmartModuleSpec, C> + Sync + Send + 'static,
            SpgStore: Store<SpuGroupSpec, C> + Sync + Send + 'static,
            TableFormatStore: Store<TableFormatSpec, C> + Sync + Send + 'static,
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
            auth_policy_option: Option<BasicRbacPolicy>,
        ) where
            C::UId: Send + Sync,
        {
            if let Some(policy) = auth_policy_option {
                info!("using basic authorization");
                start_public_server(AuthGlobalContext::new(
                    ctx,
                    Arc::new(BasicAuthorization::new(policy)),
                ));
            } else {
                info!("using root authorization");
                start_public_server(AuthGlobalContext::new(
                    ctx,
                    Arc::new(RootAuthorization::new()),
                ));
            }
        }
    }

    ctx
}
