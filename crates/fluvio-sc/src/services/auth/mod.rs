pub mod basic;

pub use common::*;

mod common {

    use std::{sync::Arc, marker::PhantomData};
    use std::fmt::Debug;

    use async_trait::async_trait;

    use fluvio_auth::{AuthContext, Authorization, TypeAction, InstanceAction, AuthError};
    use fluvio_socket::FluvioSocket;
    use fluvio_controlplane_metadata::{
        extended::ObjectType, spu::SpuSpec, partition::PartitionSpec, topic::TopicSpec,
        tableformat::TableFormatSpec, smartmodule::SmartModuleSpec, spg::SpuGroupSpec,
    };
    use fluvio_stream_model::core::MetadataItem;

    use crate::{core::SharedContext, stores::Store};

    /// SC global context with authorization
    /// auth is trait object which contains global auth auth policy
    #[derive(Clone, Debug)]
    pub struct AuthGlobalContext<
        A,
        C: MetadataItem,
        SpuStore: Store<SpuSpec, C>,
        PartitionStore: Store<PartitionSpec, C>,
        TopicStore: Store<TopicSpec, C>,
        SpgStore: Store<SpuGroupSpec, C>,
        SmartModuleStore: Store<SmartModuleSpec, C>,
        TableFormatStore: Store<TableFormatSpec, C>,
    > {
        pub global_ctx: SharedContext<
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >,
        pub auth: Arc<A>,
        pub phantom: std::marker::PhantomData<C>,
    }

    impl<
            A,
            C: MetadataItem,
            SpuStore: Store<SpuSpec, C>,
            PartitionStore: Store<PartitionSpec, C>,
            TopicStore: Store<TopicSpec, C>,
            SpgStore: Store<SpuGroupSpec, C>,
            SmartModuleStore: Store<SmartModuleSpec, C>,
            TableFormatStore: Store<TableFormatSpec, C>,
        >
        AuthGlobalContext<
            A,
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >
    {
        pub fn new(
            global_ctx: SharedContext<
                C,
                SpuStore,
                PartitionStore,
                TopicStore,
                SpgStore,
                SmartModuleStore,
                TableFormatStore,
            >,
            auth: Arc<A>,
        ) -> Self {
            Self {
                global_ctx,
                auth,
                phantom: PhantomData,
            }
        }
    }

    /// Authorization that allows anything
    /// Used for personal development
    #[derive(Debug, Clone)]
    pub struct RootAuthorization {}

    #[async_trait]
    impl Authorization for RootAuthorization {
        type Context = RootAuthContext;

        async fn create_auth_context(
            &self,
            _socket: &mut FluvioSocket,
        ) -> Result<Self::Context, AuthError> {
            Ok(RootAuthContext {})
        }
    }

    impl RootAuthorization {
        pub fn new() -> Self {
            Self {}
        }
    }

    #[derive(Debug)]
    pub struct RootAuthContext {}

    #[async_trait]
    impl AuthContext for RootAuthContext {
        async fn allow_type_action(
            &self,
            _ty: ObjectType,
            _action: TypeAction,
        ) -> Result<bool, AuthError> {
            Ok(true)
        }

        /// check if specific instance of spec can be deleted
        async fn allow_instance_action(
            &self,
            _ty: ObjectType,
            _action: InstanceAction,
            _key: &str,
        ) -> Result<bool, AuthError> {
            Ok(true)
        }
    }

    /// Auth Service Context, this hold individual context that is enough enforce auth
    /// for this service context
    #[derive(Debug, Clone)]
    pub struct AuthServiceContext<
        AC,
        C: MetadataItem,
        SpuStore: Store<SpuSpec, C>,
        PartitionStore: Store<PartitionSpec, C>,
        TopicStore: Store<TopicSpec, C>,
        SpgStore: Store<SpuGroupSpec, C>,
        SmartModuleStore: Store<SmartModuleSpec, C>,
        TableFormatStore: Store<TableFormatSpec, C>,
    > {
        pub global_ctx: SharedContext<
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >,
        pub auth: AC,
        pub phantom: std::marker::PhantomData<C>,
    }

    impl<
            AC,
            C: MetadataItem,
            SpuStore: Store<SpuSpec, C>,
            PartitionStore: Store<PartitionSpec, C>,
            TopicStore: Store<TopicSpec, C>,
            SpgStore: Store<SpuGroupSpec, C>,
            SmartModuleStore: Store<SmartModuleSpec, C>,
            TableFormatStore: Store<TableFormatSpec, C>,
        >
        AuthServiceContext<
            AC,
            C,
            SpuStore,
            PartitionStore,
            TopicStore,
            SpgStore,
            SmartModuleStore,
            TableFormatStore,
        >
    {
        pub fn new(
            global_ctx: SharedContext<
                C,
                SpuStore,
                PartitionStore,
                TopicStore,
                SpgStore,
                SmartModuleStore,
                TableFormatStore,
            >,
            auth: AC,
        ) -> Self {
            Self {
                global_ctx,
                auth,
                phantom: PhantomData,
            }
        }
    }
}
