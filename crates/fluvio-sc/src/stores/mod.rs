pub mod spu;
pub mod topic;
pub mod partition;
pub mod spg;
pub mod smartmodule;
pub mod tableformat;

use std::sync::Arc;

use fluvio_stream_dispatcher::actions::WSAction;
use fluvio_stream_model::core::MetadataItem;

pub use crate::dispatcher::store::*;

pub mod actions {
    pub use crate::dispatcher::actions::*;
}

#[async_trait::async_trait]
pub trait Store<T: fluvio_stream_dispatcher::core::Spec, C: MetadataItem>:
    Clone + std::fmt::Debug
{
    fn new() -> Self;
    fn store(&self) -> Arc<LocalStore<T, C>>;
    async fn delete(&self, name: T::IndexKey) -> std::io::Result<()>;
    async fn create_spec(
        &self,
        name: <T as fluvio_stream_model::core::Spec>::IndexKey,
        spec: T,
    ) -> std::io::Result<MetadataStoreObject<T, C>>;

    fn change_listener(&self) -> ChangeListener<T, C>;

    async fn send_action(&self, action: WSAction<T, C>);
    async fn wait_action_with_timeout(
        &self,
        key: &T::IndexKey,
        action: WSAction<T, C>,
        duration: std::time::Duration,
    ) -> std::io::Result<()>;
    async fn update_status(&self, name: T::IndexKey, status: T::Status) -> std::io::Result<()>;
}

#[async_trait::async_trait]
impl<T: fluvio_stream_dispatcher::core::Spec, C: MetadataItem> Store<T, C> for StoreContext<T, C>
where
    T::IndexKey: std::fmt::Display + Send + Sync,
    T::Status: Send + Sync,
    T: Send + Sync,
{
    fn new() -> Self {
        Self::new()
    }

    fn store(&self) -> Arc<LocalStore<T, C>> {
        self.store().clone()
    }

    async fn delete(
        &self,
        name: <T as fluvio_stream_model::core::Spec>::IndexKey,
    ) -> std::io::Result<()> {
        self.delete(name).await?;
        Ok(())
    }

    async fn create_spec(
        &self,
        name: <T as fluvio_stream_model::core::Spec>::IndexKey,
        spec: T,
    ) -> std::io::Result<MetadataStoreObject<T, C>> {
        let result = self.create_spec(name, spec).await?;
        Ok(result)
    }

    fn change_listener(&self) -> ChangeListener<T, C> {
        self.change_listener()
    }

    async fn send_action(&self, action: WSAction<T, C>) {
        self.send_action(action).await
    }

    async fn wait_action_with_timeout(
        &self,
        key: &T::IndexKey,
        action: WSAction<T, C>,
        duration: std::time::Duration,
    ) -> std::io::Result<()> {
        self.wait_action_with_timeout(key, action, duration).await?;
        Ok(())
    }

    async fn update_status(&self, name: T::IndexKey, status: T::Status) -> std::io::Result<()> {
        self.update_status(name, status).await?;
        Ok(())
    }
}
