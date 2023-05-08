use async_trait::async_trait;
use std::{fmt::Debug, marker::Send};
use tokio::sync::{mpsc, oneshot};
use tracing::instrument;

use crate::{Client, Connection, MessageClient, TransportError};

pub struct ChannelClient<Req, Res> {
    server: mpsc::Sender<(Req, oneshot::Sender<Res>)>,
}

impl<Req, Res> ChannelClient<Req, Res>
where
    Req: Debug,
    Res: Debug,
{
    pub fn new(server: mpsc::Sender<(Req, oneshot::Sender<Res>)>) -> Self {
        Self { server }
    }
}

impl<Req, Res> Clone for ChannelClient<Req, Res> {
    fn clone(&self) -> ChannelClient<Req, Res> {
        ChannelClient {
            server: self.server.clone(),
        }
    }
}

#[async_trait]
impl<Req, Res> Client<Req, Res> for ChannelClient<Req, Res>
where
    Req: Send + Sync + Debug + 'static,
    Res: Send + Sync + Debug + 'static,
{
    async fn connection(&self) -> Result<Box<dyn Connection<Req, Res>>, TransportError> {
        Ok(Box::new(ChannelClient::new(self.server.clone())))
    }
    fn box_clone(&self) -> Box<dyn Client<Req, Res>> {
        Box::new(Self {
            server: self.server.clone(),
        })
    }
}

#[async_trait]
impl<Req, Res> Connection<Req, Res> for ChannelClient<Req, Res>
where
    Req: Send + Sync + Debug + 'static,
    Res: Send + Sync + Debug + 'static,
{
    async fn stream(&self) -> Result<Box<dyn MessageClient<Req, Res>>, TransportError> {
        Ok(Box::new(ChannelClient::new(self.server.clone())))
    }
    fn box_clone(&self) -> Box<dyn Connection<Req, Res>> {
        Box::new(Self {
            server: self.server.clone(),
        })
    }
}

#[async_trait]
impl<Req, Res> MessageClient<Req, Res> for ChannelClient<Req, Res>
where
    Req: Debug + Send + 'static,
    Res: Debug + Send + 'static,
{
    #[instrument(skip(self), fields(message_client = "Channel"))]
    async fn request(&mut self, req: Req) -> Result<Res, TransportError> {
        let (tx, rx) = oneshot::channel();
        self.server
            .send((req, tx))
            .await
            .expect("channel should be open");
        let result = rx.await.expect("channel should be open");

        Ok(result)
    }

    fn box_clone(&self) -> Box<dyn MessageClient<Req, Res>> {
        Box::new(ChannelClient {
            server: self.server.clone(),
        })
    }
}

impl<Req, Res> Debug for ChannelClient<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelClient").finish()
    }
}
