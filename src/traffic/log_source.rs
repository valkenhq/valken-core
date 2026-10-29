//! [`LogSource`]: where a listener gets its log lines from.
//!
//! The production impl follows `pods/log?follow=true` through the kube
//! client; tests hand the listener a scripted stream of lines.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{AsyncBufReadExt, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::{Api, Client};

use crate::k8s::error::describe;
use crate::traffic::manager::PodTarget;
use crate::traffic::telemetry::{classify_stream_error, StreamError, StreamErrorKind};

/// Returns the current kube client, or `None` while disconnected.
pub type ClientProvider = Arc<dyn Fn() -> Option<Client> + Send + Sync>;

/// Lines of one container's log, as delivered with `timestamps=true`
/// (`<rfc3339> <content>`).  Ends when the stream is dropped server-side.
pub type LogLines = BoxStream<'static, io::Result<String>>;

/// Opens a follow stream for a pod's access log.
#[async_trait]
pub trait LogSource: Send + Sync + 'static {
    async fn open(&self, target: &PodTarget) -> Result<LogLines, StreamError>;
}

/// [`LogSource`] over the Kubernetes API.
pub struct KubeLogSource {
    client_provider: ClientProvider,
}

impl KubeLogSource {
    pub fn new(client_provider: ClientProvider) -> Self {
        Self { client_provider }
    }
}

#[async_trait]
impl LogSource for KubeLogSource {
    async fn open(&self, target: &PodTarget) -> Result<LogLines, StreamError> {
        let Some(client) = (self.client_provider)() else {
            return Err(StreamError {
                kind: StreamErrorKind::Other,
                message: "no kube client available".into(),
            });
        };
        let api: Api<Pod> = Api::namespaced(client, &target.namespace);
        let params = LogParams {
            follow: true,
            container: Some(target.container.clone()),
            // Never replay history: buckets already flushed would double
            // count.  Lines written while a stream is down (reconnect
            // backoff) are therefore lost; that is the accepted trade.
            tail_lines: Some(0),
            timestamps: true,
            ..Default::default()
        };
        match api.log_stream(&target.name, &params).await {
            Ok(reader) => Ok(reader.lines().boxed()),
            Err(e) => Err(StreamError {
                kind: classify_stream_error(&e),
                message: describe(&e),
            }),
        }
    }
}
