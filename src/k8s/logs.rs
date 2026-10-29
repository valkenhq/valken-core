//! Container logs on demand (the Logs window), one stream per container.
//!
//! Lines are requested with `timestamps=true` and come back with the
//! server's timestamp parsed, so a caller can merge several pods' streams
//! in time order.

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{AsyncBufReadExt, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::LogParams;
use kube::{Api, Client};

use crate::k8s::error::LogError;

/// What to read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LogRequest {
    pub namespace: String,
    pub pod: String,
    pub container: String,
    /// Start this far back (`None`: the whole current log).
    pub since_seconds: Option<i64>,
    /// The previous (terminated) container instead of the running one.
    pub previous: bool,
    /// Keep the stream open for new lines.
    pub follow: bool,
    /// At most this many lines of history.
    pub tail_lines: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    pub ts: Option<DateTime<Utc>>,
    pub text: String,
}

/// `2026-10-21T14:01:52.104Z message` → timestamp + message.  Lines
/// without a leading timestamp keep their whole text.
pub fn parse_line(raw: &str) -> LogLine {
    let raw = raw.strip_suffix('\r').unwrap_or(raw);
    if let Some((head, rest)) = raw.split_once(' ') {
        if let Ok(ts) = DateTime::parse_from_rfc3339(head) {
            return LogLine {
                ts: Some(ts.with_timezone(&Utc)),
                text: rest.to_string(),
            };
        }
    }
    LogLine {
        ts: None,
        text: raw.to_string(),
    }
}

pub type LogStream = BoxStream<'static, Result<LogLine, LogError>>;

fn map_log_error(e: kube::Error, previous: bool) -> LogError {
    match &e {
        kube::Error::Api(s)
            if s.code == 400 && previous && s.message.contains("previous terminated container") =>
        {
            LogError::NoPrevious
        }
        kube::Error::Api(s) if s.code == 404 => LogError::NotFound,
        kube::Error::Api(s) if s.code == 403 => LogError::Forbidden(s.message.clone()),
        _ => LogError::Request(e),
    }
}

/// Open one container's log.  Ends when the container (or the request,
/// without `follow`) does.
#[tracing::instrument(name = "k8s.logs.open", skip(client), fields(pod = %req.pod, container = %req.container, previous = req.previous, follow = req.follow))]
pub async fn open_logs(client: &Client, req: &LogRequest) -> Result<LogStream, LogError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), &req.namespace);
    let params = LogParams {
        container: Some(req.container.clone()),
        follow: req.follow,
        previous: req.previous,
        since_seconds: req.since_seconds,
        tail_lines: req.tail_lines,
        timestamps: true,
        ..Default::default()
    };
    let reader = api
        .log_stream(&req.pod, &params)
        .await
        .map_err(|e| map_log_error(e, req.previous))?;
    Ok(reader
        .lines()
        .map(|l| l.map(|raw| parse_line(&raw)).map_err(LogError::Io))
        .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_errors_map_to_variants() {
        let api = |code: u16, msg: &str| {
            kube::Error::Api(
                kube::core::Status::failure(msg, "x")
                    .with_code(code)
                    .boxed(),
            )
        };
        let no_prev = "previous terminated container \"c\" in pod \"p\" not found";
        assert!(matches!(
            map_log_error(api(400, no_prev), true),
            LogError::NoPrevious
        ));
        assert!(
            matches!(
                map_log_error(api(400, no_prev), false),
                LogError::Request(_)
            ),
            "400 without previous=true is not NoPrevious"
        );
        assert!(matches!(
            map_log_error(api(404, "nf"), false),
            LogError::NotFound
        ));
        assert!(
            matches!(map_log_error(api(403, "nope"), false), LogError::Forbidden(m) if m == "nope")
        );
        assert!(matches!(
            map_log_error(api(500, "boom"), false),
            LogError::Request(_)
        ));
    }

    #[test]
    fn timestamps_are_split_off() {
        let l = parse_line("2026-10-21T14:01:52.104000000Z GET /api 200");
        assert_eq!(l.text, "GET /api 200");
        assert_eq!(l.ts.unwrap().timestamp_millis() % 1000, 104);
        assert_eq!(
            parse_line("no timestamp here"),
            LogLine {
                ts: None,
                text: "no timestamp here".into()
            }
        );
        assert_eq!(parse_line("2026-10-21T14:01:52Z \r").text, "");
        assert_eq!(parse_line("").text, "");
    }
}
