//! Utility functions for working with Kubernetes resource timestamps.

use chrono::{DateTime, Duration, Utc};
use tracing::error;

#[derive(Debug, PartialEq, Eq)]
pub enum DurationFormat {
    Seconds,
    Minutes,
    Hours,
    Days,
}

/// Converts a Kubernetes resource time to a UTC DateTime.
/// Returns None and logs an error if the timestamp is invalid.
pub fn k8s_time_to_datetime_utc(
    time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
) -> Option<DateTime<Utc>> {
    // k8s-openapi 0.27+ wraps jiff::Timestamp; convert via unix seconds.
    let seconds = time.0.as_second();
    let nanos = time.0.subsec_nanosecond();
    let dt = DateTime::from_timestamp(seconds, nanos as u32);
    if dt.is_none() {
        error!(
            seconds,
            nanos, "Failed to convert Kubernetes timestamp to DateTime (invalid upstream data)"
        );
    }
    dt
}

/// Converts a duration to a human-readable age (`"3d"`, `"2h"`, `"5m"`,
/// `"12s"`) and the unit used.  Negative durations (clock skew) read as `0s`.
pub fn duration_to_age_string(duration: Duration) -> (String, DurationFormat) {
    let seconds = duration.num_seconds().max(0);
    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;

    if days > 0 {
        (format!("{}d", days), DurationFormat::Days)
    } else if hours > 0 {
        (format!("{}h", hours), DurationFormat::Hours)
    } else if minutes > 0 {
        (format!("{}m", minutes), DurationFormat::Minutes)
    } else {
        (format!("{}s", seconds), DurationFormat::Seconds)
    }
}

/// Age of a resource created at `creation_time`, measured against `now`.
pub fn k8s_creation_time_to_age_string_at(
    creation_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
    now: DateTime<Utc>,
) -> (String, DurationFormat) {
    match k8s_time_to_datetime_utc(creation_time) {
        Some(created) => duration_to_age_string(now.signed_duration_since(created)),
        None => ("unknown".to_string(), DurationFormat::Seconds),
    }
}

/// Converts a Kubernetes resource creation time to a human-readable age
/// string relative to now.  If the timestamp is invalid, returns `"unknown"`.
pub fn k8s_creation_time_to_age_string(
    creation_time: k8s_openapi::apimachinery::pkg::apis::meta::v1::Time,
) -> (String, DurationFormat) {
    k8s_creation_time_to_age_string_at(creation_time, Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use k8s_openapi::jiff::Timestamp;

    fn at(unix: i64) -> Time {
        Time(Timestamp::from_second(unix).unwrap())
    }

    #[test]
    fn ages_pick_the_largest_unit() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let cases = [
            (3600, "1h", DurationFormat::Hours),
            (120, "2m", DurationFormat::Minutes),
            (5, "5s", DurationFormat::Seconds),
            (2 * 86400, "2d", DurationFormat::Days),
            (0, "0s", DurationFormat::Seconds),
        ];
        for (ago, text, unit) in cases {
            let got = k8s_creation_time_to_age_string_at(at(1_800_000_000 - ago), now);
            assert_eq!(got, (text.to_string(), unit));
        }
    }

    #[test]
    fn future_timestamps_from_clock_skew_read_as_zero() {
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        assert_eq!(
            k8s_creation_time_to_age_string_at(at(1_800_000_000 + 90), now),
            ("0s".to_string(), DurationFormat::Seconds)
        );
    }

    #[test]
    fn conversion_keeps_sub_second_precision() {
        let ts = Timestamp::from_second(1_800_000_000).unwrap()
            + k8s_openapi::jiff::SignedDuration::from_nanos(250);
        let dt = k8s_time_to_datetime_utc(Time(ts)).unwrap();
        assert_eq!(dt.timestamp(), 1_800_000_000);
        assert_eq!(dt.timestamp_subsec_nanos(), 250);
    }

    #[test]
    fn whole_jiff_range_converts() {
        // chrono's range (±262143 years) covers jiff's (±9999), so the
        // `None` branch in `k8s_time_to_datetime_utc` is purely defensive.
        assert!(k8s_time_to_datetime_utc(Time(Timestamp::MAX)).is_some());
        assert!(k8s_time_to_datetime_utc(Time(Timestamp::MIN)).is_some());
        let (age, unit) = k8s_creation_time_to_age_string(Time(Timestamp::MIN));
        assert!(age.ends_with('d'));
        assert_eq!(unit, DurationFormat::Days);
    }
}
