//! The sparklines' data: a fixed-size, in-memory ring per series.
//!
//! There is no metrics store behind this.  History starts empty when the
//! watch session starts and is gone when it stops; at the default 30 s
//! sample interval 120 points cover the last hour.

use std::collections::VecDeque;

/// Default number of points kept per series (1 h at 30 s).
pub const DEFAULT_HISTORY_LEN: usize = 120;

/// One series of samples, oldest first.  `NaN` marks a sample with no
/// value (no metrics-server at that moment, no API requests in a slot), so
/// gaps stay gaps instead of being drawn as zero.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    points: VecDeque<f32>,
    cap: usize,
}

impl Series {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            points: VecDeque::with_capacity(cap),
            cap: cap.max(1),
        }
    }

    /// Append, dropping the oldest point when full.
    pub fn push(&mut self, value: Option<f32>) {
        if self.points.len() == self.cap {
            self.points.pop_front();
        }
        self.points.push_back(value.unwrap_or(f32::NAN));
    }

    /// Points oldest first; gaps are `None`.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Option<f32>> + '_ {
        self.points.iter().map(|v| (!v.is_nan()).then_some(*v))
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// The newest value, if it isn't a gap.
    pub fn last(&self) -> Option<f32> {
        self.points.back().copied().filter(|v| !v.is_nan())
    }

    /// Smallest and largest value, ignoring gaps (for scaling a sparkline).
    pub fn range(&self) -> Option<(f32, f32)> {
        self.iter().flatten().fold(None, |acc, v| match acc {
            None => Some((v, v)),
            Some((lo, hi)) => Some((lo.min(v), hi.max(v))),
        })
    }
}

impl Default for Series {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_HISTORY_LEN)
    }
}

/// The four KPI series, sampled together.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct History {
    /// Cluster CPU in percent of allocatable: usage when the metrics-server
    /// answered, else requests (see [`super::Totals::basis`]).
    pub cpu_percent: Series,
    pub memory_percent: Series,
    /// Running pods.
    pub pods_running: Series,
    /// API server p95 in milliseconds over the last sample interval.
    pub api_p95_ms: Series,
}

impl History {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            cpu_percent: Series::with_capacity(cap),
            memory_percent: Series::with_capacity(cap),
            pods_running: Series::with_capacity(cap),
            api_p95_ms: Series::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.cpu_percent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cpu_percent.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_drops_the_oldest_when_full() {
        let mut s = Series::with_capacity(3);
        for v in 1..=5 {
            s.push(Some(v as f32));
        }
        assert_eq!(s.len(), 3);
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            vec![Some(3.0), Some(4.0), Some(5.0)]
        );
        assert_eq!(s.last(), Some(5.0));
    }

    #[test]
    fn gaps_stay_gaps_and_are_ignored_by_range() {
        let mut s = Series::with_capacity(4);
        assert_eq!(s.range(), None);
        s.push(Some(2.0));
        s.push(None);
        s.push(Some(8.0));
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            vec![Some(2.0), None, Some(8.0)]
        );
        assert_eq!(s.range(), Some((2.0, 8.0)));
        s.push(None);
        assert_eq!(s.last(), None);
    }

    #[test]
    fn zero_capacity_still_holds_one_point() {
        let mut s = Series::with_capacity(0);
        s.push(Some(1.0));
        s.push(Some(2.0));
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![Some(2.0)]);
    }
}
