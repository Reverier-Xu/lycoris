use std::collections::VecDeque;

const DEFAULT_WINDOW_SIZE: usize = 1_000;
const MIN_SAMPLES: usize = 2;
const MAX_PHI: f64 = 30.0;
const MIN_TAIL: f64 = 1e-15;

/// Coefficients for the Abramowitz and Stegun erf approximation.
const ERF_P: f64 = 0.3275911;
const ERF_A1: f64 = 0.254829592;
const ERF_A2: f64 = -0.284496736;
const ERF_A3: f64 = 1.421413741;
const ERF_A4: f64 = -1.453152027;
const ERF_A5: f64 = 1.061405429;

/// A Phi accrual failure detector.
///
/// The detector maintains a sliding window of inter-arrival times between
/// heartbeats. From this distribution it estimates the probability that the
/// next heartbeat is still "on time". The result is expressed as `phi`, the
/// negative log10 of that probability. Higher phi means higher confidence that
/// the peer has failed.
///
/// Timestamps are plain `u64` millisecond values. Callers using `std::time`
/// should convert `Instant`/`SystemTime` deltas to milliseconds before calling
/// this detector.
#[derive(Debug, Clone)]
pub struct PhiAccrualDetector {
  max_window_size: usize,
  intervals_ms: VecDeque<u64>,
  last_heartbeat_ms: Option<u64>,
}

impl PhiAccrualDetector {
  /// Create a detector with the default window size.
  pub fn new() -> Self {
    Self::with_window(DEFAULT_WINDOW_SIZE)
  }

  /// Create a detector with a custom window size.
  pub fn with_window(window_size: usize) -> Self {
    Self {
      max_window_size: window_size,
      intervals_ms: VecDeque::new(),
      last_heartbeat_ms: None,
    }
  }

  /// Record a heartbeat arrival at `now_ms`.
  pub fn heartbeat(&mut self, now_ms: u64) {
    if let Some(last) = self.last_heartbeat_ms
      && now_ms > last
    {
      self.intervals_ms.push_back(now_ms - last);
      while self.intervals_ms.len() > self.max_window_size {
        self.intervals_ms.pop_front();
      }
    }
    self.last_heartbeat_ms = Some(now_ms);
  }

  /// Return the current suspicion level at `now_ms`.
  ///
  /// A value of `0.0` means either insufficient history or the peer is clearly
  /// alive. Values above a configured threshold (commonly `8.0` or `12.0`)
  /// indicate that the peer should be suspected.
  pub fn phi(&self, now_ms: u64) -> f64 {
    let last = match self.last_heartbeat_ms {
      Some(last) => last,
      None => return 0.0,
    };

    if now_ms < last || self.intervals_ms.len() < MIN_SAMPLES {
      return 0.0;
    }

    let elapsed = (now_ms - last) as f64;
    let mean = mean(&self.intervals_ms);
    let variance = variance(&self.intervals_ms, mean);

    if variance < 1e-12 {
      return if elapsed <= mean { 0.0 } else { MAX_PHI };
    }

    let stddev = variance.sqrt();
    let tail = complementary_cdf(elapsed, mean, stddev);
    let clamped = tail.clamp(MIN_TAIL, 1.0);
    (-clamped.log10()).clamp(0.0, MAX_PHI)
  }

  /// Return the timestamp of the last recorded heartbeat, if any.
  pub fn last_heartbeat(&self) -> Option<u64> {
    self.last_heartbeat_ms
  }

  /// Return the number of stored inter-arrival samples.
  pub fn sample_count(&self) -> usize {
    self.intervals_ms.len()
  }
}

impl Default for PhiAccrualDetector {
  fn default() -> Self {
    Self::new()
  }
}

fn mean(intervals: &VecDeque<u64>) -> f64 {
  let n = intervals.len() as f64;
  intervals.iter().map(|&x| x as f64).sum::<f64>() / n
}

fn variance(intervals: &VecDeque<u64>, mean: f64) -> f64 {
  let n = intervals.len() as f64;
  intervals
    .iter()
    .map(|&x| {
      let d = x as f64 - mean;
      d * d
    })
    .sum::<f64>()
    / n
}

/// Tail probability `P(X > t)` for a normal distribution with the given mean
/// and standard deviation.
fn complementary_cdf(t: f64, mean: f64, stddev: f64) -> f64 {
  let z = (t - mean) / (stddev * std::f64::consts::SQRT_2);
  0.5 * erfc(z)
}

/// Complementary error function approximation.
fn erfc(x: f64) -> f64 {
  let z = x.abs();
  let t = 1.0 / (1.0 + ERF_P * z);
  let poly = t * (ERF_A1 + t * (ERF_A2 + t * (ERF_A3 + t * (ERF_A4 + t * ERF_A5))));
  let r = poly * (-z * z).exp();
  if x >= 0.0 { r } else { 2.0 - r }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn phi_is_zero_without_history() {
    let detector = PhiAccrualDetector::new();
    assert_eq!(detector.phi(1_000), 0.0);
  }

  #[test]
  fn phi_is_zero_with_insufficient_samples() {
    let mut detector = PhiAccrualDetector::new();
    detector.heartbeat(100);
    detector.heartbeat(200);
    assert_eq!(detector.sample_count(), 1);
    assert_eq!(detector.phi(300), 0.0);
  }

  #[test]
  fn phi_low_for_regular_intervals() {
    let mut detector = PhiAccrualDetector::with_window(10);
    let base = 0;
    for i in 1..=12 {
      detector.heartbeat(base + i * 100);
    }
    assert!(detector.phi(1_200) < 1.0);
    assert!(detector.phi(1_250) < 3.0);
  }

  #[test]
  fn phi_grows_after_long_silence() {
    let mut detector = PhiAccrualDetector::with_window(10);
    let base = 0;
    for i in 1..=12 {
      detector.heartbeat(base + i * 100);
    }
    assert!(detector.phi(2_200) > 8.0);
  }

  #[test]
  fn phi_resets_after_heartbeat() {
    let mut detector = PhiAccrualDetector::with_window(10);
    let base = 0;
    for i in 1..=12 {
      detector.heartbeat(base + i * 100);
    }
    assert!(detector.phi(2_200) > 8.0);
    detector.heartbeat(2_300);
    assert!(detector.phi(2_310) < 1.0);
  }

  #[test]
  fn phi_higher_for_irregular_pattern() {
    let mut regular = PhiAccrualDetector::with_window(10);
    let mut irregular = PhiAccrualDetector::with_window(10);
    let base = 0;

    for i in 1..=12 {
      regular.heartbeat(base + i * 100);
      let jitter = if i % 2 == 0 { 20 } else { 0 };
      irregular.heartbeat(base + i * 100 + jitter);
    }

    let elapsed = 250;
    assert!(irregular.phi(elapsed) >= regular.phi(elapsed));
  }
}
