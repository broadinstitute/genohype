use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const LOG_THRESHOLD: f64 = 10.0;
pub const LINEAR_FRACTION: f64 = 0.6;
pub const MAX_NEG_LOG_P: f64 = 350.0;

/// Canonical hybrid linear/logarithmic Manhattan y scale.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct YScale {
    #[schemars(range(min = 1, max = 4294967295_u32))]
    pub height: u32,
    #[schemars(range(min = 10.0, max = 10.0))]
    pub log_threshold: f64,
    #[schemars(range(min = 0.6, max = 0.6))]
    pub linear_fraction: f64,
    #[schemars(range(min = 350.0, max = 350.0))]
    pub max_neg_log_p: f64,
}

impl YScale {
    pub const fn new(height: u32) -> Self {
        Self {
            height,
            log_threshold: LOG_THRESHOLD,
            linear_fraction: LINEAR_FRACTION,
            max_neg_log_p: MAX_NEG_LOG_P,
        }
    }

    /// Map effective significance to raster y, with zero at the image top.
    pub fn y_for(&self, value: f64) -> f32 {
        let value = if value.is_nan() {
            0.0
        } else {
            value.clamp(0.0, self.max_neg_log_p)
        };
        let height = f64::from(self.height);
        let linear_height = height * self.linear_fraction;
        if value <= self.log_threshold {
            return (height - value / self.log_threshold * linear_height) as f32;
        }

        let log_height = height * (1.0 - self.linear_fraction);
        let normalized =
            (value / self.log_threshold).ln() / (self.max_neg_log_p / self.log_threshold).ln();
        (height - linear_height - normalized * log_height).max(0.0) as f32
    }

    pub fn threshold_y(&self, pvalue: f64) -> Option<f32> {
        if pvalue.is_finite() && pvalue > 0.0 && pvalue <= 1.0 {
            Some(self.y_for(-pvalue.log10()))
        } else {
            None
        }
    }
}

impl Default for YScale {
    fn default() -> Self {
        Self::new(800)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn canonical_boundaries_are_exact() {
        let scale = YScale::new(1_000);
        assert_eq!(scale.y_for(0.0), 1_000.0);
        assert_eq!(scale.y_for(LOG_THRESHOLD), 400.0);
        assert_eq!(scale.y_for(MAX_NEG_LOG_P), 0.0);
        assert_eq!(scale.y_for(999.0), 0.0);
        assert_eq!(scale.threshold_y(1e-10), Some(400.0));
        assert_eq!(scale.threshold_y(0.0), None);
    }

    proptest! {
        #[test]
        fn scale_is_monotonic(a in 0.0_f64..=MAX_NEG_LOG_P, b in 0.0_f64..=MAX_NEG_LOG_P) {
            let scale = YScale::new(913);
            if a <= b {
                prop_assert!(scale.y_for(a) >= scale.y_for(b));
            } else {
                prop_assert!(scale.y_for(a) <= scale.y_for(b));
            }
        }
    }
}
