use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct Tick {
    pub value: f64,
    pub is_sub_tick: bool,
}

impl Into<Tick> for f64 {
    fn into(self) -> Tick {
        Tick { value: self, is_sub_tick: false }
    }
}

impl Tick {
    pub fn format(&self, precision: usize) -> String {
        if self.is_sub_tick {
            "".to_string()
        } else {
            // format!("{:+e}", self.value)
            format!("{:.precision$e}", self.value, precision = precision)
            // format!("{:.1}", self.value)
        }
    }
    pub fn compute_tick_spacing(range: f64, max_ticks: u32) -> f64 {
        if range == 0.0 {
            return 1.0;
        }

        assert!(range > 0.0);

        assert!(max_ticks > 0);

        let temp_step = range / max_ticks as f64;

        // get the magnitude of the step size
        let mag = temp_step.log10().floor();
        let mag_pow = 10.0f64.powf(mag);

        // calculate most significant digit of the new step size
        let mut mag_msd = (temp_step / mag_pow + 0.5).round();

        // promote the MSD to either 1, 2, or 5
        if mag_msd > 5.0 {
            mag_msd = 10.0;
        } else if mag_msd > 2.0 {
            mag_msd = 5.0;
        } else if mag_msd > 1.0 {
            mag_msd = 2.0;
        }

        mag_msd * mag_pow
    }

    pub fn linear_ticks(hint: usize, min_value: f64, max_value: f64) -> Vec<Tick> {
        if !min_value.is_finite() || !max_value.is_finite() {
            return Vec::new();
        }

        if max_value - min_value == 0.0 {
            return vec![Tick { value: min_value, is_sub_tick: false }];
        }
        assert!(min_value < max_value);

        let tick_spacing = Self::compute_tick_spacing(max_value - min_value, hint as u32);

        if !tick_spacing.is_finite() {
            return vec![];
        }

        if tick_spacing == 0.0 {
            return vec![Tick { value: (max_value - min_value) / 2.0, is_sub_tick: false }];
        }

        assert!(tick_spacing > 0.0);

        let first_tick_value = (min_value / tick_spacing).floor() * tick_spacing;

        let mut result = vec![Tick { value: first_tick_value, is_sub_tick: false }];

        for i in 1..hint {
            result.push(Tick { value: first_tick_value + i as f64 * tick_spacing, is_sub_tick: false });
        }

        result
    }

    pub fn log_sub_ticks(hint: usize, min_mag: i32, max_mag: i32, min_value: f64, max_value: f64, sign: i32) -> Vec<Tick> {
        let mut result = vec![];

        let base = 10.0f64;

        for mag in min_mag..=max_mag {
            let tick_value = base.powf(mag as f64) * sign as f64;

            if mag < max_mag {
                for i in 2..10 {
                    let sub_tick_value = tick_value * (i as f64);
                    if sub_tick_value >= min_value && sub_tick_value <= max_value {
                        result.push(Tick { value: sub_tick_value, is_sub_tick: true });
                    }
                }
            }
        }

        result.sort_by(|a, b| a.value.total_cmp(&b.value));

        result
    }

    pub fn log_main_ticks(hint: usize, min_mag: i32, max_mag: i32, min_value: f64, max_value: f64, sign: i32) -> Vec<Tick> {
        let mut result = vec![];

        let base = 10.0f64;
        let mag_range = max_mag - min_mag;
        let mut mags = vec![];
        if mag_range < 100 {
            mags.extend(min_mag..=max_mag);
        } else {
            let tick_value = base.powf(min_mag as f64) * sign as f64;
            let mi_mag = if tick_value >= min_value && tick_value <= max_value {
                min_mag
            } else {
                min_mag + 1
            };
            mags.push(mi_mag);

            let tick_value = base.powf(max_mag as f64) * sign as f64;
            let ma_mag = if tick_value >= min_value && tick_value <= max_value {
                max_mag
            } else {
                max_mag - 1
            };
            mags.push(ma_mag);

            mags.push((mi_mag + ma_mag) / 2);
        }

        for mag in mags {
            let tick_value = base.powf(mag as f64) * sign as f64;
            if tick_value >= min_value && tick_value <= max_value {
                result.push(Tick { value: tick_value, is_sub_tick: false });
            }
        }

        result.sort_by(|a, b| a.value.total_cmp(&b.value));

        result
    }
    pub fn log_ticks(hint: usize, min_mag: i32, max_mag: i32, min_value: f64, max_value: f64, sign: i32) -> Vec<Tick> {
        let mut result = Self::log_main_ticks(hint, min_mag, max_mag, min_value, max_value, sign);
        result.extend(Self::log_sub_ticks(hint, min_mag, max_mag, min_value, max_value, sign));

        result.sort_by(|a, b| a.value.total_cmp(&b.value));

        result
    }
    pub fn ticks(hint: usize, min_value: f64, max_value: f64, is_log: bool) -> Vec<Tick> {
        if !is_log {
            return Self::linear_ticks(hint, min_value, max_value);
        }

        if !min_value.is_finite() || !max_value.is_finite() {
            return Vec::new();
        }

        if (max_value - min_value) == 0.0 {
            return Self::linear_ticks(hint, min_value, max_value);
        }

        let base = 10.0f64;
        let base_ln = base.ln();

        if min_value > 0.0 {
            let mag_min = (min_value.ln() / base_ln).floor() as i32;
            let mag_max = (max_value.ln() / base_ln).ceil() as i32;

            if mag_max - mag_min < 2 {
                Self::linear_ticks(hint, min_value, max_value)
            } else {
                Self::log_ticks(hint, mag_min, mag_max, min_value, max_value, 1)
            }
        } else if min_value == 0.0 {
            let mag_max = (max_value.ln() / base_ln).ceil() as i32;
            let mag_min = 0;
            if mag_max < 2 {
                Self::linear_ticks(hint, min_value, max_value)
            } else {
                Self::log_ticks(hint, mag_min, mag_max, min_value, max_value, 1)
            }
        } else if max_value == 0.0 {
            let mag_min = ((-min_value).ln() / base_ln).ceil() as i32;
            if mag_min.abs() < 2 {
                Self::linear_ticks(hint, min_value, max_value)
            } else {
                Self::log_ticks(hint, 0, mag_min, min_value, max_value, -1)
            }
        } else if max_value < 0.0 {
            let mag_min = ((-min_value).ln() / base_ln).ceil() as i32;
            let mag_max = ((-max_value).ln() / base_ln).floor() as i32;

            assert!(mag_min >= mag_max);

            if mag_min - mag_max < 2 {
                Self::linear_ticks(hint, min_value, max_value)
            } else {
                Self::log_ticks(hint, mag_max, mag_min, min_value, max_value, -1)
            }
        } else {
            let mag_min = ((-min_value).ln() / base_ln).ceil() as i32;
            let mag_max = ((max_value).ln() / base_ln).ceil() as i32;

            if mag_min >= 2 || mag_max >= 2 {
                let mut result = vec![];
                

                result.extend(Self::log_sub_ticks(hint, 0, mag_min, min_value, max_value, -1));
                result.extend(Self::log_sub_ticks(hint, 0, mag_max, min_value, max_value, 1));


                let neg_ticks = Self::log_main_ticks(hint, 0, mag_min, min_value, max_value, -1);
                let pos_ticks = Self::log_main_ticks(hint, 0, mag_max, min_value, max_value, 1);

                // println!("neg_ticks -- {:?}", neg_ticks);
                // println!("pos_ticks -- {:?}", pos_ticks);

                let mut neg_ticks = VecDeque::from(neg_ticks);
                let mut pos_ticks = VecDeque::from(pos_ticks);

                let mut main_ticks = vec![];

                while main_ticks.len() < 6 && (!neg_ticks.is_empty() || !pos_ticks.is_empty()) {
                    if let Some(tick) = pos_ticks.pop_back() {
                        main_ticks.push(tick);
                    }
                    if main_ticks.len() == 6 {
                        break;
                    }
                    if let Some(tick) = neg_ticks.pop_front() {
                        main_ticks.push(tick);
                    }
                }

                result.extend(main_ticks);

                result.sort_by(|a, b| a.value.total_cmp(&b.value));

                //yield _compute_tick(
                //             0.0, transformedMinValue, transformedRange, codec.decoder);


                result
            } else {
                Self::linear_ticks(hint, min_value, max_value)
            }
        }
    }
}