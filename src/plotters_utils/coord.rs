use std::error::Error;
use std::ops::Range;
use plotters::coord::ranged1d::{AsRangedCoord, DefaultFormatting, KeyPointHint};
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::LogScalable;
use crate::client::TercenError;
use crate::processors::PreProcess;
use plotters::prelude::*;
use crate::ticks::Tick;

#[derive(Clone)]
pub struct PreProcessorsCoord {
    linear: RangedCoordf64,
    logic: Range<f64>,
    normalized: Range<f64>,
    zero_point: f64,
    negative: bool,
    pre_processors: Vec<PreProcess>,
}


#[derive(Default)]
pub struct PreProcessorsRange {
    pub(crate) range: Range<f64>,
    zero: f64,
    pre_processors: Vec<PreProcess>,
}

impl PreProcessorsRange {
    pub(crate) fn from_range(pre_processs: Vec<PreProcess>, x_range: Range<f64>) -> Result<Self, Box<dyn Error>> {
        if !x_range.start.is_finite() {
            return Err(TercenError::new("x_range.start.is_finite").into());
        }
        if !x_range.end.is_finite() {
            return Err(TercenError::new("x_range.start.is_finite").into());
        }
        let min_encoded = PreProcess::encode_pre_processors(&pre_processs, x_range.start);
        let max_encoded = PreProcess::encode_pre_processors(&pre_processs, x_range.end);
        if !min_encoded.is_finite() {
            return Err(TercenError::new("x_range.start.is_finite").into());
        }
        if !max_encoded.is_finite() {
            return Err(TercenError::new("x_range.start.is_finite").into());
        }

        Ok(PreProcessorsRange::from_encoded_range(pre_processs, min_encoded..max_encoded))
    }

    fn from_encoded_range(pre_processors: Vec<PreProcess>, range: Range<f64>) -> Self {
        PreProcessorsRange { range, zero: 0.0, pre_processors }
    }

    fn encode(&self, fv: f64) -> f64 {
        PreProcess::encode_pre_processors(&self.pre_processors, fv)
    }

    fn decode(&self, fv: f64) -> f64 {
        PreProcess::decode_pre_processors(&self.pre_processors, fv)
    }
}

impl AsRangedCoord for PreProcessorsRange {
    type CoordDescType = PreProcessorsCoord;
    type Value = f64;
}

impl From<PreProcessorsRange> for PreProcessorsCoord {
    fn from(spec: PreProcessorsRange) -> Self {
        let zero_point = spec.zero;
        let mut start = spec.range.start.as_f64() - zero_point;
        let mut end = spec.range.end.as_f64() - zero_point;
        let negative = false;


        PreProcessorsCoord {
            linear: (spec.decode(start)..spec.decode(end)).into(),
            logic: spec.range,
            normalized: start..end,
            pre_processors: spec.pre_processors,
            zero_point,
            negative,
        }
    }
}


impl PreProcessorsCoord {
    fn is_log(&self) -> bool {
        self.pre_processors.iter()
            .any(|pre_processor| pre_processor.is_log())
    }

    fn encode(&self, v: f64) -> f64 {
        PreProcess::encode_pre_processors(&self.pre_processors, v)
    }

    fn decode(&self, v: f64) -> f64 {
        PreProcess::decode_pre_processors(&self.pre_processors, v)
    }
    fn value_to_f64(&self, value: &f64) -> f64 {
        let fv = value.as_f64() - self.zero_point;
        if self.negative {
            -fv
        } else {
            fv
        }
    }

    fn f64_to_value(&self, fv: f64) -> f64 {
        let fv = if self.negative { -fv } else { fv };
        f64::from_f64(fv + self.zero_point)
    }

    fn is_inf(&self, fv: f64) -> bool {
        let fv = if self.negative { -fv } else { fv };
        let a = f64::from_f64(fv + self.zero_point);
        let b = f64::from_f64(self.zero_point);

        (f64::as_f64(&a) - f64::as_f64(&b)).abs() < f64::EPSILON
    }
}
 
impl Ranged for PreProcessorsCoord {
    type FormatOption = DefaultFormatting;
    type ValueType = f64;

    fn map(&self, value: &Self::ValueType, limit: (i32, i32)) -> i32 {
        if self.is_log() {
            let fv = self.value_to_f64(value);
            let value_ln = self.decode(fv);
            self.linear.map(&value_ln, limit)
        } else {
            self.linear.map(value, limit)
        }
    }

    fn key_points<Hint: KeyPointHint>(&self, hint: Hint) -> Vec<f64> {
        let Range { mut start, mut end } = self.normalized;
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }
        let mut max_points = hint.max_num_points();
        // println!("key_points -- max_points {}", max_points);
        // max_points = 5;
        let result = Tick::ticks(max_points, start, end, self.is_log());
        // println!("key_points {:?}", result);
        result.into_iter().map(|t| t.value).collect()
    }
    // fn key_points2<Hint: KeyPointHint>(&self, hint: Hint) -> Vec<f64> {
    //     if self.is_log() {
    //         let max_points = hint.max_num_points();
    //
    //         let base = 10.0f64;
    //         let base_ln = base.ln();
    //
    //         let Range { mut start, mut end } = self.normalized;
    //
    //         if start > end {
    //             std::mem::swap(&mut start, &mut end);
    //         }
    //
    //         let bold_count = ((end / start).ln().abs() / base_ln).floor().max(1.0) as usize;
    //
    //         let light_density = if max_points < bold_count {
    //             0
    //         } else {
    //             let density = 1 + (max_points - bold_count) / bold_count;
    //             let mut exp = 1;
    //             while exp * 10 <= density {
    //                 exp *= 10;
    //             }
    //             exp - 1
    //         };
    //
    //         let mut multiplier = base;
    //         let mut cnt = 1;
    //         while max_points < bold_count / cnt {
    //             multiplier *= base;
    //             cnt += 1;
    //         }
    //
    //         let mut ret = vec![];
    //         let mut val = (base).powf((start.ln() / base_ln).ceil());
    //
    //         while val <= end {
    //             if !self.is_inf(val) {
    //                 ret.push(self.f64_to_value(val));
    //             }
    //             for i in 1..=light_density {
    //                 let v = val
    //                     * (1.0
    //                     + multiplier / f64::from(light_density as u32 + 1) * f64::from(i as u32));
    //                 if v > end {
    //                     break;
    //                 }
    //                 if !self.is_inf(val) {
    //                     ret.push(self.f64_to_value(v));
    //                 }
    //             }
    //             val *= multiplier;
    //         }
    //         ret
    //     } else {
    //         self.linear.key_points(hint)
    //     }
    // }


    fn range(&self) -> Range<Self::ValueType> {
        self.logic.clone()
    }
}