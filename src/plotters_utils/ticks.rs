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
pub struct PreProcessorsCoordTicks {
    linear: RangedCoordf64,
    logic: Range<f64>,
    normalized: Range<f64>,
    zero_point: f64,
    negative: bool,
    pre_processors: Vec<PreProcess>,
    generate_sub_ticks: bool
}


#[derive(Default, Clone)]
pub struct PreProcessorsRangeTicks {
    pub(crate) range: Range<f64>,
    zero: f64,
    pre_processors: Vec<PreProcess>,
}

impl From<PreProcessorsRangeTicks> for PreProcessorsCoordTicks {
    fn from(spec: PreProcessorsRangeTicks) -> Self {
        let zero_point = spec.zero;
        let mut start = spec.range.start.as_f64() - zero_point;
        let mut end = spec.range.end.as_f64() - zero_point;
        let negative = false;

        PreProcessorsCoordTicks {
            linear: (spec.decode(start)..spec.decode(end)).into(),
            logic: spec.range,
            normalized: start..end,
            pre_processors: spec.pre_processors,
            zero_point,
            negative,
            generate_sub_ticks: true
        }
    }
}

impl PreProcessorsRangeTicks {
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

        Ok(PreProcessorsRangeTicks::from_encoded_range(pre_processs, min_encoded..max_encoded))
    }

    fn from_encoded_range(pre_processors: Vec<PreProcess>, range: Range<f64>) -> Self {
        PreProcessorsRangeTicks { range, zero: 0.0, pre_processors }
    }

    fn encode(&self, fv: f64) -> f64 {
        PreProcess::encode_pre_processors(&self.pre_processors, fv)
    }

    fn decode(&self, fv: f64) -> f64 {
        PreProcess::decode_pre_processors(&self.pre_processors, fv)
    }
}

impl AsRangedCoord for PreProcessorsRangeTicks {
    type CoordDescType = PreProcessorsCoordTicks;
    type Value = Tick;
}



impl PreProcessorsCoordTicks {
    pub fn is_log(&self) -> bool {
        self.pre_processors.iter()
            .any(|pre_processor| pre_processor.is_log())
    }

    pub fn encode(&self, v: f64) -> f64 {
        PreProcess::encode_pre_processors(&self.pre_processors, v)
    }

    pub fn decode(&self, v: f64) -> f64 {
        PreProcess::decode_pre_processors(&self.pre_processors, v)
    }
    pub fn value_to_f64(&self, value: &f64) -> f64 {
        let fv = value.as_f64() - self.zero_point;
        if self.negative {
            -fv
        } else {
            fv
        }
    }

    pub fn f64_to_value(&self, fv: f64) -> f64 {
        let fv = if self.negative { -fv } else { fv };
        f64::from_f64(fv + self.zero_point)
    }

    pub fn is_inf(&self, fv: f64) -> bool {
        let fv = if self.negative { -fv } else { fv };
        let a = f64::from_f64(fv + self.zero_point);
        let b = f64::from_f64(self.zero_point);

        (f64::as_f64(&a) - f64::as_f64(&b)).abs() < f64::EPSILON
    }
}

impl Ranged for PreProcessorsCoordTicks {
    type FormatOption = DefaultFormatting;
    type ValueType = Tick;

    fn map(&self, value: &Self::ValueType, limit: (i32, i32)) -> i32 {
        if self.is_log() {
            let fv = self.value_to_f64(&value.value);
            let value_ln = self.decode(fv);
            self.linear.map(&value_ln, limit)
        } else {
            self.linear.map(&value.value, limit)
        }
    }

    fn key_points<Hint: KeyPointHint>(&self, hint: Hint) -> Vec<Tick> {
        let Range { mut start, mut end } = self.normalized;
        if start > end {
            std::mem::swap(&mut start, &mut end);
        }
        let mut max_points = hint.max_num_points();
        // println!("key_points -- max_points {}", max_points);
        // max_points = 5;

        let result = Tick::ticks(max_points, start, end, self.is_log());
        // println!("key_points {:?}", result);

        if !self.generate_sub_ticks {
            result.into_iter().filter(|tick| !tick.is_sub_tick).collect()
        } else {
            result
        }
    }



    fn range(&self) -> Range<Self::ValueType> {
        let start = Tick {value: self.logic.start, is_sub_tick:false};
        let end = Tick {value: self.logic.end, is_sub_tick:false};
        Range { start, end }
//        self.logic.clone()
    }
}