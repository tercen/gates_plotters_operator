use std::error::Error;
use std::str::FromStr;
use logicle::Logicle;
use crate::client::TercenError;
use crate::tercen::PreProcessor;

#[derive(Clone)]
pub enum PreProcess {
    Identity,
    Asinh(Asinh),
    Log,
    Logicl(Logicle),
}

#[derive(Clone)]
pub struct Asinh {
    co_factor: f64,
}

impl Asinh {
    fn decode(&self, v: f64) -> f64 {
        let value = v / self.co_factor;
        let vv = value + ((value).powi(2) + 1.0).powf(0.5);
        if vv > 0.0 {
            vv.ln()
        } else {
            f64::NAN
        }
    }

    fn encode(&self, v: f64) -> f64 {
        ((v.exp() - (-v).exp()) / 2.0) * self.co_factor
    }
}

impl PreProcess {
    fn property_value<F: FromStr>(pre_processor: &PreProcessor, name: &str) -> Result<F, Box<dyn Error>> {
        let value = pre_processor.operator_ref.as_ref()
            .map(|operator_ref| &operator_ref.property_values)
            .and_then(|pv| pv.iter().find(|pv| pv.name.eq(name)))
            .ok_or_else(|| format!("{} not found", name))?
            .value.parse::<F>()
            .map_err(|_| TercenError::new(&format!("{} parse failed", name)))?;
        Ok(value)
    }
    pub(crate) fn from_pre_processor(pre_processor: PreProcessor) -> Result<Self, Box<dyn Error>> {
        let name = pre_processor.operator_ref.as_ref().map_or("", |operator_ref| &operator_ref.name);
        match name {
            "asinh" => {
                let co_factor = PreProcess::property_value::<f64>(&pre_processor, "cofactor")
                    .unwrap_or(1.0);
                Ok(PreProcess::Asinh(Asinh { co_factor }))
            }
            "log" => Ok(PreProcess::Log),
            "logicle" => {
                let t = PreProcess::property_value::<f64>(&pre_processor, "T")?;
                let w = PreProcess::property_value::<f64>(&pre_processor, "W")?;
                let m = PreProcess::property_value::<f64>(&pre_processor, "M")?;
                let a = PreProcess::property_value::<f64>(&pre_processor, "A")?;
                let bins = PreProcess::property_value::<i32>(&pre_processor, "bins")?;
                Ok(PreProcess::Logicl(Logicle::new(t, w, m, a, bins)?))
            }
            &_ => Ok(PreProcess::Identity)
        }
    }
    pub(crate) fn encode_pre_processors(pre_processors: &Vec<PreProcess>, v: f64) -> f64 {
        pre_processors.iter().fold(v, |v, p| p.encode(v))
    }

    pub fn is_log_pre_processors(pre_processors: &Vec<PreProcess>) -> bool {
        pre_processors.iter().any(|p| p.is_log())
    }

    pub(crate) fn decode_pre_processors(pre_processors: &Vec<PreProcess>, v: f64) -> f64 {
        pre_processors.iter().rev().fold(v, |v, p| p.decode(v))
    }
    pub(crate) fn is_log(&self) -> bool {
        match self {
            PreProcess::Identity => false,
            PreProcess::Asinh(_) => true,
            PreProcess::Log => true,
            PreProcess::Logicl(_) => true,
        }
    }
    fn decode(&self, value: f64) -> f64 {
        match self {
            PreProcess::Identity => value,
            PreProcess::Asinh(asinh) => asinh.decode(value),
            PreProcess::Log => value.ln(),
            PreProcess::Logicl(logicle) => logicle.scale(value).unwrap_or(f64::NAN),
        }
    }

    fn encode(&self, value: f64) -> f64 {
        match self {
            PreProcess::Identity => value,
            PreProcess::Asinh(asinh) => asinh.encode(value),
            PreProcess::Log => value.exp(),
            PreProcess::Logicl(logicle) => logicle.inverse(value),
        }
    }
}

pub trait PreProcessorCodec {
    fn encode(&self, value: f64) -> f64;
    fn decode(&self, value: f64) -> f64;
}

impl PreProcessorCodec for Vec<PreProcess> {
    fn encode(&self, value: f64) -> f64 {
        PreProcess::encode_pre_processors(&self, value)
    }

    fn decode(&self, value: f64) -> f64 {
        PreProcess::decode_pre_processors(&self, value)
    }
}