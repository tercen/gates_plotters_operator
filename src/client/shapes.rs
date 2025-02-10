use std::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::client::TercenError;
use crate::processors::PreProcessorCodec;

#[derive(Clone)]
pub enum Shape {
    Rectangle(RectangleShape),
    Quadrant(QuadrantShape),
    XRange(XRangeShape),
    YRange(YRangeShape),
    Polygon(PolygonShape),
    XHistogramRange(XHistogramRangeShape),
}

impl Shape {
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn Error>> {
        let value: Value = serde_json::from_str(json_str)?;

        let kind = value.as_object()
            .and_then(|object| object.get("kind"))
            .ok_or_else(|| TercenError::new("shape : no kind"))?;

        match kind.as_str() {
            None => Err(Box::new(TercenError::new("shape : bad json"))),
            Some(kind) => {
                let shape = if kind == "RectangleShape" {
                    let shape: RectangleShape = serde_json::from_str(json_str)?;
                    Ok(Shape::Rectangle(shape))
                } else if kind == "QuadrantShape" {
                    let shape: QuadrantShape = serde_json::from_str(json_str)?;
                    Ok(Shape::Quadrant(shape))
                } else if kind == "XRangeShape" {
                    let shape: XRangeShape = serde_json::from_str(json_str)?;
                    Ok(Shape::XRange(shape))
                } else if kind == "YRangeShape" {
                    let shape: YRangeShape = serde_json::from_str(json_str)?;
                    Ok(Shape::YRange(shape))
                } else if kind == "PolygonShape" {
                    let shape: PolygonShape = serde_json::from_str(json_str)?;
                    Ok(Shape::Polygon(shape))
                } else if kind == "XHistogramRangeShape" {
                    let shape: XHistogramRangeShape = serde_json::from_str(json_str)?;
                    Ok(Shape::XHistogramRange(shape))
                } else {
                    Err(Box::new(TercenError::new("shape : bad kind ")))
                };

                Ok(shape?)
            }
        }
    }

    pub(crate) fn encode<T: PreProcessorCodec>(&self, x_pre_processors: &T, y_pre_processors: &T) -> Self {
        match self {
            Shape::Rectangle(shape) => Shape::Rectangle(shape.encode(x_pre_processors, y_pre_processors)),
            Shape::Quadrant(shape) => Shape::Quadrant(shape.encode(x_pre_processors, y_pre_processors)),
            Shape::XRange(shape) => Shape::XRange(shape.encode(x_pre_processors, y_pre_processors)),
            Shape::YRange(shape) => Shape::YRange(shape.encode(x_pre_processors, y_pre_processors)),
            Shape::Polygon(shape) => Shape::Polygon(shape.encode(x_pre_processors, y_pre_processors)),
            Shape::XHistogramRange(shape) => Shape::XHistogramRange(shape.encode(x_pre_processors, y_pre_processors)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RectangleShape {
    kind: String,
    pub rectangle: Rectangle,
}

impl RectangleShape {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.rectangle = self.rectangle.encode(x_pre_process, y_pre_process);
        result
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QuadrantShape {
    kind: String,
    pub center: Point,
    pub quadrant: String,
}

impl QuadrantShape {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.center = self.center.encode(x_pre_process, y_pre_process);
        result
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct XRangeShape {
    kind: String,
    pub range: Range,
}

impl XRangeShape {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, _y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.range = self.range.encode(x_pre_process);
        result
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct YRangeShape {
    kind: String,
    pub range: Range,
}

impl YRangeShape {
    fn encode<T: PreProcessorCodec>(&self, _x_pre_process: &T, y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.range = self.range.encode(y_pre_process);
        result
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PolygonShape {
    kind: String,
    pub polygon: Vec<Point>,
}

impl PolygonShape {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.polygon = self.polygon.iter()
            .map(|p| p.encode(x_pre_process, y_pre_process))
            .collect();
        result
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct XHistogramRangeShape {
    kind: String,
    pub range: Range,
}

impl XHistogramRangeShape {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, _y_pre_process: &T) -> Self {
        let mut result = self.clone();
        result.range = self.range.encode(x_pre_process);
        result
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Rectangle {
    pub left: f64,
    pub top: f64,
    pub width: f64,
    pub height: f64,
}

impl Rectangle {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        // return self.decode(x_pre_process, y_pre_process);

        let left = x_pre_process.encode(self.left);
        let right = x_pre_process.encode(self.left + self.width);
        let width = (right - left).abs();
        let top = y_pre_process.encode(self.top);
        let bottom = y_pre_process.encode(self.top + self.height);
        let height = (top - bottom).abs();
        Rectangle {
            left,
            top,
            width,
            height,
        }
    }

    fn decode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        let left = x_pre_process.decode(self.left);
        let right = x_pre_process.decode(self.left + self.width);
        let width = (right - left).abs();
        let top = y_pre_process.decode(self.top);
        let bottom = y_pre_process.decode(self.top + self.height);
        let height = (top - bottom).abs();
        Rectangle {
            left,
            top,
            width,
            height,
        }

    }

    // left = enc(left)
    // right = enc(
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
impl Point {
    fn encode<T: PreProcessorCodec>(&self, x_pre_process: &T, y_pre_process: &T) -> Self {
        Point {
            x: x_pre_process.encode(self.x),
            y: y_pre_process.encode(self.y),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Range {
    pub start: f64,
    pub end: f64,
}

impl Range {
    fn encode<T: PreProcessorCodec>(&self, pre_process: &T) -> Self {
        Range {
            start: pre_process.encode(self.start),
            end: pre_process.encode(self.end),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::client::shapes::Shape;

    #[test]
    fn shape_from_json() {
        let json_str = "{\"kind\":\"RectangleShape\",\"rectangle\":{\"left\":0.20995661562233803,\"top\":2.040387313415672,\"width\":1.9829235919887478,\"height\":2.0754847167598145}}";

        let result = match Shape::from_json(json_str).unwrap() {
            Shape::Rectangle(rec) => Some(rec),
            Shape::Quadrant(_) => None,
            Shape::XRange(_) => None,
            Shape::YRange(_) => None,
            Shape::Polygon(_) => None,
            Shape::XHistogramRange(_) => None,
        };

        result.unwrap();
    }
}

