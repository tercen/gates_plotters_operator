use std::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::client::TercenError;

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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RectangleShape {
    kind: String,
    pub rectangle: Rectangle,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QuadrantShape {
    kind: String,
    pub center: Point,
    pub quadrant: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct XRangeShape {
    kind: String,
    pub range: Range,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct YRangeShape {
    kind: String,
    pub range: Range,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PolygonShape {
    kind: String,
    pub polygon: Vec<Point>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct XHistogramRangeShape {
    kind: String,
    pub range: Range,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Rectangle {
    pub left: f64,
    pub top: f64,
    pub width: f64,
    pub height: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}



#[derive(Serialize, Deserialize, Debug)]
pub struct Range {
    pub start: f64,
    pub end: f64,
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

