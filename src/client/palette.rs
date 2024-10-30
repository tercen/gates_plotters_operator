use plotters::style::RGBAColor;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct JetPalette {
    pub(crate) min: f64,
    pub(crate) max: f64,
}

impl JetPalette {
    pub fn new(min: f64, max: f64) -> Self {
        JetPalette { min, max }
    }
    pub fn convert(&self, data: &[f64]) -> Vec<u32> {
        if self.min.is_nan() || self.max.is_nan() {
            vec![255; data.len()]
            // Vec::new_fill(255, data.len())
        } else if self.max - self.min == 0.0 {
            data.iter()
                .map(|v| {
                    if v.is_nan() {
                        255
                    } else {
                        self.get_color_rgb_from_norm_value(0.5).to_u32()
                    }
                })
                .collect::<Vec<_>>()
        } else {
            let diff = self.max - self.min;
            data.iter()
                .map(|v| {
                    if v.is_nan() {
                        255
                    } else {
                        let mut norm = (v - self.min) / diff;
                        if norm > 1.0 {
                            norm = 1.0;
                        } else if norm < 0.0 {
                            norm = 0.0;
                        }
                        self.get_color_rgb_from_norm_value(norm).to_u32()
                    }
                })
                .collect::<Vec<_>>()
        }
    }
    pub fn get_color_rgb(&self, data: f64) -> RGB {
        if self.min.is_nan() || self.max.is_nan() {
            RGB(0, 0, 0)
            // Vec::new_fill(255, data.len())
        } else if self.max - self.min == 0.0 {
            if data.is_nan() {
                RGB(0, 0, 0)
            } else {
                self.get_color_rgb_from_norm_value(0.5)
            }
        } else {
            let diff = self.max - self.min;
            if data.is_nan() {
                RGB(0, 0, 0)
            } else {
                let mut norm = (data - self.min) / diff;
                if norm > 1.0 {
                    norm = 1.0;
                } else if norm < 0.0 {
                    norm = 0.0;
                }
                self.get_color_rgb_from_norm_value(norm)
            }
        }
    }

    fn get_color_rgb_from_norm_value(&self, value: f64) -> RGB {
        let a: f64;
        let mut r: u32 = 0;
        let mut g: u32 = 0;
        let mut b: u32 = 0;
        if value < 0.125 {
            a = value / 0.25;
            b = ((0.5 + a) * 255.0) as u32;
        } else {
            if value < 0.375 {
                a = (value - 0.125) * 1020.0;
                g = a as u32;
                b = 255;
            } else {
                if value < 0.625 {
                    a = (value - 0.375) / 0.25;
                    r = (a * 255.0) as u32;
                    g = 255;
                    b = ((1.0 - a) * 255.0) as u32;
                } else {
                    if value < 0.875 {
                        a = (value - 0.625) / 0.25;
                        r = 255;
                        g = ((1.0 - a) * 255.0) as u32;
                    } else {
                        a = (value - 0.875) / 0.25;
                        r = ((1.0 - a) * 255.0) as u32;
                    }
                }
            }
        }
        assert!(r <= 255);
        assert!(g <= 255);
        assert!(b <= 255);

        RGB(r as u8, g as u8, b as u8)
    }
}

pub struct RGB(u8, u8, u8);

impl RGB {
    pub fn to_u32(&self) -> u32 {
        let r = self.0 as u32;
        let g = self.1 as u32;
        let b = self.2 as u32;
        (r << 24) | (g << 16) | (b << 8) | 255
    }

    pub fn to_plotters(&self) -> RGBAColor {
        RGBAColor(self.0, self.1, self.2, 1.0)
    }
}
