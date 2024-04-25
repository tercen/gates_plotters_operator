mod tercen;
mod client;

extern crate arrow;
extern crate plotters;
extern crate polars;
extern crate clap;

use std::error::Error;

use arrow::array::{Array};
use clap::Parser;
use plotters::coord::Shift;

use crate::client::{TercenContext, TercenError};

use polars::prelude::*;
use plotters::prelude::*;
use crate::client::args::TercenArgs;
use crate::client::palette::JetPalette;
use crate::client::quartiles::quartiles;
use crate::tercen::{CrosstabSpec, CubeAxisQuery, CubeQuery, e_meta_factor, EMetaFactor, Factor, MetaFactor};
use crate::tercen::e_operator_input_spec::Object;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tercen_ctx = TercenContext::new().await?;

    let cube_query = tercen_ctx.get_cube_query().await?;
    let sample_meta_factor = get_sample_meta_factor(cube_query)?;

    let cube_queries = tercen_ctx.get_cube_queries_from_task().await?;

    let global_column_df = create_global_column_df(&tercen_ctx, &sample_meta_factor, &cube_queries).await?;
    let n_samples = global_column_df.shape().0 as i32;

    let root_area = BitMapBackend::new(
        "test.png",
        (200 * (n_samples as u32), 250 * cube_queries.len() as u32))
        .into_drawing_area();

    root_area.fill(&WHITE)?;

    let root_area = root_area.titled("Image Title", ("sans-serif", 60))?;
    let drawing_areas = root_area.split_evenly((cube_queries.len(), 1));

    for (cube_query, drawing_area) in cube_queries.iter().zip(drawing_areas.into_iter()) {
        let qt_df = create_qt_df(
            &tercen_ctx,
            &sample_meta_factor,
            &global_column_df,
            &cube_query)
            .await?;
        let result = draw_cube_query(
            &global_column_df,
            &drawing_area,
            &cube_query,
            qt_df);


        match result {
            Ok(_) => {}
            Err(e) => {
                let text_style = ("sans-serif", 20).with_color(RED).into_text_style(&drawing_area);
                drawing_area.draw_text(&e.to_string(), &text_style, (10, 50))?;
            }
        }
    }

    // To avoid the IO failure being ignored silently, we manually call the present function
    root_area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
    // println!("Result has been saved to {}", OUT_FILE_NAME);
    Ok(())
}

fn get_sample_meta_factor(cube_query: CubeQuery) -> Result<MetaFactor, Box<dyn Error>> {
    let input_spec = get_input_spec(cube_query)?;
    println!("input_spec {:?}", &input_spec);

    let sample_meta_factor = input_spec.meta_factors
        .first()
        .and_then(|meta_factor| meta_factor.object.as_ref())
        .and_then(|object| match object {
            e_meta_factor::Object::Mappingfactor(_) => None,
            e_meta_factor::Object::Metafactor(meta_factor) => Some(meta_factor)
        })
        .ok_or_else(|| TercenError::new("Operator specification is required."))?;

    println!("sample_meta_factor {:?}", &sample_meta_factor);

    assert_eq!(&sample_meta_factor.ontology_mapping, "sample");
    assert_eq!(&sample_meta_factor.crosstab_mapping, "column");
    Ok(sample_meta_factor.clone())
}

fn get_input_spec(cube_query: CubeQuery) -> Result<CrosstabSpec, Box<dyn Error>> {
    let input_spec_object = cube_query.operator_settings.as_ref()
        .and_then(|operator_settings| operator_settings.operator_ref.as_ref())
        .and_then(|operator_ref| operator_ref.operator_spec.as_ref())
        .and_then(|operator_spec| operator_spec.input_specs.first())
        .and_then(|input_spec| input_spec.object.clone());

    if TercenArgs::try_parse().is_ok() {
        let input_spec_object = input_spec_object
            .ok_or_else(|| TercenError::new("Operator specification is required."))?;
        let input_spec = match input_spec_object {
            Object::Crosstabspec(input_spec) => Ok(input_spec),
            Object::Operatorinputspec(input_spec) => Err(TercenError::new("Operator specification is required."))
        }?;
        Ok(input_spec.clone())
    } else {
        Ok(CrosstabSpec {
            meta_factors: vec![EMetaFactor {
                object: Option::from(e_meta_factor::Object::Metafactor(MetaFactor {
                    name: "sample".to_string(),
                    r#type: "".to_string(),
                    description: "".to_string(),
                    ontology_mapping: "sample".to_string(),
                    crosstab_mapping: "column".to_string(),
                    cardinality: "".to_string(),
                    factors: vec![Factor { name: "filename".to_string(), r#type: "string".to_string() }],
                })),
            }],
            axis: vec![],
        })
    }
}

fn draw_cube_query(global_column_df: &DataFrame,
                   drawing_area: &DrawingArea<BitMapBackend, Shift>,
                   cube_query: &CubeQuery,
                   qt_df: DataFrame) -> Result<(), Box<dyn Error>> {
    let axis_query = cube_query.axis_queries
        .first()
        .ok_or_else(|| TercenError::new("x and y axis are required."))?;

    let hist_count = qt_df.column(".histogram_count")?
        .f64()?
        .iter()
        .collect::<Option<Vec<f64>>>()
        .ok_or_else(|| TercenError::new("failed to get histogram_count."))?;

    let palette = create_palette(hist_count);

    let data_frames = qt_df
        .partition_by(["_sample_offset"], true)?;

    let drawing_areas = drawing_area.split_evenly((1, global_column_df.shape().0));

    for (drawing_area, df) in drawing_areas.iter().zip(data_frames) {
        match draw_samples(&axis_query, &palette, drawing_area, df) {
            Ok(_) => {}
            Err(e) => {
                let text_style = ("sans-serif", 20).with_color(RED).into_text_style(drawing_area);
                drawing_area.draw_text(&e.to_string(), &text_style, (10, 50))?;
            }
        }
    }
    Ok(())
}

async fn create_global_column_df(tercen_ctx: &TercenContext,
                                 sample_meta_factor: &MetaFactor,
                                 cube_queries: &Vec<CubeQuery>) -> Result<DataFrame, Box<dyn Error>> {
    let sample_factor_names = sample_meta_factor.factors.iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    let (first_query, rest) = cube_queries.split_first().ok_or_else(|| TercenError::new("nothing to compute."))?;

    let mut global_column_df = tercen_ctx.select_data_frame_from_id(&first_query.column_hash, sample_factor_names.clone()).await?;

    for cube_query in rest {
        let col_df = tercen_ctx.select_data_frame_from_id(&cube_query.column_hash, sample_factor_names.clone()).await?;
        global_column_df.extend(&col_df)?;
    }

    let mut global_column_df = global_column_df
        .unique_stable(None, UniqueKeepStrategy::First, None)?
        .sort(sample_factor_names.clone(), Default::default())?;

    let n_samples = global_column_df.shape().0 as i32;

    println!("global_column_df {:?}", global_column_df);

    global_column_df.insert_column(0, Series::new("_sample_offset", (0..n_samples).collect::<Vec<_>>()))?;
    Ok(global_column_df)
}

async fn create_qt_df(tercen_ctx: &TercenContext,
                      sample_meta_factor: &MetaFactor,
                      global_column_df: &DataFrame,
                      cube_query: &CubeQuery) -> Result<DataFrame, Box<dyn Error>> {
    let qt_df = tercen_ctx.select_data_frame_from_id(&cube_query.qt_hash,
                                                     vec![
                                                         ".histogram_count".to_string(),
                                                         ".y_bin_size".to_string(),
                                                         ".x_bin_size".to_string(),
                                                         ".y".to_string(),
                                                         ".x".to_string(),
                                                         ".ci".to_string()]).await?;

    let sample_factor_names = sample_meta_factor.factors.iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();
    let mut col_df = tercen_ctx.select_data_frame_from_id(&cube_query.column_hash,
                                                          sample_factor_names.clone())
        .await?;

    col_df.insert_column(0, Series::new(".ci", (0..(col_df.shape().0 as i32))
        .collect::<Vec<_>>()))?;

    let qt_df = col_df.left_join(&qt_df, [".ci"], [".ci"])?;

    let qt_df = global_column_df.left_join(
        &qt_df,
        sample_factor_names.clone(),
        sample_factor_names.clone())?;

    Ok(qt_df)
}

fn create_palette(hist_count: Vec<f64>) -> JetPalette {
    let mut sorted_histo_counts = hist_count.clone();
    sorted_histo_counts.sort_by(|a, b| a.total_cmp(b));

    let (q1, q2, q3) = quartiles(sorted_histo_counts);

    let color_palette_min_value = q2 - 1.5 * (q3 - q1);
    let color_palette_max_value = q2 + 1.5 * (q3 - q1);

    let palette = JetPalette {
        min: color_palette_min_value,
        max: color_palette_max_value,
    };
    palette
}

fn draw_samples(axis_query: &CubeAxisQuery,
                palette: &JetPalette,
                drawing_area: &DrawingArea<BitMapBackend, Shift>,
                df: DataFrame) -> Result<(), Box<dyn Error>> {
    let x_axis_factor = axis_query.x_axis.as_ref().ok_or_else(|| TercenError::new("x axis are required."))?;
    let y_axis_factor = axis_query.y_axis.as_ref().ok_or_else(|| TercenError::new("y axis are required."))?;

    let yy = df.column(".y")?.f64()?;
    let xx = df.column(".x")?.f64()?;
    let hist_count = df.column(".histogram_count")?.f64()?;
    let y_bin_size = df.column(".y_bin_size")?.f64()?;
    let x_bin_size = df.column(".x_bin_size")?.f64()?;

    let x_min = xx.into_no_null_iter().min_by(|a, b| a.total_cmp(b)).unwrap();
    let x_max = xx.into_no_null_iter().max_by(|a, b| a.total_cmp(b)).unwrap();

    let y_min = yy.into_no_null_iter().min_by(|a, b| a.total_cmp(b)).unwrap();
    let y_max = yy.into_no_null_iter().max_by(|a, b| a.total_cmp(b)).unwrap();

    let mut cc = ChartBuilder::on(drawing_area)
        .margin(5)
        .set_left_and_bottom_label_area_size(50)
        // .caption("Sine and Cosine", ("sans-serif", 40))
        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    cc.configure_mesh().x_desc(&x_axis_factor.name).y_desc(&y_axis_factor.name)
        .x_labels(10)
        .y_labels(10)
        .disable_mesh()
        .x_label_formatter(&|v| format!("{:.1}", v))
        .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()?;

    let line = xx.into_no_null_iter().zip(yy.into_no_null_iter())
        .zip(x_bin_size.into_no_null_iter().zip(y_bin_size.into_no_null_iter()))
        .zip(hist_count.into_no_null_iter())
        .map(|((point, bin_sizes), histo_count)|
            Rectangle::new(
                [(point.0 - bin_sizes.0 / 2.0, point.1 - bin_sizes.1 / 2.0),
                    (point.0 + bin_sizes.0 / 2.0, point.1 + bin_sizes.1 / 2.0)],
                palette
                    .get_color_rgb(histo_count)
                    .to_plotters()
                    .filled(),
            ));

    cc.draw_series(line)?;

    // cc.configure_series_labels().border_style(BLACK).draw()?;
    Ok(())
}
