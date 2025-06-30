extern crate arrow;
extern crate clap;
extern crate plotters;
extern crate polars;
extern crate logicle;
extern crate num_format;

use num_format::{Locale, ToFormattedString};

use logicle::{Logicle};

use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::iter;

use std::ops::Range;
use std::str::FromStr;

use base64::prelude::*;
use clap::Parser;

use plotters::coord::Shift;
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use plotters::coord::ranged1d::{AsRangedCoord, DefaultFormatting, KeyPointHint};
use plotters::data::float::FloatPrettyPrinter;
use polars::export::num::{Float, Pow, ToPrimitive};
use polars::export::num::float::FloatCore;
use polars::export::num::real::Real;
use polars::prelude::*;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tonic::codegen::tokio_stream::StreamExt;
use crate::client::{TercenContext, TercenError};
use crate::client::args::TercenArgs;
use crate::client::palette::JetPalette;
use crate::client::quartiles::quartiles;
use crate::client::query_helper::CubeAxisQueryHelper;
use crate::client::shapes::Shape;
use crate::tercen::{Acl, CrosstabSpec, CubeAxisQuery, CubeQuery, e_file_document, e_file_metadata, e_meta_factor, e_relation, e_task, EFileDocument, EFileMetadata, EMetaFactor, ERelation, ETask, Factor, FileDocument, FileMetadata, MetaFactor, Pair, ReqUploadTable, SimpleRelation, PreProcessor};
use crate::tercen::e_operator_input_spec ;
use crate::client::utils::*;
use crate::plotters_utils::coord::{PreProcessorsCoord, PreProcessorsRange};
use crate::plotters_utils::ticks::{PreProcessorsCoordTicks, PreProcessorsRangeTicks};
use crate::processors::PreProcess;
use crate::tercen::e_column_schema::Object;
use crate::tercen::e_schema;

mod client;
mod tercen;
mod plotters_utils;
mod processors;
mod ticks;

const FONT: &str = "sans-serif";


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tercen_ctx = TercenContext::new().await?;

    let cube_query = tercen_ctx.get_task().await?.get_cube_query()?.clone();
    let sample_meta_factor = get_sample_meta_factor(cube_query)?;

    // println!("shape_tbl -- {:?}", shape_tbl);

    let shapes_by_index = get_shapes_by_index(&tercen_ctx).await?;

    let overview_tbl = get_sample_overview_tbl(&tercen_ctx, &sample_meta_factor).await?;

    let task_and_pop_df = overview_tbl
        .select([".population.level", ".taskId", ".parentPopName", ".populationName"])?
        .unique_stable(None, UniqueKeepStrategy::First, None)?
        .sort([".population.level"], Default::default())?;

    let task_ids = task_and_pop_df
        .column(".taskId")?
        .iter()
        .map(|v| v.get_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>().ok_or_else(|| TercenError::new("Failed to get taskIds."))?;

    let population_levels = task_and_pop_df
        .column(".population.level")?
        .i32()?
        .into_no_null_iter()
        .collect::<Vec<_>>();

    let pop_names = task_and_pop_df
        .column(".populationName")?
        .iter()
        .map(|v| v.get_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| TercenError::new("Failed to get populationName."))?;

    let parent_pop_names = task_and_pop_df
        .column(".parentPopName")?
        .iter()
        .map(|v| v.get_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| TercenError::new("Failed to get parentPopName."))?;

    let last_pop_names = pop_names
        .last()
        .map(|e| e.to_string())
        .ok_or_else(|| TercenError::new("Failed to get last last_pop_names."))?;


    let mut cube_queries = vec![];
    for task_id in task_ids.iter() {
        cube_queries.push(tercen_ctx
            .get_task_from_id(task_id)
            .await?
            .get_cube_query()?
            .clone());
    }

    let mut global_column_df = overview_tbl
        .select(sample_meta_factor.factors.iter().map(|f| &f.name))?
        .unique_stable(None, UniqueKeepStrategy::First, None)?
        .sort(sample_meta_factor.factors.iter().map(|f| &f.name), Default::default())?;

    let n_samples = global_column_df.shape().0 as i32;

    global_column_df.insert_column(
        0,
        Series::new(".ci.global", (0..n_samples).collect::<Vec<_>>()),
    )?;

    let sample_factor_names: Vec<_> = sample_meta_factor.factors.iter()
        .map(|f| f.name.to_string())
        .collect();

    let shape_join_df = overview_tbl.left_join(&global_column_df,
                                               &sample_factor_names,
                                               &sample_factor_names)?;


    let filename = "gate_overview.png";
    let mime_type = "image/png";

    let rows_width = 80;
    let cols_height = 50;
    let pop_title_size = 0;
    let n_rows = cube_queries.len() - 1; // remove last

    let mut pop_count_previous = HashMap::new();

    let root_area = BitMapBackend::new(filename,
                                       (pop_title_size + rows_width + 250 * (n_samples as u32),
                                        cols_height + 250 * (n_rows) as u32)).into_drawing_area();

    root_area.fill(&WHITE)?;

    let (pop_names_area, drawing_areas) = root_area.split_horizontally(rows_width);

    let (_, pop_names_area) = pop_names_area.split_vertically(cols_height);
    let pop_name_areas = pop_names_area.split_evenly((n_rows, 1));

    let (col_header, drawing_areas) = drawing_areas.split_vertically(cols_height);

    let col_headers = col_header.split_evenly((1, n_samples as usize));
    let sample_df = global_column_df
        .select(sample_meta_factor.factors.iter()
            .map(|f| f.name.clone()))?;

    let mut samples = vec![];
    for i in 0..n_samples {
        let sample = sample_df
            .get_row(i as usize)?.0
            .iter()
            .map(|f| {
                match f {
                    AnyValue::String(v) => v.to_string(),
                    _ => f.to_string(),
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        samples.push(sample);
    }

    for (col_header_area, sample) in col_headers.iter().zip(samples.iter()) {
        let (width, height) = col_header_area.dim_in_pixel();
        let pos_w = ((width as f64) / 2.0) as i32;
        let pos_h = ((height as f64) / 2.0) as i32;

        let style = (FONT, 14, &BLACK)
            .into_text_style(col_header_area)
            .pos(Pos::new(HPos::Center, VPos::Center));

        col_header_area.draw_text(&sample, &style, (pos_w, pos_h))?;
    }


    let drawing_areas = drawing_areas.split_evenly((cube_queries.len() - 1, 1));

    // use std::time::Instant;
    // let now = Instant::now();

    // we should use .global.ci
    for (cube_query, population_level) in cube_queries.iter().zip(population_levels.into_iter()) {
        let axis_query = cube_query
            .axis_queries
            .first()
            .ok_or_else(|| TercenError::new("axis_query is required"))?;

        let has_x_axis = axis_query
            .x_axis
            .as_ref()
            .map(|factor| !factor.name.is_empty())
            .ok_or_else(|| TercenError::new("x_axis is required"))?;

        let has_y_axis = axis_query
            .y_axis
            .as_ref()
            .map(|factor| !factor.name.is_empty())
            .ok_or_else(|| TercenError::new("y_axis is required"))?;

        let has_log_color_pre_processor = axis_query.preprocessors.iter()
            .any(|p| p.r#type.eq("color"));

        let is_histo =
            axis_query.preprocessors.iter()
                .any(|p| p.operator_ref.as_ref()
                    .map_or(false, |v| v.name == "histogram"));

        let histogram_count_name = if has_x_axis && has_y_axis && is_histo { ".histogram_count" } else { ".y" };

        let mut stream = tercen_ctx
            .select_stream(&cube_query.qt_hash,
                           &vec![".ci".to_string(),
                                 histogram_count_name.to_string()])
            .await?;

        while let Some(evt) = stream.next().await {
            let data_frame = evt?;
            let data_frames_by_ci = data_frame
                .partition_by_stable([".ci"], true)?;
            for data_frame_by_ci in data_frames_by_ci.into_iter() {
                let current_ci: i32 = data_frame_by_ci
                    .column(".ci")?
                    .i32()?
                    .into_no_null_iter()
                    .next()
                    .ok_or_else(|| TercenError::new("failed to get .ci"))?;

                let old_histogram_count = pop_count_previous.get(&(current_ci, population_level))
                    .map(|c| *c)
                    .unwrap_or(0.0);

                let histogram_count = if is_histo {
                    if has_log_color_pre_processor {
                        data_frame_by_ci.column(histogram_count_name)?.f64()?
                            .into_no_null_iter()
                            .fold(old_histogram_count, |sum, value| sum + value.exp())
                    } else {
                        data_frame_by_ci.column(histogram_count_name)?.f64()?
                            .into_no_null_iter()
                            .fold(old_histogram_count, |sum, value| sum + value)
                    }
                } else {
                    data_frame_by_ci.column(histogram_count_name)?.f64()?
                        .into_no_null_iter().len().to_f64().
                        ok_or_else(|| TercenError::new("Failed to compute histogram_count."))?
                };


                // println!("histogram_count {} -- (current_ci, population_level) {:?}", histogram_count, (current_ci, population_level));

                pop_count_previous.insert((current_ci, population_level), histogram_count);
            }
        }
    }

    let population_levels = task_and_pop_df
        .column(".population.level")?
        .i32()?
        .into_no_null_iter()
        .collect::<Vec<_>>();

    let total_pop_count = pop_count_previous.iter()
        .filter(|((_ci, level), _pop_count)| level.eq(&0))
        .fold(0.0, |sum, ((_ci, _level), pop_count)| sum + *pop_count);

    // pop_names
    // for (pop_name, pop_area) in parent_pop_names.iter().zip(pop_name_areas.iter()) {
    for (((_parent_pop_name, pop_name), pop_area), population_level) in (parent_pop_names.iter().zip(pop_names.iter())).zip(pop_name_areas.iter()).zip(population_levels.iter()) {
        let (width, height) = pop_area.dim_in_pixel();

        let pop_count = pop_count_previous.iter()
            .filter(|((_ci, level), _pop_count)| level.eq(population_level))
            .fold(0.0, |sum, ((_ci, _level), pop_count)| sum + *pop_count);

        let next_pop_count = pop_count_previous.iter()
            .filter(|((_ci, level), _pop_count)| level.eq(&(population_level + 1)))
            .fold(0.0, |sum, ((_ci, _level), pop_count)| sum + *pop_count);

        let style = (FONT, 18, &BLACK)
            .into_text_style(pop_area)
            .pos(Pos::new(HPos::Center, VPos::Bottom))
            .transform(FontTransform::Rotate270);

        let pop_name_desc = format!("{}", pop_name);
        let pos_w = ((width as f64) / 2.0) as i32 - 10;
        let pos_h = ((height as f64) / 2.0) as i32;

        pop_area.draw_text(&pop_name_desc, &style, (pos_w, pos_h))?;

        let style = (FONT, 12, &BLACK)
            .into_text_style(pop_area)
            .pos(Pos::new(HPos::Center, VPos::Top))
            .transform(FontTransform::Rotate270);

        let pop_name_desc = if *population_level == 0 {
            format!("Total : {:.1}% N: {}",
                    100.0 * next_pop_count / total_pop_count,
                    (next_pop_count as u64).to_formatted_string(&Locale::en))
        } else {
            format!("Total : {:.1}% Parent: {:.1}% N: {}",
                    100.0 * next_pop_count / total_pop_count,
                    100.0 * next_pop_count / pop_count,
                    (next_pop_count as u64).to_formatted_string(&Locale::en))
        };

        let pos_w = ((width as f64) / 2.0) as i32 + 7;
        let pos_h = ((height as f64) / 2.0) as i32;

        pop_area.draw_text(&pop_name_desc, &style, (pos_w, pos_h))?;
    }


    let population_levels = task_and_pop_df
        .column(".population.level")?
        .i32()?
        .into_no_null_iter()
        .collect::<Vec<_>>();

    for (((task_id, _pop_name), cube_query), (drawing_area, population_level)) in task_ids.iter()
        .zip(parent_pop_names.iter())
        .zip(cube_queries.iter())
        .zip(drawing_areas.into_iter()
            .zip(population_levels.into_iter())) {
        let (qt_df, ci_df) = create_col_and_qt_dataframe(
            &tercen_ctx,
            &sample_meta_factor,
            &global_column_df,
            &cube_query,
        ).await?;

        draw_cube_query(&global_column_df,
                        &drawing_area,
                        task_id,
                        &cube_query,
                        qt_df,
                        ci_df,
                        &shape_join_df,
                        &shapes_by_index, population_level,
                        &pop_count_previous)?;
    }

    // To avoid the IO failure being ignored silently, we manually call the present function
    root_area.present().expect("Unable to write result to file");
    // println!("Result has been saved to {}", OUT_FILE_NAME);

    // let elapsed = now.elapsed();
    // println!("Elapsed: {:.2?}", elapsed);

    // return Ok(());

    let mut file = File::open(filename).await?;
    let mut bytes = vec![];

    file.read_to_end(&mut bytes).await?;

    let chunks = bytes.chunks(200_000);
    let n_chunks = chunks.len();

    let content = chunks
        .map(|chunk| BASE64_STANDARD.encode(chunk))
        .collect::<Vec<_>>();

    let filename = format!("{}_overview.png", &last_pop_names);

    let filename_series =
        Series::new("filename", iter::repeat(filename.to_string()).take(n_chunks).collect::<Vec<_>>());
    let mime_type_series =
        Series::new("mimetype", iter::repeat(mime_type.to_string()).take(n_chunks).collect::<Vec<_>>());
    let content_series =
        Series::new(".content", content);

    let mut result_df = DataFrame::new(vec![filename_series, mime_type_series, content_series])?;

    let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

    IpcStreamWriter::new(&mut buf).finish(&mut result_df).expect("ipc writer");

    let bytes = buf.into_inner();

    let mut file = FileDocument::default();

    file.name = filename.to_string();
    file.acl = Some(Acl { owner: tercen_ctx.owner().await?, aces: vec![] });
    file.project_id = tercen_ctx.project_id().await?;
    file.meta.push(Pair { key: "hidden".to_string(), value: "true".to_string() });
    let mut meta_data = FileMetadata::default();
    meta_data.content_type = "application/vnd.apache.arrow.file".to_string();
    file.metadata = Some(EFileMetadata { object: Some(e_file_metadata::Object::Filemetadata(meta_data)) });

    let outbound = async_stream::stream! {yield ReqUploadTable {file: Some(EFileDocument { object: Some(e_file_document::Object::Filedocument(file)) }), bytes}};
    let response = tercen_ctx.factory
        .table_service()?
        .upload_table(outbound).await?;

    let schema = response.into_inner().result
        .ok_or_else(|| TercenError::new("response.result missing"))?;

    let mut task = tercen_ctx
        .get_task()
        .await?.
        get_run_computation_task()?;

    task.computed_relation = Some(ERelation {
        object: Option::from(e_relation::Object::Simplerelation(SimpleRelation {
            id: schema.get_id()?.to_string(),
            index: 0,
        }))
    });

    tercen_ctx.factory
        .task_service()?
        .update(ETask { object: Option::from(e_task::Object::Runcomputationtask(task)) })
        .await?;

    Ok(())
}

async fn get_shapes_by_index(tercen_ctx: &TercenContext) -> Result<HashMap<i32, Shape>, Box<dyn Error>> {
    let mut shapes_by_index = HashMap::new();
    // ".shape.index", ".shape.json"
    let shape_tbl = get_shape_tbl(&tercen_ctx).await?;

    let shape_index = shape_tbl.column(".shape.index")?
        .i32()?
        .into_no_null_iter()
        .collect::<Vec<_>>();

    let shape_json = shape_tbl
        .column(".shape.json")?
        .iter()
        .map(|v| v.get_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| TercenError::new("Failed to get shapes from json."))?;

    for (shape_idx, shape_json) in shape_index.into_iter().zip(shape_json.into_iter()) {
        let shape = Shape::from_json(&shape_json)?;
        shapes_by_index.insert(shape_idx, shape);
    }
    Ok(shapes_by_index)
}

async fn get_sample_overview_tbl(ctx: &TercenContext, sample_factors: &MetaFactor) -> Result<DataFrame, Box<dyn Error>> {
    let task_env = ctx.get_task_env().await?;
    let schema_id = match task_env
        .iter()
        .find(|env| env.key.eq("sample.task.shape.index.schema.id"))
    {
        None => {
            Err(TercenError::new("failed to get tbl overview in env sample.task.shape.index.schema.id"))
        }
        Some(env) => Ok(env.value.clone()),
    }?;

    let mut columns = vec![".taskId".to_string(),
                           ".parentPopName".to_string(),
                           ".population.level".to_string(),
                           ".populationName".to_string(),
                           ".shape.index".to_string()];

    columns.extend(sample_factors.factors.iter().map(|f| f.name.to_string()));

    ctx.select_data_frame_from_id(&schema_id, &columns).await
}

async fn get_shape_tbl(ctx: &TercenContext) -> Result<DataFrame, Box<dyn Error>> {
    let task_env = ctx.get_task_env().await?;
    let schema_id = match task_env
        .iter()
        .find(|env| env.key.eq("shape.schema.id"))
    {
        None => {
            Err(TercenError::new("failed to get tbl shape in env shape.schema.id"))
        }
        Some(env) => Ok(env.value.clone()),
    }?;

    let columns = vec![".shape.index".to_string(),
                       ".shape.json".to_string()];

    ctx.select_data_frame_from_id(&schema_id, &columns).await
}

fn get_sample_meta_factor(cube_query: CubeQuery) -> Result<MetaFactor, Box<dyn Error>> {
    let input_spec = get_input_spec(cube_query)?;
    // println!("input_spec {:?}", &input_spec);

    let sample_meta_factor = input_spec
        .meta_factors
        .first()
        .and_then(|meta_factor| meta_factor.object.as_ref())
        .and_then(|object| match object {
            e_meta_factor::Object::Mappingfactor(_) => None,
            e_meta_factor::Object::Metafactor(meta_factor) => Some(meta_factor),
        })
        .ok_or_else(|| TercenError::new("Operator specification is required."))?;

    assert_eq!(&sample_meta_factor.ontology_mapping, "sample");
    assert_eq!(&sample_meta_factor.crosstab_mapping, "column");

    if sample_meta_factor.factors.is_empty() {
        return Err(Box::new(TercenError::new("A sample factor is required.")));
    }

    // println!("{:?}", sample_meta_factor);

    Ok(sample_meta_factor.clone())
}

fn get_input_spec(cube_query: CubeQuery) -> Result<CrosstabSpec, Box<dyn Error>> {
    let input_spec_object = cube_query
        .operator_settings
        .as_ref()
        .and_then(|operator_settings| operator_settings.operator_ref.as_ref())
        .and_then(|operator_ref| operator_ref.operator_spec.as_ref())
        .and_then(|operator_spec| operator_spec.input_specs.first())
        .and_then(|input_spec| input_spec.object.clone());

    let input_spec_object = input_spec_object
        .ok_or_else(|| TercenError::new("Operator specification is required."))?;
    let input_spec = match input_spec_object {
        e_operator_input_spec::Object::Crosstabspec(input_spec) => Ok(input_spec),
        e_operator_input_spec::Object::Operatorinputspec(_) => {
            Err(TercenError::new("Operator specification is required."))
        }
    }?;
    Ok(input_spec.clone())

    // if TercenArgs::try_parse().is_ok() {
    //     let input_spec_object = input_spec_object
    //         .ok_or_else(|| TercenError::new("Operator specification is required."))?;
    //     let input_spec = match input_spec_object {
    //         Object::Crosstabspec(input_spec) => Ok(input_spec),
    //         Object::Operatorinputspec(_) => {
    //             Err(TercenError::new("Operator specification is required."))
    //         }
    //     }?;
    //     Ok(input_spec.clone())
    // } else {
    //     Ok(CrosstabSpec {
    //         meta_factors: vec![EMetaFactor {
    //             object: Option::from(e_meta_factor::Object::Metafactor(MetaFactor {
    //                 name: "sample".to_string(),
    //                 r#type: "".to_string(),
    //                 description: "".to_string(),
    //                 ontology_mapping: "sample".to_string(),
    //                 crosstab_mapping: "column".to_string(),
    //                 cardinality: "".to_string(),
    //                 factors: vec![Factor {
    //                     name: "filename".to_string(),
    //                     r#type: "string".to_string(),
    //                 }],
    //             })),
    //         }],
    //         axis: vec![],
    //     })
    // }
}
fn exec_cube_query(cube_query: &CubeQuery, qt_df: DataFrame, population_level: i32, pop_count_previous: &mut HashMap<(i32, i32), f64>) -> Result<(), Box<dyn Error>> {
    let axis_query = cube_query
        .axis_queries
        .first()
        .ok_or_else(|| TercenError::new("x and y axis are required."))?;

    let data_frames_by_ci = qt_df.partition_by_stable([".ci"], true)?;

    for qt_df in data_frames_by_ci.into_iter() {
        let current_ci: i32 = qt_df
            .column(".ci")?
            .i32()?
            .into_no_null_iter()
            .next()
            .ok_or_else(|| TercenError::new("failed to get .ci"))?;

        // println!("current_ci -- {:?}", &current_ci);
        let has_x_axis = axis_query
            .x_axis
            .as_ref()
            .map(|factor| !factor.name.is_empty())
            .ok_or_else(|| TercenError::new("x_axis is required"))?;

        let has_y_axis = axis_query
            .y_axis
            .as_ref()
            .map(|factor| !factor.name.is_empty())
            .ok_or_else(|| TercenError::new("y_axis is required"))?;

        let is_histo =
            axis_query.preprocessors.iter()
                .any(|p| p.operator_ref.as_ref()
                    .map_or(false, |v| v.name == "histogram"));

        if has_x_axis && has_y_axis && is_histo {
            let histogram_count = qt_df.column(".histogram_count")?.f64()?
                .into_no_null_iter()
                .fold(0.0, |sum, value| sum + value);

            pop_count_previous.insert((current_ci, population_level), histogram_count);
        } else {
            let histogram_count = qt_df.column(".y")?.f64()?
                .into_no_null_iter()
                .fold(0.0, |sum, value| sum + value);

            pop_count_previous.insert((current_ci, population_level), histogram_count);
        }
    }
    Ok(())
}
fn draw_cube_query(
    global_column_df: &DataFrame,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
    task_id: &str,
    cube_query: &CubeQuery,
    qt_df: DataFrame,
    ci_df: DataFrame,
    shape_join_df: &DataFrame,
    shapes_by_index: &HashMap<i32, Shape>,
    population_level: i32,
    pop_count_previous: &HashMap<(i32, i32), f64>,
) -> Result<(), Box<dyn Error>>
{
    let axis_query = cube_query
        .axis_queries
        .first()
        .ok_or_else(|| TercenError::new("x and y axis are required."))?;

    let palette = create_palette_from_df(axis_query, &qt_df)?;
    let drawing_areas = drawing_area.split_evenly((1, global_column_df.shape().0));
    let data_frames_by_ci = qt_df.partition_by_stable([".ci"], true)?;

    let ci = ci_df.column(".ci")?.i32()?.into_no_null_iter().collect::<Vec<_>>();
    let ci_global = ci_df.column(".ci.global")?.i32()?.into_no_null_iter().collect::<Vec<_>>();

    // println!("ci -- {:?}", &ci);
    // println!("ci_global -- {:?}", &ci_global);

    // println!("global_column_df -- {:?}", global_column_df);
    // println!("ci_df -- {:?}", &ci_df);
    // println!("shape_join_df -- {:?}", shape_join_df);
    // println!("shapes_by_index -- {:?}", shapes_by_index);

    // println!("qt_df -- {:?}", &qt_df);


    for qt_df in data_frames_by_ci.into_iter() {
        let current_ci: i32 = qt_df
            .column(".ci")?
            .i32()?
            .into_no_null_iter()
            .next()
            .ok_or_else(|| TercenError::new("failed to get .ci"))?;

        // println!("current_ci -- {:?}", &current_ci);

        let (_, ci_global) = ci.iter()
            .zip(ci_global.iter())
            .find(|(i, _)| (**i) == current_ci)
            .ok_or_else(|| TercenError::new("failed to get ci_global"))?;

        let drawing_area = drawing_areas
            .get(*ci_global as usize)
            .ok_or_else(|| TercenError::new("failed to get drawing_area"))?;

        let iter_ci_global = shape_join_df.column(".ci.global")?.i32()?.into_no_null_iter();
        let iter_shape_index = shape_join_df.column(".shape.index")?.i32()?.into_no_null_iter();
        let iter_task_id = shape_join_df
            .column(".taskId")?
            .iter()
            .map(|v| v.get_str().map(|e| e.to_string()))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| TercenError::new("failed to get taskId"))?;

        let shapes = iter_task_id.iter()
            .zip(iter_shape_index)
            .zip(iter_ci_global)
            .filter(|((task, _), ci_glob)| ci_glob.eq(ci_global) && (*task).eq(&task_id.to_string()))
            .map(|((_, shape_index), _)| shape_index)
            .map(|shape_index| shapes_by_index.get(&shape_index))
            .filter(|shape| shape.is_some())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| TercenError::new("failed to get shapes"))?
            .iter()
            .map(|shape| (*shape).clone().clone())
            .collect::<Vec<_>>();

        let axis_query = AxisQueryWrapper::new(
            population_level,
            current_ci,
            axis_query.clone(),
            palette.clone(),
            qt_df,
            shapes);


        draw_sample(axis_query, pop_count_previous, drawing_area)?;
    }
    Ok(())
}

struct AxisQueryWrapper {
    population_level: i32,
    current_ci: i32,
    axis_query: CubeAxisQuery,
    palette: JetPalette,
    qt_df: DataFrame,
    shapes: Vec<Shape>,
}

impl AxisQueryWrapper {
    fn new(population_level: i32,
           current_ci: i32,
           axis_query: CubeAxisQuery,
           palette: JetPalette,
           qt_df: DataFrame,
           shapes: Vec<Shape>) -> Self {
        AxisQueryWrapper {
            population_level,
            current_ci,
            axis_query,
            palette,
            qt_df,
            shapes,
        }
    }

    fn is_histo(&self) -> Result<bool, Box<dyn Error>> {
        let flag = self.axis_query.preprocessors.iter()
            .any(|p| p.operator_ref.as_ref()
                .map_or(false, |v| v.name == "histogram"));

        // let flag = self.axis_query.preprocessors.iter()
        //     .any(|p| p.r#type.eq("histogram"));
        Ok(flag)
    }

    fn is_density_plot(&self) -> Result<bool, Box<dyn Error>> {
        Ok(self.is_histo()? && self.has_x_axis()? && self.has_y_axis()?)
    }

    fn is_2d(&self) -> Result<bool, Box<dyn Error>> {
        Ok(self.has_x_axis()? && self.has_y_axis()?)
    }

    fn is_histogram_plot(&self) -> Result<bool, Box<dyn Error>> {
        Ok(self.is_histo()? && !self.is_2d()?)
    }

    fn has_x_axis(&self) -> Result<bool, Box<dyn Error>> {
        let flag = self.axis_query
            .x_axis
            .as_ref()
            .map(|x_factor| !x_factor.name.is_empty())
            .ok_or_else(|| TercenError::new("x_axis is required"))?;

        Ok(flag)
    }

    fn has_y_axis(&self) -> Result<bool, Box<dyn Error>> {
        let flag = self.axis_query
            .y_axis
            .as_ref()
            .map(|x_factor| !x_factor.name.is_empty())
            .ok_or_else(|| TercenError::new("y_axis is required"))?;

        Ok(flag)
    }


    fn x_pre_processors(&self) -> Result<Vec<PreProcess>, Box<dyn Error>> {
        self.pre_processors_type("x")
    }

    fn y_pre_processors(&self) -> Result<Vec<PreProcess>, Box<dyn Error>> {
        self.pre_processors_type("y")
    }

    fn pre_processors_type(&self, ttype: &str) -> Result<Vec<PreProcess>, Box<dyn Error>> {
        let tt = self.axis_query.preprocessors.iter()
            .filter(|p| p.r#type.eq(ttype))
            .map(|p| PreProcess::from_pre_processor(p.clone()))
            .collect::<Result<Vec<PreProcess>, _>>()?;
        Ok(tt)
    }
    fn x_axis_factor(&self) -> Result<&Factor, Box<dyn Error>> {
        let x_axis_factor = self.axis_query
            .x_axis
            .as_ref()
            .ok_or_else(|| TercenError::new("x axis are required."))?;

        Ok(x_axis_factor)
    }

    fn y_axis_factor(&self) -> Result<&Factor, Box<dyn Error>> {
        let y_axis_factor = self.axis_query
            .y_axis
            .as_ref()
            .ok_or_else(|| TercenError::new("y axis are required."))?;

        Ok(y_axis_factor)
    }

    fn column_f64(&self, name: &str) -> PolarsResult<&Float64Chunked> {
        self.qt_df.column(name)?.f64()
    }

    fn column_i32(&self, name: &str) -> PolarsResult<&Int32Chunked> {
        self.qt_df.column(name)?.i32()
    }

    fn has_color_levels(&self) -> bool {
        self.qt_df.get_column_names().contains(&".colorLevels")
    }

    fn column_range_f64(&self, name: &str) -> Result<Range<f64>, Box<dyn Error>> {
        let values = self.column_f64(name)?;
        let x_min = values
            .into_no_null_iter()
            .min_by(|a, b| a.total_cmp(b))
            .unwrap();
        let x_max = values
            .into_no_null_iter()
            .max_by(|a, b| a.total_cmp(b))
            .unwrap();

        Ok(x_min..x_max)
    }

    fn total_pop(&self, pop_count_previous: &HashMap<(i32, i32), f64>) -> Result<f64, Box<dyn Error>> {
        let tt = pop_count_previous
            .get(&(self.current_ci, 0))
            .map(|v| *v);

        match tt {
            None => {
                Ok(0.0)
            }
            Some(v) => {
                Ok(v)
            }
        }
    }

    fn histogram_count_with(&self, pop_count_previous: &HashMap<(i32, i32), f64>) -> Result<f64, Box<dyn Error>> {
        let tt = pop_count_previous
            .get(&(self.current_ci, self.population_level))
            .map(|v| *v);

        match tt {
            None => {
                Ok(0.0)
            }
            Some(v) => {
                Ok(v)
            }
        }
    }

    fn population_count(&self, pop_count_previous: &HashMap<(i32, i32), f64>) -> Option<f64> {
        pop_count_previous
            .get(&(self.current_ci, self.population_level + 1))
            .map(|v| *v)
    }
}

fn create_palette_from_df(
    axis_query: &CubeAxisQuery,
    qt_df: &DataFrame) -> Result<JetPalette, Box<dyn Error>>
{
    let has_x_axis = axis_query
        .x_axis
        .as_ref()
        .map(|factor| !factor.name.is_empty())
        .ok_or_else(|| TercenError::new("x_axis is required"))?;

    let has_y_axis = axis_query
        .y_axis
        .as_ref()
        .map(|factor| !factor.name.is_empty())
        .ok_or_else(|| TercenError::new("y_axis is required"))?;

    let is_histo =
        axis_query.preprocessors.iter()
            .any(|p| p.operator_ref.as_ref()
                .map_or(false, |v| v.name == "histogram"));
    if is_histo {
        if has_x_axis && has_y_axis {
            let hist_count = qt_df
                .column(".histogram_count")?
                .f64()?
                .iter()
                .collect::<Option<Vec<f64>>>()
                .ok_or_else(|| TercenError::new("failed to get histogram_count."))?;

            let palette = create_palette(hist_count);
            Ok(palette)
        } else {
            let palette = JetPalette {
                min: 0.0,
                max: 1.0,
            };
            Ok(palette)
        }
    } else {
        let palette = JetPalette {
            min: 0.0,
            max: 1.0,
        };
        Ok(palette)
    }
}

async fn create_col_and_qt_dataframe(
    tercen_ctx: &TercenContext,
    sample_meta_factor: &MetaFactor,
    global_column_df: &DataFrame,
    cube_query: &CubeQuery) -> Result<(DataFrame, DataFrame), Box<dyn Error>> {
    let qt_df = create_qt_df(tercen_ctx, cube_query).await?;
    let ci_df = create_col_df(tercen_ctx, sample_meta_factor, global_column_df, cube_query).await?;
    Ok((qt_df, ci_df))
}

async fn create_qt_df(
    tercen_ctx: &TercenContext,
    cube_query: &CubeQuery) -> Result<DataFrame, Box<dyn Error>>
{
    let axis_query = cube_query
        .axis_queries
        .first()
        .ok_or_else(|| TercenError::new("axis_query is required"))?;

    let has_x_axis = axis_query
        .x_axis
        .as_ref()
        .map(|x_factor| !x_factor.name.is_empty())
        .ok_or_else(|| TercenError::new("x_axis is required"))?;

    let has_y_axis = axis_query
        .y_axis
        .as_ref()
        .map(|factor| !factor.name.is_empty())
        .ok_or_else(|| TercenError::new("y_axis is required"))?;

    let is_histo =
        axis_query.preprocessors.iter()
            .any(|p| p.operator_ref.as_ref()
                .map_or(false, |v| v.name == "histogram"));

    let mut columns = if is_histo {
        if has_x_axis && has_y_axis {
            vec![
                ".histogram_count".to_string(),
                ".y_bin_size".to_string(),
                ".x_bin_size".to_string(),
                ".y".to_string(),
                ".x".to_string(),
                ".ci".to_string(),
            ]
        } else {
            vec![
                ".y".to_string(),
                ".x".to_string(),
                ".ci".to_string(),
            ]
        }
    } else {
        if has_x_axis && has_y_axis {
            vec![
                ".y".to_string(),
                ".x".to_string(),
                ".ci".to_string(),
            ]
        } else {
            if has_x_axis {
                vec![
                    ".x".to_string(),
                    ".ci".to_string(),
                ]
            } else {
                vec![
                    ".y".to_string(),
                    ".ci".to_string(),
                ]
            }
        }
    };

    // let columns = if has_x_axis && has_y_axis {
    //     vec![
    //         ".histogram_count".to_string(),
    //         ".y_bin_size".to_string(),
    //         ".x_bin_size".to_string(),
    //         ".y".to_string(),
    //         ".x".to_string(),
    //         ".ci".to_string(),
    //     ]
    // } else {
    //     vec![
    //         // ".x_bin_size".to_string(),
    //         ".y".to_string(),
    //         ".x".to_string(),
    //         ".ci".to_string(),
    //     ]
    // };

    let schema = tercen_ctx.get_schema(&cube_query.qt_hash).await?;

    if schema.has_column(".colorLevels")? {
        columns.push(".colorLevels".to_string())
    }
    
    let qt_df = tercen_ctx
        .select_data_frame_from_id(&cube_query.qt_hash, &columns)
        .await?;

    Ok(qt_df)
}


async fn create_col_df(
    tercen_ctx: &TercenContext,
    sample_meta_factor: &MetaFactor,
    global_column_df: &DataFrame,
    cube_query: &CubeQuery) -> Result<DataFrame, Box<dyn Error>>
{
    let sample_factor_names = sample_meta_factor
        .factors
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>();

    let mut col_df = tercen_ctx
        .select_data_frame_from_id(&cube_query.column_hash,
                                   &sample_factor_names)
        .await?;

    col_df.insert_column(0, Series::new(".ci", 0..(col_df.shape().0 as i32)))?;

    let ci_df = global_column_df
        .left_join(
            &col_df,
            &sample_factor_names,
            &sample_factor_names)?
        .select([".ci", ".ci.global"])?;

    Ok(ci_df)
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

fn draw_sample(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>) -> Result<(), Box<dyn Error>>
{
    // println!("draw_sample -- {:?}", &axis_query.axis_query);
    if axis_query.is_density_plot()? {
        draw_sample_density_ticks(axis_query, pop_count_previous, drawing_area)
    } else if axis_query.is_histogram_plot()? {
        draw_sample_histogram_ticks(axis_query, pop_count_previous, drawing_area)
    } else if axis_query.is_2d()? {
        draw_sample_2d_ticks(axis_query, pop_count_previous, drawing_area)
    } else {
        Err(TercenError::new("draw_sample -- not impl"))?
    }
}

fn draw_sample_density(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
) -> Result<(), Box<dyn Error>>
{
    let primary_color = TRANSPARENT; //RGBAColor(0, 0, 0, 0.0);
    let secondary_color = TRANSPARENT; //RGBAColor(0, 0, 255, 0.0);


    let x_axis_factor = axis_query.x_axis_factor()?;
    let y_axis_factor = axis_query.y_axis_factor()?;

    let x_range = axis_query.column_range_f64(".x")?; //      x_min..x_max;
    let y_range = axis_query.column_range_f64(".y")?; //y_min..y_max;

    let primary_x_coord = PreProcessorsRange::from_range(axis_query.x_pre_processors()?, x_range.clone())?;
    let primary_y_coord = PreProcessorsRange::from_range(axis_query.y_pre_processors()?, y_range.clone())?;

    let primary_values = vec![(primary_x_coord.range.start, primary_y_coord.range.start),
                              (primary_x_coord.range.end, primary_y_coord.range.end)];

    // 1000000.to_formatted_string(&Locale::en);

    let total_pop = axis_query.total_pop(pop_count_previous)?;
    let histogram_count = axis_query.histogram_count_with(pop_count_previous)?;
    let population_count = axis_query.population_count(pop_count_previous);

    let caption = get_population_caption(&axis_query, total_pop, histogram_count, population_count);

    let caption_style = TextStyle::from((FONT, 13).into_font()).pos(Pos::new(HPos::Right, VPos::Top));

    let mut chart_builder = ChartBuilder::on(drawing_area)
        .caption(caption, caption_style)
        .margin(5)
        .margin_bottom(20)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(primary_x_coord, primary_y_coord)?
        .set_secondary_coord(x_range, y_range);

    chart_builder.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc(&y_axis_factor.name)
        .x_labels(5)
        .y_labels(7)
        .disable_mesh()
        // .x_label_formatter(&|v| FloatPrettyPrinter {
        //     allow_scientific: true,
        //     min_decimal: 1,
        //     max_decimal: 3,
        // }.print(*v))
        // .y_label_formatter(&|v| FloatPrettyPrinter {
        //     allow_scientific: true,
        //     min_decimal: 1,
        //     max_decimal: 3,
        // }.print(*v))
        .x_label_formatter(&|v| format!("{:.precision$e}", v, precision = 1))
        .y_label_formatter(&|v| format!("{:.precision$e}", v, precision = 1))
        .draw()?;


    chart_builder
        .configure_secondary_axes()
        .axis_style(secondary_color.clone())
        .label_style((FONT, 20).into_font().color(&secondary_color))
        .draw()?;


    chart_builder.draw_series(LineSeries::new(primary_values, &primary_color))?;

    let yy = axis_query.column_f64(".y")?;
    let xx = axis_query.column_f64(".x")?;

    let hist_count = axis_query.column_f64(".histogram_count")?;
    let y_bin_size = axis_query.column_f64(".y_bin_size")?;
    let x_bin_size = axis_query.column_f64(".x_bin_size")?;

    let line = xx
        .into_no_null_iter()
        .zip(yy.into_no_null_iter())
        .zip(
            x_bin_size
                .into_no_null_iter()
                .zip(y_bin_size.into_no_null_iter()),
        )
        .zip(hist_count.into_no_null_iter())
        .map(|((point, bin_sizes), histo_count)| {
            Rectangle::new(
                [
                    (point.0 - bin_sizes.0 / 2.0, point.1 - bin_sizes.1 / 2.0),
                    (point.0 + bin_sizes.0 / 2.0, point.1 + bin_sizes.1 / 2.0),
                ],
                axis_query.palette.get_color_rgb(histo_count).to_plotters().filled(),
            )
        });

    chart_builder.draw_secondary_series(line)?;

    draw_shapes(axis_query, &mut chart_builder)?;

    Ok(())
}

fn draw_sample_2d_ticks(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
) -> Result<(), Box<dyn Error>>
{
    let primary_color = TRANSPARENT; //RGBAColor(0, 0, 0, 0.0);
    let secondary_color = TRANSPARENT; //RGBAColor(0, 0, 255, 0.0);


    let x_axis_factor = axis_query.x_axis_factor()?;
    let y_axis_factor = axis_query.y_axis_factor()?;

    let x_range = axis_query.column_range_f64(".x")?; //      x_min..x_max;
    let y_range = axis_query.column_range_f64(".y")?; //y_min..y_max;

    let x_is_log = PreProcess::is_log_pre_processors(&axis_query.x_pre_processors()?);
    let y_is_log = PreProcess::is_log_pre_processors(&axis_query.y_pre_processors()?);
    let x_precision = if x_is_log { 0 } else { 1 };
    let y_precision = if y_is_log { 0 } else { 1 };
    let primary_x_coord = PreProcessorsRangeTicks::from_range(axis_query.x_pre_processors()?, x_range.clone())?;
    let primary_y_coord = PreProcessorsRangeTicks::from_range(axis_query.y_pre_processors()?, y_range.clone())?;


    // 1000000.to_formatted_string(&Locale::en);

    let total_pop = axis_query.total_pop(pop_count_previous)?;
    let histogram_count = axis_query.histogram_count_with(pop_count_previous)?;
    let population_count = axis_query.population_count(pop_count_previous);

    let caption = get_population_caption(&axis_query, total_pop, histogram_count, population_count);

    let caption_style = TextStyle::from((FONT, 13).into_font()).pos(Pos::new(HPos::Right, VPos::Top));

    let mut chart_builder = ChartBuilder::on(drawing_area)
        .caption(caption, caption_style)
        .margin(5)
        .margin_bottom(20)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(primary_x_coord.clone(), primary_y_coord.clone())?
        .set_secondary_coord(x_range, y_range);

    chart_builder.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc(&y_axis_factor.name)
        .x_labels(5)
        .y_labels(7)
        .disable_mesh()
        .x_label_formatter(&|v| v.format(x_precision))
        .y_label_formatter(&|v| v.format(y_precision))
        .draw()?;


    chart_builder
        .configure_secondary_axes()
        .axis_style(secondary_color.clone())
        .label_style((FONT, 20).into_font().color(&secondary_color))
        .draw()?;


    let primary_values = vec![(primary_x_coord.range.start.into(), primary_y_coord.range.start.into()),
                              (primary_x_coord.range.end.into(), primary_y_coord.range.end.into())];

    chart_builder.draw_series(LineSeries::new(primary_values, &primary_color))?;

    let yy = axis_query.column_f64(".y")?;
    let xx = axis_query.column_f64(".x")?;

    let point_size = (axis_query.axis_query.point_size / 2).min(1);


    let color_levels = if axis_query.has_color_levels() {
        axis_query.column_i32(".colorLevels")?
            .to_vec()
            .iter()
            .map(|op| op.unwrap_or(1))
            .collect::<Vec<_>>()
    } else {
        vec![0; yy.len()]
    };

    let min_level = color_levels.iter().min().unwrap_or(&0).to_f64().unwrap_or(0.0);
    let max_level = color_levels.iter().max().unwrap_or(&1).to_f64().unwrap_or(1.0)*1.2;


    let palette = crate::client::palette::JetPalette::new(min_level, max_level);

    let point_series = xx
        .into_no_null_iter()
        .zip(yy.into_no_null_iter())
        .zip(color_levels)
        .map(|((x, y), color_level)| {
            Circle::new((x, y), point_size,
                        palette.get_color_rgb(color_level.to_f64().unwrap()).to_plotters().filled(),
            )
        });


    chart_builder.draw_secondary_series(point_series)?;

    draw_shapes_ticks(axis_query, &mut chart_builder)?;

    Ok(())
}

fn draw_sample_density_ticks(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
) -> Result<(), Box<dyn Error>>
{
    let primary_color = TRANSPARENT; //RGBAColor(0, 0, 0, 0.0);
    let secondary_color = TRANSPARENT; //RGBAColor(0, 0, 255, 0.0);


    let x_axis_factor = axis_query.x_axis_factor()?;
    let y_axis_factor = axis_query.y_axis_factor()?;

    let x_range = axis_query.column_range_f64(".x")?; //      x_min..x_max;
    let y_range = axis_query.column_range_f64(".y")?; //y_min..y_max;

    let x_is_log = PreProcess::is_log_pre_processors(&axis_query.x_pre_processors()?);
    let y_is_log = PreProcess::is_log_pre_processors(&axis_query.y_pre_processors()?);
    let x_precision = if x_is_log { 0 } else { 1 };
    let y_precision = if y_is_log { 0 } else { 1 };
    let primary_x_coord = PreProcessorsRangeTicks::from_range(axis_query.x_pre_processors()?, x_range.clone())?;
    let primary_y_coord = PreProcessorsRangeTicks::from_range(axis_query.y_pre_processors()?, y_range.clone())?;


    // 1000000.to_formatted_string(&Locale::en);

    let total_pop = axis_query.total_pop(pop_count_previous)?;
    let histogram_count = axis_query.histogram_count_with(pop_count_previous)?;
    let population_count = axis_query.population_count(pop_count_previous);

    let caption = get_population_caption(&axis_query, total_pop, histogram_count, population_count);

    let caption_style = TextStyle::from((FONT, 13).into_font()).pos(Pos::new(HPos::Right, VPos::Top));

    let mut chart_builder = ChartBuilder::on(drawing_area)
        .caption(caption, caption_style)
        .margin(5)
        .margin_bottom(20)
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(primary_x_coord.clone(), primary_y_coord.clone())?
        .set_secondary_coord(x_range, y_range);

    chart_builder.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc(&y_axis_factor.name)
        .x_labels(5)
        .y_labels(7)
        .disable_mesh()
        .x_label_formatter(&|v| v.format(x_precision))
        .y_label_formatter(&|v| v.format(y_precision))
        .draw()?;


    chart_builder
        .configure_secondary_axes()
        .axis_style(secondary_color.clone())
        .label_style((FONT, 20).into_font().color(&secondary_color))
        .draw()?;


    let primary_values = vec![(primary_x_coord.range.start.into(), primary_y_coord.range.start.into()),
                              (primary_x_coord.range.end.into(), primary_y_coord.range.end.into())];

    chart_builder.draw_series(LineSeries::new(primary_values, &primary_color))?;

    let yy = axis_query.column_f64(".y")?;
    let xx = axis_query.column_f64(".x")?;

    let hist_count = axis_query.column_f64(".histogram_count")?;
    let y_bin_size = axis_query.column_f64(".y_bin_size")?;
    let x_bin_size = axis_query.column_f64(".x_bin_size")?;

    let line = xx
        .into_no_null_iter()
        .zip(yy.into_no_null_iter())
        .zip(
            x_bin_size
                .into_no_null_iter()
                .zip(y_bin_size.into_no_null_iter()),
        )
        .zip(hist_count.into_no_null_iter())
        .map(|((point, bin_sizes), histo_count)| {
            Rectangle::new(
                [
                    (point.0 - bin_sizes.0 / 2.0, point.1 - bin_sizes.1 / 2.0),
                    (point.0 + bin_sizes.0 / 2.0, point.1 + bin_sizes.1 / 2.0),
                ],
                axis_query.palette.get_color_rgb(histo_count).to_plotters().filled(),
            )
        });

    chart_builder.draw_secondary_series(line)?;

    draw_shapes_ticks(axis_query, &mut chart_builder)?;

    Ok(())
}

fn get_population_caption(axis_query: &AxisQueryWrapper, total_pop: f64, histogram_count: f64, population_count: Option<f64>) -> String {
    let caption = match population_count {
        None => {
            format!("Total: {:.1}% ",
                    (histogram_count / total_pop) * 100.0)
        }
        Some(population_count) => {
            if axis_query.population_level == 0 {
                format!("Total : {:.1}% N: {}",
                        (population_count / total_pop) * 100.0, (population_count as u64).to_formatted_string(&Locale::en))
            } else {
                format!("Total : {:.1}% Parent : {:.1}% N: {}",
                        (population_count / total_pop) * 100.0,
                        (population_count / histogram_count) * 100.0, (population_count as u64).to_formatted_string(&Locale::en))
            }
        }
    };
    caption
}

fn draw_sample_histogram(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>) -> Result<(), Box<dyn Error>>
{
    let color = BLUE; //RGBAColor(0, 0, 0, 1.0);
    let primary_color = TRANSPARENT; //RGBAColor(0, 0, 0, 0.0);
    let secondary_color = TRANSPARENT; //RGBAColor(0, 0, 255, 0.0);

    let y_axis_factor = axis_query.y_axis_factor()?;
    let x_axis_factor = axis_query.x_axis_factor()?;

    let x_range = axis_query.column_range_f64(".x")?; //x_min..x_max;
    let y_range = axis_query.column_range_f64(".y")?; //y_min..y_max;

    let primary_x_coord = PreProcessorsRange::from_range(axis_query.x_pre_processors()?, x_range.clone())?;
    let primary_y_coord = PreProcessorsRange::from_range(axis_query.y_pre_processors()?, y_range.clone())?;

    let primary_values = vec![(primary_x_coord.range.start, primary_y_coord.range.start),
                              (primary_x_coord.range.end, primary_y_coord.range.end)];

    let total_pop = axis_query.total_pop(pop_count_previous)?;
    let histogram_count = axis_query.histogram_count_with(pop_count_previous)?;
    let population_count = axis_query.population_count(pop_count_previous);

    let caption = get_population_caption(&axis_query, total_pop, histogram_count, population_count);

    let caption_style = TextStyle::from((FONT, 13).into_font()).pos(Pos::new(HPos::Right, VPos::Top));

    let mut chart_builder = ChartBuilder::on(drawing_area)
        .caption(caption, caption_style)
        .margin(5)
        .set_left_and_bottom_label_area_size(50)
        .build_cartesian_2d(primary_x_coord, primary_y_coord)?
        .set_secondary_coord(x_range, y_range);

    // let text_style = ("sans-serif", 20).with_color(RED).into_text_style(drawing_area) .transform(FontTransform::Rotate90);

    chart_builder.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc("count")
        .x_labels(10)
        .y_labels(10)
        .disable_mesh()
        // .x_label_style(text_style)
        .x_label_formatter(&|v| FloatPrettyPrinter {
            allow_scientific: true,
            min_decimal: 1,
            max_decimal: 5,
        }.print(*v))
        .y_label_formatter(&|v| FloatPrettyPrinter {
            allow_scientific: false,
            min_decimal: 0,
            max_decimal: 5,
        }.print(*v))

        // .x_label_formatter(&|v| format!("{:.1}", v))
        // .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()?;

    chart_builder
        .configure_secondary_axes()
        .axis_style(secondary_color.clone())
        .label_style((FONT, 20).into_font().color(&secondary_color))
        .draw()?;

    chart_builder.draw_series(LineSeries::new(primary_values, &primary_color))?;

    let yy = axis_query.column_f64(".y")?;
    let xx = axis_query.column_f64(".x")?;

    let xy = xx.into_no_null_iter()
        .zip(yy.into_no_null_iter()).collect::<Vec<(f64, f64)>>();

    // xy.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    chart_builder.draw_secondary_series(LineSeries::new(xy, &color))?;

    draw_shapes(axis_query, &mut chart_builder)?;

    Ok(())
}

fn draw_sample_histogram_ticks(
    axis_query: AxisQueryWrapper,
    pop_count_previous: &HashMap<(i32, i32), f64>,
    drawing_area: &DrawingArea<BitMapBackend, Shift>) -> Result<(), Box<dyn Error>>
{
    let color = BLUE; //RGBAColor(0, 0, 0, 1.0);
    let primary_color = TRANSPARENT; //RGBAColor(0, 0, 0, 0.0);
    let secondary_color = TRANSPARENT; //RGBAColor(0, 0, 255, 0.0);

    // let y_axis_factor = axis_query.y_axis_factor()?;
    let x_axis_factor = axis_query.x_axis_factor()?;

    let x_range = axis_query.column_range_f64(".x")?; //x_min..x_max;
    let y_range = axis_query.column_range_f64(".y")?; //y_min..y_max;

    let x_is_log = PreProcess::is_log_pre_processors(&axis_query.x_pre_processors()?);
    let y_is_log = PreProcess::is_log_pre_processors(&axis_query.y_pre_processors()?);
    let x_precision = if x_is_log { 0 } else { 1 };
    let y_precision = if y_is_log { 0 } else { 1 };

    let primary_x_coord = PreProcessorsRangeTicks::from_range(axis_query.x_pre_processors()?, x_range.clone())?;
    let primary_y_coord = PreProcessorsRangeTicks::from_range(axis_query.y_pre_processors()?, y_range.clone())?;

    let total_pop = axis_query.total_pop(pop_count_previous)?;
    let histogram_count = axis_query.histogram_count_with(pop_count_previous)?;
    let population_count = axis_query.population_count(pop_count_previous);

    let caption = get_population_caption(&axis_query, total_pop, histogram_count, population_count);

    let caption_style = TextStyle::from((FONT, 13).into_font()).pos(Pos::new(HPos::Right, VPos::Top));

    let mut chart_builder = ChartBuilder::on(drawing_area)
        .caption(caption, caption_style)
        .margin(5)
        .set_left_and_bottom_label_area_size(50)
        .build_cartesian_2d(primary_x_coord.clone(), primary_y_coord.clone())?
        .set_secondary_coord(x_range, y_range);

    // let text_style = ("sans-serif", 20).with_color(RED).into_text_style(drawing_area) .transform(FontTransform::Rotate90);


    chart_builder.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc("count")
        .x_labels(10)
        .y_labels(10)
        .disable_mesh()
        // .x_label_style(text_style)
        .x_label_formatter(&|v| v.format(x_precision))
        .y_label_formatter(&|v| v.format(y_precision))
        .draw()?;

    let primary_values = vec![(primary_x_coord.range.start.into(), primary_y_coord.range.start.into()),
                              (primary_x_coord.range.end.into(), primary_y_coord.range.end.into())];

    chart_builder.draw_series(LineSeries::new(primary_values.clone(), &primary_color))?;


    chart_builder
        .configure_secondary_axes()
        .axis_style(secondary_color.clone())
        .label_style((FONT, 20).into_font().color(&secondary_color))
        .draw()?;


    let yy = axis_query.column_f64(".y")?;
    let xx = axis_query.column_f64(".x")?;

    let xy = xx.into_no_null_iter()
        .zip(yy.into_no_null_iter()).collect::<Vec<(f64, f64)>>();

    // xy.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    chart_builder.draw_secondary_series(LineSeries::new(xy, &color))?;

    draw_shapes_ticks(axis_query, &mut chart_builder)?;
    // draw_shapes_ticks

    Ok(())
}

fn draw_shapes(axis_query: AxisQueryWrapper,
               cc: &mut ChartContext<BitMapBackend, Cartesian2d<PreProcessorsCoord, PreProcessorsCoord>>)
               -> Result<(), Box<dyn Error>>
{
    // let shapes = axis_query.encode_shapes()?;
    //we draw shapes on primary axis, we don't need to encode
    let shapes = axis_query.shapes.clone();


    let shape_style = ShapeStyle {
        color: BLACK.to_rgba(), //. mix(0.6),
        filled: false,
        stroke_width: 3,
    };
    for shape in shapes {
        match shape {
            Shape::Rectangle(shape) => {
                let rec = Rectangle::new(
                    [
                        (shape.rectangle.left, shape.rectangle.top),
                        (shape.rectangle.left + shape.rectangle.width, shape.rectangle.top + shape.rectangle.height)
                    ],
                    shape_style,
                );

                cc.draw_series([rec])?;
            }
            Shape::Quadrant(shape) => {
                if shape.quadrant.eq("Q1") {
                    // top left
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start, cc.y_range().end),
                            (shape.center.x, shape.center.y)
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q2") {
                    // top right
                    let rec = Rectangle::new(
                        [
                            (shape.center.x, cc.y_range().end),
                            (cc.x_range().end, shape.center.y)
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q3") {
                    // bottom right
                    let rec = Rectangle::new(
                        [
                            (shape.center.x, shape.center.y),
                            (cc.x_range().end, cc.y_range().start)
                        ],
                        shape_style,
                    );
                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q4") {
                    //bottom left
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start, shape.center.y),
                            (shape.center.x, cc.y_range().start)
                        ],
                        shape_style,
                    );
                    cc.draw_series([rec])?;
                } else {
                    //
                }
            }
            Shape::XRange(shape) => {
                cc.draw_series(LineSeries::new([(shape.range.start, 0.0),
                                                   (shape.range.start, cc.y_range().end)],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(shape.range.end, 0.0),
                                                   (shape.range.end, cc.y_range().end)],
                                               shape_style))?;
            }
            Shape::YRange(shape) => {
                // if axis_query.axis_query.is_histogram() {
                //     cc.draw_series(LineSeries::new([(shape.range.start, 0.0),
                //                                        (shape.range.start, cc.y_range().end)],
                //                                    shape_style))?;
                // 
                //     cc.draw_series(LineSeries::new([(shape.range.end, 0.0),
                //                                        (shape.range.end, cc.y_range().end)],
                //                                    shape_style))?;
                // } else {
                //     
                // }

                cc.draw_series(LineSeries::new([(cc.x_range().start, shape.range.start),
                                                   (cc.x_range().end, shape.range.start)],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(cc.x_range().start, shape.range.end),
                                                   (cc.x_range().end, shape.range.end)],
                                               shape_style))?;
            }
            Shape::Polygon(shape) => {
                if !shape.polygon.is_empty() {
                    cc.draw_series(LineSeries::new(
                        shape.polygon.iter()
                            .map(|p| (p.x, p.y))
                            .chain([shape.polygon.first().map(|p| (p.x, p.y)).unwrap()]), shape_style))?;
                }
            }
            Shape::XHistogramRange(shape) => {
                cc.draw_series(LineSeries::new([(shape.range.start, 0.0),
                                                   (shape.range.start, cc.y_range().end)],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(shape.range.end, 0.0),
                                                   (shape.range.end, cc.y_range().end)],
                                               shape_style))?;
            }
        }
    }
    Ok(())
}

fn draw_shapes_ticks(axis_query: AxisQueryWrapper,
                     cc: &mut ChartContext<BitMapBackend, Cartesian2d<PreProcessorsCoordTicks, PreProcessorsCoordTicks>>)
                     -> Result<(), Box<dyn Error>>
{
    // let shapes = axis_query.encode_shapes()?;
    //we draw shapes on primary axis, we don't need to encode
    let shapes = axis_query.shapes.clone();


    let shape_style = ShapeStyle {
        color: BLACK.to_rgba(), //. mix(0.6),
        filled: false,
        stroke_width: 3,
    };
    for shape in shapes {
        match shape {
            Shape::Rectangle(shape) => {
                let rec = Rectangle::new(
                    [
                        (shape.rectangle.left.into(), shape.rectangle.top.into()),
                        ((shape.rectangle.left + shape.rectangle.width).into(), (shape.rectangle.top + shape.rectangle.height).into())
                    ],
                    shape_style,
                );

                cc.draw_series([rec])?;
            }
            Shape::Quadrant(shape) => {
                if shape.quadrant.eq("Q1") {
                    // top left
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start.into(), cc.y_range().end.into()),
                            (shape.center.x.into(), shape.center.y.into())
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q2") {
                    // top right
                    let rec = Rectangle::new(
                        [
                            (shape.center.x.into(), cc.y_range().end.into()),
                            (cc.x_range().end.into(), shape.center.y.into())
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q3") {
                    // bottom right
                    let rec = Rectangle::new(
                        [
                            (shape.center.x.into(), shape.center.y.into()),
                            (cc.x_range().end.into(), cc.y_range().start.into())
                        ],
                        shape_style,
                    );
                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q4") {
                    //bottom left
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start.into(), shape.center.y.into()),
                            (shape.center.x.into(), cc.y_range().start.into())
                        ],
                        shape_style,
                    );
                    cc.draw_series([rec])?;
                } else {
                    //
                }
            }
            Shape::XRange(shape) => {
                cc.draw_series(LineSeries::new([(shape.range.start.into(), 0.0.into()),
                                                   (shape.range.start.into(), cc.y_range().end.into())],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(shape.range.end.into(), 0.0.into()),
                                                   (shape.range.end.into(), cc.y_range().end.into())],
                                               shape_style))?;
            }
            Shape::YRange(shape) => {
                // if axis_query.axis_query.is_histogram() {
                //     cc.draw_series(LineSeries::new([(shape.range.start, 0.0),
                //                                        (shape.range.start, cc.y_range().end)],
                //                                    shape_style))?;
                // 
                //     cc.draw_series(LineSeries::new([(shape.range.end, 0.0),
                //                                        (shape.range.end, cc.y_range().end)],
                //                                    shape_style))?;
                // } else {
                //     
                // }

                cc.draw_series(LineSeries::new([(cc.x_range().start.into(), shape.range.start.into()),
                                                   (cc.x_range().end.into(), shape.range.start.into())],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(cc.x_range().start.into(), shape.range.end.into()),
                                                   (cc.x_range().end.into(), shape.range.end.into())],
                                               shape_style))?;
            }
            Shape::Polygon(shape) => {
                if !shape.polygon.is_empty() {
                    cc.draw_series(LineSeries::new(
                        shape.polygon.iter()
                            .map(|p| (p.x.into(), p.y.into()))
                            .chain([shape.polygon.first().map(|p| (p.x.into(), p.y.into())).unwrap()]), shape_style))?;
                }
            }
            Shape::XHistogramRange(shape) => {
                cc.draw_series(LineSeries::new([(shape.range.start.into(), 0.0.into()),
                                                   (shape.range.start.into(), cc.y_range().end.into())],
                                               shape_style))?;

                cc.draw_series(LineSeries::new([(shape.range.end.into(), 0.0.into()),
                                                   (shape.range.end.into(), cc.y_range().end.into())],
                                               shape_style))?;
            }
        }
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use crate::tercen::{OperatorRef, PropertyValue};
    use super::*;
    const OUT_FILE_NAME: &str = "plotters-doc-data/main_test.png";
    const OUT_FILE_NAME2: &str = "plotters-doc-data/twoscale.png";


    #[test]
    fn main_test() -> Result<(), Box<dyn std::error::Error>> {
        let color = RGBAColor(0, 0, 0, 1.0);
        let primary_color = RGBAColor(0, 0, 0, 0.0);
        let secondary_color = RGBAColor(0, 0, 255, 1.0);

        let root_area = BitMapBackend::new(OUT_FILE_NAME, (1024, 768)).into_drawing_area();

        root_area.fill(&WHITE)?;

        let root_area = root_area.titled("Image Title", (FONT, 60))?;

        let (upper, _lower) = root_area.split_vertically(512);

        let mut x_pre_processor = PreProcessor::default();
        let mut y_pre_processor = PreProcessor::default();
        let mut asinh_operator_ref = OperatorRef::default();
        asinh_operator_ref.name = "asinh".to_string();
        let mut pv = PropertyValue::default();
        pv.name = "cofactor".to_string();
        pv.value = "400".to_string();
        asinh_operator_ref.property_values.push(pv);

        x_pre_processor.operator_ref = Some(asinh_operator_ref.clone());
        y_pre_processor.operator_ref = Some(asinh_operator_ref.clone());

        let x_pre_processors: Vec<PreProcessor> = vec![]; // vec![x_pre_processor];
        let y_pre_processors = vec![y_pre_processor];
        let y_pre_processors: Vec<PreProcessor> = vec![];

        let x_pre_processs = x_pre_processors
            .iter()
            .map(|p| PreProcess::from_pre_processor(p.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let y_pre_processs = y_pre_processors
            .iter()
            .map(|p| PreProcess::from_pre_processor(p.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let min_x = -10000.0f64;
        let max_x = 10000.0f64;

        let x_axis_linspace = (min_x..max_x).step((max_x - min_x).abs() / 200.0);

        let secondary_values = x_axis_linspace.values()
            .map(|x| (PreProcess::decode_pre_processors(&x_pre_processs, x),
                      2.0 + PreProcess::decode_pre_processors(&y_pre_processs, x)))
            .collect::<Vec<_>>();

        let min_x = secondary_values.iter().fold(f64::MAX, |a, &b| a.min(b.0));
        let max_x = secondary_values.iter().fold(f64::MIN, |a, &b| a.max(b.0));

        let min_y = secondary_values.iter().fold(f64::MAX, |a, &b| a.min(b.1));
        let max_y = secondary_values.iter().fold(f64::MIN, |a, &b| a.max(b.1));

        let x_range = min_x..max_x;
        let y_range = min_y..max_y;

        let primary_x_coord = PreProcessorsRange::from_range(x_pre_processs, x_range.clone())?;
        let primary_y_coord = PreProcessorsRange::from_range(y_pre_processs, y_range.clone())?;

        let primary_values = vec![(primary_x_coord.range.start, primary_y_coord.range.start),
                                  (primary_x_coord.range.end, primary_y_coord.range.end)];

        let mut chart_builder = ChartBuilder::on(&upper)
            .margin(5)
            .set_all_label_area_size(50)
            .caption("Exp with Asinh axis", (FONT, 40))
            .build_cartesian_2d(primary_x_coord, primary_y_coord)?
            .set_secondary_coord(x_range, y_range);

        chart_builder.configure_mesh()
            .x_labels(20)
            .y_labels(10)
            .disable_mesh()
            .y_desc("Values")
            .x_label_formatter(&|v| format!("{:.1}", v))
            .y_label_formatter(&|v| format!("{:+e}", v))
            .draw()?;

        chart_builder
            .configure_secondary_axes()
            .axis_style(secondary_color.clone())
            .label_style((FONT, 20).into_font().color(&secondary_color))
            .draw()?;

        chart_builder.draw_series(LineSeries::new(primary_values, &primary_color))?;

        chart_builder
            .draw_secondary_series(LineSeries::new(
                secondary_values,
                &color,
            ))?;

        // To avoid the IO failure being ignored silently, we manually call the present function
        root_area.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
        println!("Result has been saved to {}", OUT_FILE_NAME);
        Ok(())
    }

    #[test]
    fn main_test2() -> Result<(), Box<dyn std::error::Error>> {
        let root = BitMapBackend::new(OUT_FILE_NAME2, (1024, 768)).into_drawing_area();
        root.fill(&WHITE)?;

        let mut chart = ChartBuilder::on(&root)
            .x_label_area_size(35)
            .y_label_area_size(40)
            .right_y_label_area_size(40)
            .margin(5)
            .caption("Dual Y-Axis Example", (FONT, 50.0).into_font())
            .build_cartesian_2d(0f32..10f32, (0.1f32..1e10f32).log_scale())?
            .set_secondary_coord(0f32..10f32, -1.0f32..1.0f32);

        chart
            .configure_mesh()
            .disable_x_mesh()
            .disable_y_mesh()
            .y_desc("Log Scale")
            .y_label_formatter(&|x| format!("{:e}", x))
            .draw()?;

        chart
            .configure_secondary_axes()
            .y_desc("Linear Scale")
            .draw()?;

        chart
            .draw_series(LineSeries::new(
                (0..=100).map(|x| (x as f32 / 10.0, (1.02f32).powf(x as f32 * x as f32 / 10.0))),
                &BLUE,
            ))?
            .label("y = 1.02^x^2")
            .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], BLUE));

        chart
            .draw_secondary_series(LineSeries::new(
                (0..=100).map(|x| (x as f32 / 10.0, (x as f32 / 5.0).sin())),
                &RED,
            ))?
            .label("y = sin(2x)")
            .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], RED));

        chart
            .configure_series_labels()
            .background_style(RGBColor(128, 128, 128))
            .draw()?;

        // To avoid the IO failure being ignored silently, we manually call the present function
        root.present().expect("Unable to write result to file, please make sure 'plotters-doc-data' dir exists under current dir");
        println!("Result has been saved to {}", OUT_FILE_NAME);

        Ok(())
    }
}


