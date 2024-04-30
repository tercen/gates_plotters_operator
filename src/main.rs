extern crate arrow;
extern crate clap;
extern crate plotters;
extern crate polars;

use std::collections::HashMap;
use std::error::Error;
use std::io::Cursor;
use std::iter;

use arrow::array::Array;
use base64::prelude::*;
use clap::Parser;
use plotters::coord::Shift;
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};
use polars::export::arrow::io::iterator::StreamingIterator;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;
use prost::bytes::Buf;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tonic::codegen::Body;

use crate::client::{TercenContext, TercenError};
use crate::client::args::TercenArgs;
use crate::client::palette::JetPalette;
use crate::client::quartiles::quartiles;
use crate::client::shapes::Shape;
use crate::tercen::{Acl, CrosstabSpec, CubeAxisQuery, CubeQuery, e_file_document, e_file_metadata, e_meta_factor, e_relation, e_schema, e_task, EFileDocument, EFileMetadata, EMetaFactor, ERelation, ETask, Factor, FileDocument, FileMetadata, MetaFactor, Pair, ReqUploadTable, SimpleRelation};
use crate::tercen::e_operator_input_spec::Object;

mod client;
mod tercen;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tercen_ctx = TercenContext::new().await?;

    let cube_query = tercen_ctx.get_cube_query().await?;
    // println!("cube_query {:?}", &cube_query);
    let sample_meta_factor = get_sample_meta_factor(cube_query)?;
    // println!("sample_meta_factor {:?}", &sample_meta_factor);

    let shape_tbl = get_shape_tbl(&tercen_ctx).await?;
    // println!("shape_tbl {:?}", &shape_tbl);

    let mut shapes_by_index = HashMap::new();
    let shape_index = shape_tbl.column(".shape.index")?.i32()?.into_no_null_iter().collect::<Vec<_>>();
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

    // // contains the following columns
    // let mut columns = vec![".taskId".to_string(),
    //                        ".parentPopName".to_string(),
    //                        ".shape.index".to_string()];
    //
    // for factor in sample_factors.factors.iter() {
    //     columns.push(factor.name.clone());
    // }

    let overview_tbl = get_sample_overview_tbl(&tercen_ctx, &sample_meta_factor).await?;
    // println!("overview_tbl {:?}", &overview_tbl);

    let task_and_pop_df = overview_tbl
        .select([".population.level", ".taskId", ".parentPopName", ".populationName"])?
        .unique_stable(None, UniqueKeepStrategy::First, None)?
        .sort([".population.level"], Default::default())?;

    let task_ids = task_and_pop_df
        .column(".taskId")?
        .iter()
        .map(|v| v.get_str().map(|s| s.to_string()))
        .collect::<Option<Vec<_>>>().ok_or_else(|| TercenError::new("Failed to get taskIds."))?;

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


    println!("task_ids {:?}", &task_ids);

    let mut cube_queries = vec![];
    for task_id in task_ids.iter() {
        cube_queries.push(tercen_ctx.get_cube_query_from_task(task_id).await?);
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
    println!("global_column_df {:?}", global_column_df);

    let sample_factor_names: Vec<_> = sample_meta_factor.factors.iter()
        .map(|f| f.name.to_string())
        .collect();

    let shape_join_df = overview_tbl.left_join(&global_column_df,
                                               &sample_factor_names,
                                               &sample_factor_names)?;


    let filename = "gate_overview.png";
    let mime_type = "image/png";

    let rows_width = 100;
    let cols_height = 100;
    let title_size = 50;

    let root_area = BitMapBackend::new(filename,
                                       (title_size + rows_width + 250 * (n_samples as u32),
                                        cols_height + 250 * cube_queries.len() as u32)).into_drawing_area();

    root_area.fill(&WHITE)?;


    let root_area = root_area.titled(&format!("{} overview", &last_pop_names), ("sans-serif", 30))?;

    let (pop_names_area, drawing_areas) = root_area.split_horizontally(rows_width);

    let (_, pop_names_area) = pop_names_area.split_vertically(cols_height);
    let pop_name_areas = pop_names_area.split_evenly((parent_pop_names.len(), 1));

    for (pop_name, pop_area) in parent_pop_names.iter().zip(pop_name_areas.iter()) {
        let (width, height) = pop_area.dim_in_pixel();
        let pos_w = ((width as f64) / 2.0) as i32;
        let pos_h = ((height as f64) / 2.0) as i32;

        let style = ("sans-serif", 14, &BLACK)
            .into_text_style(pop_area)
            .pos(Pos::new(HPos::Center, VPos::Center))
            .transform(FontTransform::Rotate270);

        pop_area.draw_text(pop_name, &style, (pos_w, pos_h))?;
    }

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

        let style = ("sans-serif", 14, &BLACK)
            .into_text_style(col_header_area)
            .pos(Pos::new(HPos::Center, VPos::Center));

        col_header_area.draw_text(&sample, &style, (pos_w, pos_h))?;
    }


    let drawing_areas = drawing_areas.split_evenly((cube_queries.len(), 1));

    for (((task_id, pop_name), cube_query), drawing_area) in task_ids.iter()
        .zip(parent_pop_names.iter())
        .zip(cube_queries.iter())
        .zip(drawing_areas.into_iter()) {
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
                        &shapes_by_index)?;
        // let result = draw_cube_query(&global_column_df, &drawing_area, &cube_query, qt_df, ci_df);
        //
        // match result {
        //     Ok(_) => {}
        //     Err(e) => {
        //         let text_style = ("sans-serif", 20)
        //             .with_color(RED)
        //             .into_text_style(&drawing_area);
        //         drawing_area.draw_text(&e.to_string(), &text_style, (10, 50))?;
        //     }
        // }
    }

    // To avoid the IO failure being ignored silently, we manually call the present function
    root_area.present().expect("Unable to write result to file");
    // println!("Result has been saved to {}", OUT_FILE_NAME);

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
    file.meta.push(Pair {key:"hidden".to_string() , value: "true".to_string() });
    let mut meta_data = FileMetadata::default();
    meta_data.content_type = "application/vnd.apache.arrow.file".to_string();
    file.metadata = Some(EFileMetadata { object: Some(e_file_metadata::Object::Filemetadata(meta_data)) });

    let outbound = async_stream::stream! {yield ReqUploadTable {file: Some(EFileDocument { object: Some(e_file_document::Object::Filedocument(file)) }), bytes}};
    let response = tercen_ctx.factory.table_service()?.upload_table(outbound).await?;

    // println!("table_service.upload_table -- {:?}", response);

    let tbl_id = match response.into_inner().result.unwrap().object.unwrap() {
        e_schema::Object::Tableschema(schema) => schema.id,
        e_schema::Object::Computedtableschema(schema) => schema.id,
        e_schema::Object::Cubequerytableschema(schema) => schema.id,
        e_schema::Object::Schema(schema) => schema.id,
    };

    let simple_relation = SimpleRelation {
        id: tbl_id,
        index: 0,
    };

    let mut task = tercen_ctx.get_task().await?;
    task.computed_relation = Some(ERelation { object: Option::from(e_relation::Object::Simplerelation(simple_relation)) });

    tercen_ctx.factory.task_service()?.update(ETask { object: Option::from(e_task::Object::Runcomputationtask(task)) }).await?;

    Ok(())
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

    // println!("sample_meta_factor {:?}", &sample_meta_factor);

    assert_eq!(&sample_meta_factor.ontology_mapping, "sample");
    assert_eq!(&sample_meta_factor.crosstab_mapping, "column");
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

    if TercenArgs::try_parse().is_ok() {
        let input_spec_object = input_spec_object
            .ok_or_else(|| TercenError::new("Operator specification is required."))?;
        let input_spec = match input_spec_object {
            Object::Crosstabspec(input_spec) => Ok(input_spec),
            Object::Operatorinputspec(_) => {
                Err(TercenError::new("Operator specification is required."))
            }
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
                    factors: vec![Factor {
                        name: "filename".to_string(),
                        r#type: "string".to_string(),
                    }],
                })),
            }],
            axis: vec![],
        })
    }
}

fn draw_cube_query(
    global_column_df: &DataFrame,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
    task_id: &str,
    cube_query: &CubeQuery,
    qt_df: DataFrame, ci_df: DataFrame,
    shape_join_df: &DataFrame,
    shapes_by_index: &HashMap<i32, Shape>,
) -> Result<(), Box<dyn Error>> {
    let axis_query = cube_query
        .axis_queries
        .first()
        .ok_or_else(|| TercenError::new("x and y axis are required."))?;

    let palette = create_palette_from_df(axis_query, &qt_df)?;
    let drawing_areas = drawing_area.split_evenly((1, global_column_df.shape().0));
    let data_frames = qt_df.partition_by_stable([".ci"], true)?;

    let ci = ci_df.column(".ci")?.i32()?.into_no_null_iter().collect::<Vec<_>>();
    let ci_global = ci_df.column(".ci.global")?.i32()?.into_no_null_iter().collect::<Vec<_>>();

    println!("ci -- {:?}", &ci);
    println!("ci_global -- {:?}", &ci_global);

    for qt_df in data_frames.into_iter() {
        let current_ci: i32 = qt_df
            .column(".ci")?
            .i32()?
            .into_no_null_iter()
            .next()
            .ok_or_else(|| TercenError::new("failed to get .ci"))?;

        println!("current_ci -- {:?}", &current_ci);

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

        let shapes: Vec<_> = iter_task_id.iter()
            .zip(iter_shape_index)
            .zip(iter_ci_global)
            .filter(|((task, _), ci_glob)| ci_glob.eq(ci_global) && (*task).eq(&task_id.to_string()))
            .map(|((_, shape_index), _)| shape_index)
            .map(|shape_index| shapes_by_index.get(&shape_index))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| TercenError::new("failed to get shapes"))?;

        draw_sample(&axis_query, &palette, drawing_area, qt_df, shapes)?;


        // match draw_samples(&axis_query, &palette, drawing_area, qt_df) {
        //     Ok(_) => {}
        //     Err(e) => {
        //         let text_style = ("sans-serif", 20)
        //             .with_color(RED)
        //             .into_text_style(drawing_area);
        //         drawing_area.draw_text(&e.to_string(), &text_style, (10, 50))?;
        //     }
        // }
    }
    Ok(())
}

fn create_palette_from_df(
    axis_query: &CubeAxisQuery,
    qt_df: &DataFrame) -> Result<JetPalette, Box<dyn Error>> {
    let has_x_axis = axis_query
        .x_axis
        .as_ref()
        .map(|x_factor| !x_factor.name.is_empty())
        .ok_or_else(|| TercenError::new("x_axis is required"))?;

    if has_x_axis {
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
    cube_query: &CubeQuery) -> Result<DataFrame, Box<dyn Error>> {
    let axis_query = cube_query
        .axis_queries
        .first()
        .ok_or_else(|| TercenError::new("axis_query is required"))?;

    let has_x_axis = axis_query
        .x_axis
        .as_ref()
        .map(|x_factor| !x_factor.name.is_empty())
        .ok_or_else(|| TercenError::new("x_axis is required"))?;

    let columns = if has_x_axis {
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
            ".x_bin_size".to_string(),
            ".y".to_string(),
            ".x".to_string(),
            ".ci".to_string(),
        ]
    };


    let qt_df = tercen_ctx
        .select_data_frame_from_id(&cube_query.qt_hash, &columns)
        .await?;

    Ok(qt_df)
}


async fn create_col_df(
    tercen_ctx: &TercenContext,
    sample_meta_factor: &MetaFactor,
    global_column_df: &DataFrame,
    cube_query: &CubeQuery) -> Result<DataFrame, Box<dyn Error>> {
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
    axis_query: &CubeAxisQuery,
    palette: &JetPalette,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
    df: DataFrame,
    shapes: Vec<&Shape>) -> Result<(), Box<dyn Error>> {
    let has_x_axis = axis_query
        .x_axis
        .as_ref()
        .map(|x_factor| !x_factor.name.is_empty())
        .ok_or_else(|| TercenError::new("x_axis is required"))?;

    if has_x_axis {
        draw_sample_density(axis_query, palette, drawing_area, df, shapes)
    } else {
        draw_sample_histogram(axis_query, palette, drawing_area, df, shapes)
    }
}

fn draw_sample_density(
    axis_query: &CubeAxisQuery,
    palette: &JetPalette,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
    df: DataFrame,
    shapes: Vec<&Shape>) -> Result<(), Box<dyn Error>> {
    let x_axis_factor = axis_query
        .x_axis
        .as_ref()
        .ok_or_else(|| TercenError::new("x axis are required."))?;
    let y_axis_factor = axis_query
        .y_axis
        .as_ref()
        .ok_or_else(|| TercenError::new("y axis are required."))?;

    let yy = df.column(".y")?.f64()?;
    let xx = df.column(".x")?.f64()?;
    let hist_count = df.column(".histogram_count")?.f64()?;
    let y_bin_size = df.column(".y_bin_size")?.f64()?;
    let x_bin_size = df.column(".x_bin_size")?.f64()?;

    let x_min = xx
        .into_no_null_iter()
        .min_by(|a, b| a.total_cmp(b))
        .unwrap();
    let x_max = xx
        .into_no_null_iter()
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let y_min = yy
        .into_no_null_iter()
        .min_by(|a, b| a.total_cmp(b))
        .unwrap();
    let y_max = yy
        .into_no_null_iter()
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let mut cc = ChartBuilder::on(drawing_area)
        .margin(5)
        .set_left_and_bottom_label_area_size(50)

        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    cc.configure_mesh()
        .x_desc(&x_axis_factor.name)
        .y_desc(&y_axis_factor.name)
        .x_labels(10)
        .y_labels(10)
        .disable_mesh()
        .x_label_formatter(&|v| format!("{:.1}", v))
        .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()?;


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
                palette.get_color_rgb(histo_count).to_plotters().filled(),
            )
        });

    cc.draw_series(line)?;

    draw_shapes(shapes, &mut cc)?;

    Ok(())
}


fn draw_sample_histogram(
    axis_query: &CubeAxisQuery,
    _palette: &JetPalette,
    drawing_area: &DrawingArea<BitMapBackend, Shift>,
    df: DataFrame,
    shapes: Vec<&Shape>) -> Result<(), Box<dyn Error>> {
    let y_axis_factor = axis_query
        .y_axis
        .as_ref()
        .ok_or_else(|| TercenError::new("y axis are required."))?;

    let yy = df.column(".y")?.f64()?;
    let xx = df.column(".x")?.f64()?;

    // let x_bin_size = df.column(".x_bin_size")?.f64()?;

    let x_min = xx
        .into_no_null_iter()
        .min_by(|a, b| a.total_cmp(b))
        .unwrap();

    let x_max = xx
        .into_no_null_iter()
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let y_min = yy
        .into_no_null_iter()
        .min_by(|a, b| a.total_cmp(b))
        .unwrap();
    let y_max = yy
        .into_no_null_iter()
        .max_by(|a, b| a.total_cmp(b))
        .unwrap();

    let mut cc = ChartBuilder::on(drawing_area)
        .margin(5)
        .set_left_and_bottom_label_area_size(50)
        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    cc.configure_mesh()
        .x_desc(&y_axis_factor.name)
        .y_desc("count")
        .x_labels(10)
        .y_labels(10)
        .disable_mesh()
        .x_label_formatter(&|v| format!("{:.1}", v))
        .y_label_formatter(&|v| format!("{:.1}", v))
        .draw()?;

    cc.draw_series(LineSeries::new(xx.into_no_null_iter().zip(yy.into_no_null_iter()), &BLUE))?;


    draw_shapes(shapes, &mut cc)?;

    Ok(())
}

fn draw_shapes(shapes: Vec<&Shape>,
               cc: &mut ChartContext<BitMapBackend, Cartesian2d<RangedCoordf64, RangedCoordf64>>) -> Result<(), Box<dyn Error>> {
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
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start , cc.y_range().start),
                            (shape.center.x, shape.center.y)
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q2") {
                    let rec = Rectangle::new(
                        [
                            (shape.center.x , cc.y_range().start),
                            (cc.x_range().end , shape.center.y)
                        ],
                        shape_style,
                    );

                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q3") {
                    let rec = Rectangle::new(
                        [
                            (shape.center.x , shape.center.y),
                            (cc.x_range().end , cc.y_range().start)
                        ],
                        shape_style,
                    );
                    cc.draw_series([rec])?;
                } else if shape.quadrant.eq("Q4") {
                    let rec = Rectangle::new(
                        [
                            (cc.x_range().start , shape.center.y),
                            (shape.center.x , cc.y_range().start)
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
