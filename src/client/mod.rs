pub mod args;
pub mod palette;
pub mod quartiles;

use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use clap::Parser;
use polars::prelude::*;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Cursor;
use std::str::FromStr;

use crate::client::args::TercenArgs;
use crate::tercen::e_task::Object;
use crate::tercen::file_service_client::FileServiceClient;
use crate::tercen::table_schema_service_client::TableSchemaServiceClient;
use crate::tercen::task_service_client::TaskServiceClient;
use crate::tercen::user_service_client::UserServiceClient;
use crate::tercen::workflow_service_client::WorkflowServiceClient;
use crate::tercen::{
    e_schema, CubeQuery, ESchema, GetRequest, Pair, ReqConnect, ReqGetCubeQuery, ReqStreamTable,
    TableSchema,
};
use tonic::codegen::tokio_stream::StreamExt;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

#[derive(Debug)]
pub struct TercenError {
    message: String,
}

impl From<PolarsError> for TercenError {
    fn from(value: PolarsError) -> Self {
        TercenError::new(&value.to_string())
    }
}

impl TercenError {
    pub fn new(message: &str) -> Self {
        TercenError {
            message: message.to_string(),
        }
    }
}

impl Display for TercenError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TercenError({})", &self.message)
    }
}

impl Error for TercenError {}

pub struct AuthInterceptor {
    authorization: Option<AsciiMetadataValue>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(auth) = &self.authorization {
            request.metadata_mut().insert("authorization", auth.clone());
        }

        Ok(request)
    }
}

pub struct ServiceFactory {
    uri: Uri,
    channel: Channel,
    pub(crate) token: Option<String>,
}

impl ServiceFactory {
    pub async fn new(uri: Uri) -> Result<Self, Box<dyn Error>> {
        let channel = Channel::builder(uri.clone()).connect().await?;
        Ok(ServiceFactory {
            uri,
            channel,
            token: None,
        })
    }

    pub async fn connect(
        &mut self,
        username_or_email: String,
        password: String,
    ) -> Result<(), Box<dyn Error>> {
        let mut client = UserServiceClient::new(self.channel.clone());

        let response = client
            .connect(ReqConnect {
                username_or_email,
                password,
            })
            .await?;

        let req_token = response
            .into_inner()
            .result
            .and_then(|user_session| user_session.token)
            .ok_or_else(|| Box::new(TercenError::new("failed to connect")))?;

        self.token = Some(req_token.token);

        Ok(())
    }

    fn auth_interceptor(&self) -> Result<AuthInterceptor, Box<dyn Error>> {
        let interceptor = match self.token.as_ref() {
            None => AuthInterceptor {
                authorization: None,
            },
            Some(token) => AuthInterceptor {
                authorization: Some(token.parse()?),
            },
        };

        Ok(interceptor)
    }

    pub fn user_service(
        &self,
    ) -> Result<UserServiceClient<InterceptedService<Channel, AuthInterceptor>>, Box<dyn Error>>
    {
        Ok(UserServiceClient::with_interceptor(
            self.channel.clone(),
            self.auth_interceptor()?,
        ))
    }

    pub fn workflow_service(
        &self,
    ) -> Result<WorkflowServiceClient<InterceptedService<Channel, AuthInterceptor>>, Box<dyn Error>>
    {
        Ok(WorkflowServiceClient::with_interceptor(
            self.channel.clone(),
            self.auth_interceptor()?,
        ))
    }

    pub fn table_service(
        &self,
    ) -> Result<
        TableSchemaServiceClient<InterceptedService<Channel, AuthInterceptor>>,
        Box<dyn Error>,
    > {
        Ok(TableSchemaServiceClient::with_interceptor(
            self.channel.clone(),
            self.auth_interceptor()?,
        ))
    }

    pub fn file_service(
        &self,
    ) -> Result<FileServiceClient<InterceptedService<Channel, AuthInterceptor>>, Box<dyn Error>>
    {
        Ok(FileServiceClient::with_interceptor(
            self.channel.clone(),
            self.auth_interceptor()?,
        ))
    }

    pub fn task_service(
        &self,
    ) -> Result<TaskServiceClient<InterceptedService<Channel, AuthInterceptor>>, Box<dyn Error>>
    {
        Ok(TaskServiceClient::with_interceptor(
            self.channel.clone(),
            self.auth_interceptor()?,
        ))
    }
}

pub struct TercenContext {
    factory: ServiceFactory,
}

impl TercenContext {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => {
                let uri : Uri = args.serviceUri.parse()?;
                let mut factory = ServiceFactory::new(uri).await?;
                factory.token = Some(args.token);
                Ok(TercenContext { factory })
            }
            Err(_) => {
                let uri = Uri::from_static("http://localhost:50051");
                let mut factory = ServiceFactory::new(uri).await?;
                factory
                    .connect("admin".to_string(), "admin".to_string())
                    .await?;
                Ok(TercenContext { factory })
            }
        }
    }

    pub async fn get_cube_query_from_task(
        &self,
        task_id: &str,
    ) -> Result<CubeQuery, Box<dyn Error>> {
        let response = self
            .factory
            .task_service()?
            .get(GetRequest {
                id: task_id.to_string(),
            })
            .await?;

        let cube_query = response
            .into_inner()
            .object
            .and_then(|object| match object {
                Object::Runcomputationtask(task) => task.query,
                Object::Cubequerytask(task) => task.query,
                Object::Computationtask(task) => task.query,
                _ => None,
            })
            .ok_or_else(|| TercenError::new("A cube query is required."))?;

        Ok(cube_query)
    }

    pub async fn get_cube_queries_from_task(&self) -> Result<Vec<CubeQuery>, Box<dyn Error>> {
        if (TercenArgs::try_parse().is_ok()) {
            let task_ids = self.get_cube_queries_from_task_ids().await?;
            let mut result = vec![];
            for task_id in task_ids {
                result.push(self.get_cube_query_from_task(&task_id).await?);
            }
            Ok(result)
        } else {
            Ok(vec![
                self.get_cube_query().await?,
                self.get_cube_query().await?,
            ])
        }
    }

    pub async fn get_cube_queries_from_task_ids(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let ids = match TercenArgs::try_parse() {
            Ok(args) => {
                let response = self
                    .factory
                    .task_service()?
                    .get(GetRequest {
                        id: args.taskId.clone(),
                    })
                    .await?;

                let task_env = response
                    .into_inner()
                    .object
                    .and_then(|object| match object {
                        Object::Runcomputationtask(task) => Some(task.environment),
                        Object::Cubequerytask(task) => Some(task.environment),
                        Object::Computationtask(task) => Some(task.environment),
                        _ => None,
                    })
                    .ok_or_else(|| TercenError::new("A task environment is required."))?;

                match task_env
                    .iter()
                    .find(|env| env.key.eq("tercen.gating.task.ids"))
                {
                    None => {
                        vec![]
                    }
                    Some(env) => env
                        .value
                        .split(",")
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>(),
                }
            }
            Err(_) => {
                vec![]
            }
        };

        if ids.is_empty() {
            return Err(Box::new(TercenError::new("Task ids is empty.")));
        }

        Ok(ids)
    }

    pub async fn get_cube_query(&self) -> Result<CubeQuery, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => Ok(self.get_cube_query_from_task(&args.taskId).await?),
            Err(_) => {
                // http://127.0.0.1:5400/test/w/ac401b828c61c1d7ee60a704f298cd6e/ds/1347b4b0-3cdf-4d08-b908-5fe41caa44e2

                let workflow_id = "ac401b828c61c1d7ee60a704f298cd6e".to_string();
                let step_id = "1347b4b0-3cdf-4d08-b908-5fe41caa44e2".to_string();

                let response = self
                    .factory
                    .workflow_service()?
                    .get_cube_query(ReqGetCubeQuery {
                        workflow_id: workflow_id.clone(),
                        step_id: step_id.clone(),
                    })
                    .await?;

                let cube_query = response
                    .into_inner()
                    .result
                    .ok_or_else(|| Box::new(TercenError::new("failed to get_cube_query")))?;

                Ok(cube_query)
            }
        }
    }

    pub async fn schema(&self) -> Result<ESchema, Box<dyn Error>> {
        let cube_query = self.get_cube_query().await?;
        Ok(self.get_schema(&cube_query.qt_hash).await?)
    }

    pub async fn get_schema(&self, id: &str) -> Result<ESchema, Box<dyn Error>> {
        let response = self
            .factory
            .table_service()?
            .get(GetRequest { id: id.to_string() })
            .await?;
        Ok(response.into_inner())
    }

    // pub async fn select(&self, column_names: Vec<String>) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    //     let cube_query = self.get_cube_query().await?;
    //     let mut req = ReqStreamTable::default();
    //     req.offset = 0;
    //     req.limit = schema_n_rows(self.schema().await?) as i64;
    //     req.table_id = cube_query.qt_hash;
    //     req.cnames.extend(column_names);
    //     req.binary_format = "arrow".to_string();
    //
    //     let mut stream = self.factory
    //         .table_service()?
    //         .stream_table(req)
    //         .await?
    //         .into_inner();
    //
    //     let mut results = vec![];
    //
    //
    //     while let Some(evt) = stream.next().await {
    //         let record_bytes = evt?.result;
    //         let mut reader = StreamReader::try_new(&record_bytes[..], None)?;
    //         while let Some(evt) = reader.next() {
    //             results.push(evt?);
    //         }
    //     }
    //
    //     if results.is_empty() {
    //         return Err(Box::new(TercenError::new("results.is_empty")));
    //     }
    //
    //     Ok(results)
    // }

    pub async fn select_data_frame(
        &self,
        column_names: Vec<String>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let cube_query = self.get_cube_query().await?;
        Ok(self
            .select_data_frame_from_id(&cube_query.qt_hash, column_names)
            .await?)
    }

    pub async fn select_data_frame_from_id(
        &self,
        id: &str,
        column_names: Vec<String>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let schema = self.get_schema(&id).await?;
        let n_rows = schema_n_rows(schema) as i64;

        let mut stream = self
            .factory
            .table_service()?
            .stream_table(ReqStreamTable {
                table_id: id.to_string(),
                cnames: column_names,
                offset: 0,
                limit: n_rows,
                binary_format: "arrow".to_string(),
            })
            .await?
            .into_inner();

        let mut bytes = vec![];

        while let Some(evt) = stream.next().await {
            bytes.extend(evt?.result);
        }

        let c = Cursor::new(bytes);
        let reader = IpcStreamReader::new(c);

        reader
            .finish()
            .map_err(|e| Box::new(TercenError::new(&e.to_string())) as Box<dyn Error>)
    }
}

pub fn schema_n_rows(schema: ESchema) -> i32 {
    match &schema.object.unwrap() {
        e_schema::Object::Computedtableschema(schema) => schema.n_rows,
        e_schema::Object::Cubequerytableschema(schema) => schema.n_rows,
        e_schema::Object::Schema(schema) => schema.n_rows,
        e_schema::Object::Tableschema(schema) => schema.n_rows,
    }
}
