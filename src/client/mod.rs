pub mod args;
pub mod palette;
pub mod quartiles;
pub mod shapes;
pub mod utils;
pub mod query_helper;

use clap::Parser;
use polars::prelude::*;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io::Cursor;


use crate::client::args::TercenArgs;
use crate::tercen::e_task::Object;
use crate::tercen::file_service_client::FileServiceClient;
use crate::tercen::table_schema_service_client::TableSchemaServiceClient;
use crate::tercen::task_service_client::TaskServiceClient;
use crate::tercen::user_service_client::UserServiceClient;
use crate::tercen::workflow_service_client::WorkflowServiceClient;
use crate::tercen::{CubeQuery, ESchema, GetRequest, Pair, ReqConnect, ReqStreamTable, e_workflow, ETask};
use tonic::codegen::tokio_stream::{Stream, StreamExt};
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Uri};
use tonic::{Request, Status};

use crate::client::utils::*;

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
    pub factory: ServiceFactory,
}

impl TercenContext {
    pub async fn new() -> Result<Self, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => {
                let uri: Uri = args.serviceUri.parse()?;
                let mut factory = ServiceFactory::new(uri).await?;
                factory.token = Some(args.token);
                Ok(TercenContext { factory })
            }
            Err(_) => {
                // let uri = Uri::from_static("http://localhost:50051");
                let uri = Uri::from_static("http://172.17.0.1:50051");
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
        let task = self.factory
            .task_service()?
            .get(GetRequest { id: task_id.to_string() })
            .await?
            .into_inner();

        let cube_query = task.get_cube_query()?;

        Ok(cube_query.clone())
    }

    pub async fn get_task_from_id(
        &self,
        task_id: &str,
    ) -> Result<ETask, Box<dyn Error>> {
        let task = self.factory
            .task_service()?
            .get(GetRequest { id: task_id.to_string() })
            .await?
            .into_inner();
        Ok(task)
    }

    pub async fn get_task(&self) -> Result<ETask, Box<dyn Error>> {
        Ok(self.factory
            .task_service()?
            .get(GetRequest { id: self.get_task_id()? })
            .await
            .map(|res| res.into_inner())?)
    }

    fn get_task_id(&self) -> Result<String, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => {
                Ok(args.taskId.clone())
            }
            Err(_) => {
                // Err(Box::new(TercenError::new("taskId is required")))
                Ok("7ab92261a0f45c8119450f081e71d16c".to_string())
            }
        }
    }

    pub async fn get_task_env(&self) -> Result<Vec<Pair>, Box<dyn Error>> {
        let task = self.get_task().await?;
        let task_env = task
            .get_env()
            .map_err(|_| TercenError::new("A task environment is required."))?;

        Ok(task_env.to_vec())
    }

    pub async fn get_cube_queries_from_task_ids(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let ids = match TercenArgs::try_parse() {
            Ok(args) => {
                let task = self.factory
                    .task_service()?
                    .get(GetRequest { id: args.taskId.clone() })
                    .await?
                    .into_inner();

                task
                    .get_env()?
                    .iter()
                    .find(|env| env.key.eq("tercen.gating.task.ids"))
                    .map_or(vec![], |env| env
                        .value
                        .split(",")
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>())
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

    fn get_workflow_id(&self) -> Result<String, Box<dyn Error>> {
        Ok("9cbb3d9212ecae4ed01564f4e80fa385".to_string())
    }

    fn get_step_id(&self) -> Result<String, Box<dyn Error>> {
        Ok("a79a5429-6924-42ac-9701-082ee4eb9470".to_string())
    }

    pub async fn get_cube_query(&self) -> Result<CubeQuery, Box<dyn Error>> {
        Ok(self.get_task().await?.get_cube_query()?.clone())

        // match TercenArgs::try_parse() {
        //     Ok(args) => Ok(self.get_cube_query_from_task(&args.taskId).await?),
        //     Err(_) => {
        //         // http://127.0.0.1:5400/test/w/ac401b828c61c1d7ee60a704f298cd6e/ds/1347b4b0-3cdf-4d08-b908-5fe41caa44e2
        //
        //         let workflow_id = self.get_workflow_id()?;
        //         let step_id = self.get_step_id()?;
        //
        //         let response = self
        //             .factory
        //             .workflow_service()?
        //             .get_cube_query(ReqGetCubeQuery {
        //                 workflow_id: workflow_id.clone(),
        //                 step_id: step_id.clone(),
        //             })
        //             .await?;
        //
        //         let cube_query = response
        //             .into_inner()
        //             .result
        //             .ok_or_else(|| Box::new(TercenError::new("failed to get_cube_query")))?;
        //
        //         Ok(cube_query)
        //     }
        // }
    }

    pub async fn schema(&self) -> Result<ESchema, Box<dyn Error>> {
        let cube_query = self.get_task().await?.get_cube_query()?.clone();
        Ok(self.get_schema(&cube_query.qt_hash).await?)
    }

    pub async fn owner(&self) -> Result<String, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => {
                let response = self
                    .factory
                    .task_service()?
                    .get(GetRequest {
                        id: args.taskId.clone(),
                    })
                    .await?;

                let owner = response
                    .into_inner()
                    .object
                    .and_then(|object| match object {
                        Object::Runcomputationtask(task) => Some(task.owner),
                        Object::Cubequerytask(task) => Some(task.owner),
                        Object::Computationtask(task) => Some(task.owner),
                        _ => None,
                    })
                    .ok_or_else(|| TercenError::new("A task impl is required."))?;

                Ok(owner)
            }
            Err(_) => {
                Ok("admin".to_string())
            }
        }
    }

    pub async fn project_id(&self) -> Result<String, Box<dyn Error>> {
        match TercenArgs::try_parse() {
            Ok(args) => {
                let response = self
                    .factory
                    .task_service()?
                    .get(GetRequest {
                        id: args.taskId.clone(),
                    })
                    .await?;

                let owner = response
                    .into_inner()
                    .object
                    .and_then(|object| match object {
                        Object::Runcomputationtask(task) => Some(task.project_id),
                        Object::Cubequerytask(task) => Some(task.project_id),
                        Object::Computationtask(task) => Some(task.project_id),
                        _ => None,
                    })
                    .ok_or_else(|| TercenError::new("A task impl is required."))?;

                Ok(owner)
            }
            Err(_) => {
                let workflow_id = self.get_workflow_id()?;
                let workflow = self.factory.workflow_service()?.get(GetRequest { id: workflow_id }).await?;
                let workflow = match workflow.into_inner().object.unwrap() {
                    e_workflow::Object::Workflow(workflow) => workflow
                };

                Ok(workflow.project_id.clone())
            }
        }
    }

    pub async fn get_schema(&self, id: &str) -> Result<ESchema, Box<dyn Error>> {
        let response = self
            .factory
            .table_service()?
            .get(GetRequest { id: id.to_string() })
            .await?;
        Ok(response.into_inner())
    }

    pub async fn select_data_frame_from_id(
        &self,
        id: &str,
        column_names: &Vec<String>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let mut result = None;

        let mut stream = self.select_stream(id, column_names).await?;

        while let Some(evt) = stream.next().await {
            let data_frame = evt?;

            if result.is_none() {
                result = Some(data_frame);
            } else {
                result
                    .as_mut()
                    .unwrap()
                    .extend(&data_frame)
                    .map_err(|e| Box::new(TercenError::new(&e.to_string())) as Box<dyn Error>)?;
            }
        }

        if result.is_none() {
            Err(Box::new(TercenError::new("select_data_frame_from_id -- empty result !!")) as Box<dyn Error>)
        } else {
            Ok(result.unwrap())
        }
    }

    pub async fn select_stream(
        &self,
        id: &str,
        column_names: &Vec<String>,
    ) -> Result<impl Stream<Item=Result<DataFrame, Box<dyn std::error::Error + 'static>>>, Box<dyn Error>> {

        // Map<Streaming<RespStreamTable>, fn(Result<RespStreamTable, Status>) -> Result<DataFrame, Box<dyn Error>>> {
        let schema = self.get_schema(&id).await?;
        let n_rows = schema.get_n_rows()? as i64;

        let stream = self
            .factory
            .table_service()?
            .stream_table(ReqStreamTable {
                table_id: id.to_string(),
                cnames: column_names.clone(),
                offset: 0,
                limit: n_rows,
                binary_format: "arrow".to_string(),
            })
            .await.unwrap()
            .into_inner();

        Ok(stream.map(|evt| {
            if evt.is_ok() {
                let reader = IpcStreamReader::new(Cursor::new(evt.unwrap().result));
                let data_frame = reader
                    .finish()
                    .map_err(|e| Box::new(TercenError::new(&e.to_string())) as Box<dyn Error>);
                data_frame
            } else {
                let status = evt.unwrap_err();
                Err(Box::new(TercenError::new(&format!("select_stream -- bad status {status}"))) as Box<dyn Error>)
            }
        }))
    }
}
