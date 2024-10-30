use std::error::Error;
use crate::client::TercenError;
use crate::tercen::{e_schema, CubeQuery, ESchema, GetRequest, Pair, ReqConnect, ReqGetCubeQuery, ReqStreamTable, TableSchema, e_workflow, RunComputationTask, ETask};
use crate::tercen::e_task::Object;

pub(crate) trait ETaskHelper {
    fn get_env(&self) -> Result<&[Pair], Box<dyn Error>>;
    fn get_run_computation_task(self) -> Result<RunComputationTask, Box<dyn Error>>;
    fn get_cube_query(&self) -> Result<&CubeQuery, Box<dyn Error>>;
}

pub(crate) trait GetNRows {
    fn get_n_rows(&self) -> Result<i32, Box<dyn Error>>;
}

pub(crate) trait GetId {
    fn get_id(&self) -> Result<&str, Box<dyn Error>>;
}

impl ETaskHelper for ETask {
    fn get_env(&self) -> Result<&[Pair], Box<dyn Error>> {
        let result = match self.object.as_ref().ok_or_else(|| TercenError::new("ETask : object missing"))?
        {
            Object::Csvtask(task) => task.environment.as_slice(),
            Object::Computationtask(task) => task.environment.as_slice(),
            Object::Creategitoperatortask(task) => task.environment.as_slice(),
            Object::Cubequerytask(task) => task.environment.as_slice(),
            Object::Exporttabletask(task) => task.environment.as_slice(),
            Object::Exportworkflowtask(task) => task.environment.as_slice(),
            Object::Gitprojecttask(task) => task.environment.as_slice(),
            Object::Gltask(task) => task.environment.as_slice(),
            Object::Importgitdatasettask(task) => task.environment.as_slice(),
            Object::Importgitworkflowtask(task) => task.environment.as_slice(),
            Object::Importworkflowtask(task) => task.environment.as_slice(),
            Object::Librarytask(task) => task.environment.as_slice(),
            Object::Projecttask(task) => task.environment.as_slice(),
            Object::Runcomputationtask(task) => task.environment.as_slice(),
            Object::Runwebapptask(task) => task.environment.as_slice(),
            Object::Runworkflowtask(task) => task.environment.as_slice(),
            Object::Savecomputationresulttask(task) => task.environment.as_slice(),
            Object::Task(task) => task.environment.as_slice(),
            Object::Testoperatortask(task) => task.environment.as_slice(),

        };

        Ok(result )
    }
    fn get_run_computation_task(self) -> Result<RunComputationTask, Box<dyn Error>> {
        match self.object.ok_or_else(|| TercenError::new("ETask : object missing"))? {
            Object::Runcomputationtask(task) => Ok(task),
            _ => Err(Box::new(TercenError::new("ETask : not a computation task"))),
        }
    }

    fn get_cube_query(&self) -> Result<&CubeQuery, Box<dyn Error>> {
        let result = match self.object.as_ref().ok_or_else(|| TercenError::new("ETask : object missing"))? {
            Object::Computationtask(task) => &task.query,
            Object::Cubequerytask(task) => &task.query,
            Object::Runcomputationtask(task) => &task.query,
            _ => &None,
        };

        let result = result.as_ref().ok_or_else(|| TercenError::new("ETask : object missing"))?;

        Ok(result)
    }
}

impl GetId for ESchema {
    fn get_id(&self) -> Result<&str, Box<dyn Error>> {
        let id = match self.object.as_ref().ok_or_else(|| TercenError::new("ESchema : object missing"))?
        {
            e_schema::Object::Tableschema(schema) => &schema.id,
            e_schema::Object::Computedtableschema(schema) => &schema.id,
            e_schema::Object::Cubequerytableschema(schema) => &schema.id,
            e_schema::Object::Schema(schema) => &schema.id,
        };

        Ok(id)
    }
}

impl GetNRows for ESchema {
    fn get_n_rows(&self) -> Result<i32, Box<dyn Error>> {
        let id = match self.object.as_ref().ok_or_else(|| TercenError::new("ESchema : object missing"))?
        {
            e_schema::Object::Tableschema(schema) => schema.n_rows,
            e_schema::Object::Computedtableschema(schema) => schema.n_rows,
            e_schema::Object::Cubequerytableschema(schema) => schema.n_rows,
            e_schema::Object::Schema(schema) => schema.n_rows,
        };

        Ok(id)
    }
}
