use std::error::Error;
use crate::client::TercenError;
use crate::tercen::{e_schema, CubeQuery, ESchema, Pair, RunComputationTask, ETask};
use crate::tercen::e_column_schema ;
use crate::tercen::e_task ;

pub(crate) trait ETaskHelper {
    fn get_env(&self) -> Result<&[Pair], Box<dyn Error>>;
    fn get_run_computation_task(self) -> Result<RunComputationTask, Box<dyn Error>>;
    fn get_cube_query(&self) -> Result<&CubeQuery, Box<dyn Error>>;
}

pub(crate) trait GetNRows {
    fn get_n_rows(&self) -> Result<i32, Box<dyn Error>>;
}

pub(crate) trait HasColumn {
    fn has_column(&self,name: &str) -> Result<bool, Box<dyn Error>>;
}

pub(crate) trait GetId {
    fn get_id(&self) -> Result<&str, Box<dyn Error>>;
}

impl ETaskHelper for ETask {
    fn get_env(&self) -> Result<&[Pair], Box<dyn Error>> {
        let result = match self.object.as_ref().ok_or_else(|| TercenError::new("ETask : object missing"))?
        {
            e_task::Object::Csvtask(task) => task.environment.as_slice(),
            e_task::Object::Computationtask(task) => task.environment.as_slice(),
            e_task::Object::Creategitoperatortask(task) => task.environment.as_slice(),
            e_task::Object::Cubequerytask(task) => task.environment.as_slice(),
            e_task::Object::Exporttabletask(task) => task.environment.as_slice(),
            e_task::Object::Exportworkflowtask(task) => task.environment.as_slice(),
            e_task::Object::Gitprojecttask(task) => task.environment.as_slice(),
            e_task::Object::Gltask(task) => task.environment.as_slice(),
            e_task::Object::Importgitdatasettask(task) => task.environment.as_slice(),
            e_task::Object::Importgitworkflowtask(task) => task.environment.as_slice(),
            e_task::Object::Importworkflowtask(task) => task.environment.as_slice(),
            e_task::Object::Librarytask(task) => task.environment.as_slice(),
            e_task::Object::Projecttask(task) => task.environment.as_slice(),
            e_task::Object::Runcomputationtask(task) => task.environment.as_slice(),
            e_task::Object::Runwebapptask(task) => task.environment.as_slice(),
            e_task::Object::Runworkflowtask(task) => task.environment.as_slice(),
            e_task::Object::Savecomputationresulttask(task) => task.environment.as_slice(),
            e_task::Object::Task(task) => task.environment.as_slice(),
            e_task::Object::Testoperatortask(task) => task.environment.as_slice(),

        };

        Ok(result )
    }
    fn get_run_computation_task(self) -> Result<RunComputationTask, Box<dyn Error>> {
        match self.object.ok_or_else(|| TercenError::new("ETask : object missing"))? {
            e_task::Object::Runcomputationtask(task) => Ok(task),
            _ => Err(Box::new(TercenError::new("ETask : not a computation task"))),
        }
    }

    fn get_cube_query(&self) -> Result<&CubeQuery, Box<dyn Error>> {
        let result = match self.object.as_ref().ok_or_else(|| TercenError::new("ETask : object missing"))? {
            e_task::Object::Computationtask(task) => &task.query,
            e_task::Object::Cubequerytask(task) => &task.query,
            e_task::Object::Runcomputationtask(task) => &task.query,
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

impl HasColumn for ESchema {
 
    fn has_column(&self, name: &str) -> Result<bool, Box<dyn Error>> {
        let flag = match self.object.as_ref().ok_or_else(|| TercenError::new("ESchema : object missing"))?
        {
            e_schema::Object::Tableschema(schema) => {
                schema.columns.iter().any(|c| match c.object.as_ref().unwrap() {
                    e_column_schema::Object::Column(c) => c.name.eq(name),
                    e_column_schema::Object::Columnschema(c) => c.name.eq(name)
                }) 
            },
            e_schema::Object::Computedtableschema(schema) => {
                schema.columns.iter().any(|c| match c.object.as_ref().unwrap() {
                    e_column_schema::Object::Column(c) => c.name.eq(name),
                    e_column_schema::Object::Columnschema(c) => c.name.eq(name)
                })
            },
            e_schema::Object::Cubequerytableschema(schema) => {
                schema.columns.iter().any(|c| match c.object.as_ref().unwrap() {
                    e_column_schema::Object::Column(c) => c.name.eq(name),
                    e_column_schema::Object::Columnschema(c) => c.name.eq(name)
                })
            },
            e_schema::Object::Schema(schema) => {
                schema.columns.iter().any(|c| match c.object.as_ref().unwrap() {
                    e_column_schema::Object::Column(c) => c.name.eq(name),
                    e_column_schema::Object::Columnschema(c) => c.name.eq(name)
                })
            },
        };

        Ok(flag)
    }
}


