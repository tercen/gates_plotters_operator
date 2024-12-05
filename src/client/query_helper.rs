use crate::tercen::{CubeAxisQuery, CubeQuery, Factor};

pub trait CubeQueryHelper {
    fn has_y_axis(&self) -> bool;

    fn has_x_axis(&self) -> bool;

    fn is_2d_histogram(&self) -> bool;

    fn is_histogram(&self) -> bool;

    // fn is_x_axis_double(&self) -> bool;

    fn has_error(&self) -> bool;

    fn has_color(&self) -> bool;
  
}

impl CubeQueryHelper for CubeQuery {
    fn has_y_axis(&self) -> bool {
        self.axis_queries.iter().any(|e| e.has_y_axis())
    }

    fn has_x_axis(&self) -> bool {
        self.axis_queries.iter().any(|e| e.has_x_axis())
    }

    fn is_2d_histogram(&self) -> bool {
        self.is_histogram() && self.has_x_axis() && self.has_y_axis()
    }

    fn is_histogram(&self) -> bool {
        self.axis_queries.iter().any(|e| e.is_histogram())
    }

    // fn is_x_axis_double(&self) -> bool {
    //     self.axis_queries
    //         .iter()
    //         .any(|aq| aq.x_axis.as_ref().map(|a| a.is_double()).unwrap_or(false))
    // }

    fn has_error(&self) -> bool {
        self.axis_queries.iter().any(|e| e.has_error())
    }

    fn has_color(&self) -> bool {
        self.axis_queries.iter().any(|e| e.has_color())
    }
 
}

pub trait CubeAxisQueryHelper {
 

 
    fn has_y_axis(&self) -> bool;

    fn has_x_axis(&self) -> bool;

    fn has_error(&self) -> bool;
    fn has_color(&self) -> bool;

    fn is_histogram(&self) -> bool;

    fn is_2d_histogram(&self) -> bool {
        self.is_histogram() && self.has_x_axis() && self.has_y_axis()
    }
}


impl CubeAxisQueryHelper for CubeAxisQuery {
    

    fn has_y_axis(&self) -> bool {
        self.y_axis.as_ref().map(|a| !a.is_null_factor()).unwrap_or(false)
    }

    fn has_x_axis(&self) -> bool {
        self.x_axis.as_ref().map(|a| !a.is_null_factor()).unwrap_or(false)
    }

    fn has_error(&self) -> bool {
        !self.errors.is_empty()
    }

    fn has_color(&self) -> bool {
        !self.colors.is_empty()
    }

    fn is_histogram(&self) -> bool {
        self.preprocessors
            .iter()
            .any(|preprocessor| preprocessor.operator_ref.as_ref()
                .map(|operator_ref| operator_ref.name.eq("histogram"))
                .unwrap_or(false))
    }
}




pub trait FactorHelper {
  
 
    fn is_null_factor(&self) -> bool;
}

impl FactorHelper for Factor {
    

    fn is_null_factor(&self) -> bool {
        self.name.is_empty()
    }
}