use std::fmt;
use std::fmt::{Formatter};
use polars::prelude::*;

pub const ID: &str = "id";
pub const SRC: &str = "src";
pub const DST: &str = "dst";
pub const EDGE: &str = "edge";
pub const MSG: &str = "msg";

pub struct GraphFrame {
    pub vertices: DataFrame,
    pub edges: DataFrame
}

type Result<T> = std::result::Result<T, GraphFrameError>;

#[derive(Debug)]
pub enum GraphFrameError {
    MissingIdError,
    MissingSrcError,
    MissingDstError
}

impl fmt::Display for GraphFrameError {

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = |df, column|
            format!("The vertices {} must contain a {} for the Graph to be created", df, column);
        match self {
            GraphFrameError::MissingIdError =>  write!(f, "{}", message("vertices", ID)),
            GraphFrameError::MissingSrcError => write!(f, "{}", message("edges", SRC)),
            GraphFrameError::MissingDstError => write!(f, "{}", message("edges", DST)),
        }
    }

}

impl GraphFrame {

    pub fn new(vertices: DataFrame, edges: DataFrame) -> Result<GraphFrame> {
        if vertices.get_column_names().contains(&ID) == false {
            return Err(GraphFrameError::MissingIdError);
        }
        if edges.get_column_names().contains(&SRC) == false {
            return Err(GraphFrameError::MissingSrcError);
        }
        if edges.get_column_names().contains(&DST) == false {
            return Err(GraphFrameError::MissingDstError);
        }

        Ok(GraphFrame{ vertices, edges })
    }

}

impl fmt::Display for GraphFrame {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Vertices: {}\nEdges: {}", self.vertices, self.edges)
    }

}
