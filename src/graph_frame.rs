use std::{error, fmt};
use std::fmt::{Debug, Display, Formatter};
use polars::prelude::*;
use crate::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};

pub struct GraphFrame {
    pub vertices: DataFrame,
    pub edges: DataFrame
}

type Result<T> = std::result::Result<T, GraphFrameError>;

#[derive(Debug)]
pub enum GraphFrameError {
    FromPolars(PolarsError),
    MissingColumn(MissingColumnError)
}

impl Display for GraphFrameError {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphFrameError::FromPolars(error) => std::fmt::Display::fmt(error, f),
            GraphFrameError::MissingColumn(error) => std::fmt::Display::fmt(error, f),
        }
    }

}

impl error::Error for GraphFrameError {

    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            GraphFrameError::FromPolars(ref e) => Some(e),
            GraphFrameError::MissingColumn(_) => None,
        }
    }

}

#[derive(Debug)]
pub enum MissingColumnError {
    Id,
    Src,
    Dst
}

impl Display for MissingColumnError {

    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let message = |df, column: &str|
            format!(
                "The provided {} must contain a column named {} for the Graph to be created",
                df,
                column
            );

        match self {
            MissingColumnError::Id =>  write!(f, "{}", message("vertices", Id.as_ref())),
            MissingColumnError::Src => write!(f, "{}", message("edges", Src.as_ref())),
            MissingColumnError::Dst => write!(f, "{}", message("edges", Dst.as_ref())),
        }
    }

}

impl From<PolarsError> for GraphFrameError {
    fn from(err: PolarsError) -> GraphFrameError {
        GraphFrameError::FromPolars(err)
    }
}

impl GraphFrame {

    pub fn new(vertices: DataFrame, edges: DataFrame) -> Result<Self> {
        if !vertices.get_column_names().contains(&Id.as_ref()) {
            return Err(GraphFrameError::MissingColumn(MissingColumnError::Id));
        }
        if !edges.get_column_names().contains(&Src.as_ref()) {
            return Err(GraphFrameError::MissingColumn(MissingColumnError::Src));
        }
        if !edges.get_column_names().contains(&Dst.as_ref()) {
            return Err(GraphFrameError::MissingColumn(MissingColumnError::Dst));
        }

        Ok(GraphFrame { vertices, edges })
    }

    pub fn from_edges(edges: DataFrame) -> Result<Self> {
        let srcs = edges
            .clone()
            .lazy()
            .select([col(Src.as_ref()).alias(Id.as_ref())]);
        let dsts = edges
            .clone()
            .lazy()
            .select([col(Dst.as_ref()).alias(Id.as_ref())]);
        let vertices = concat([srcs, dsts], false, true)?
            .unique(Some(vec![Id.as_ref().to_string()]), UniqueKeepStrategy::First)
            .collect()?;

        GraphFrame::new(vertices, edges)
    }

    // pub fn from_duckdb() -> Result<Self> {
    //
    //     GraphFrame::new(vertices, edges)
    // }

    pub fn out_degrees(self) -> PolarsResult<DataFrame> {
        self
            .edges
            .lazy()
            .groupby([col(Src.as_ref()).alias(Id.as_ref())])
            .agg([count().alias(Custom("out_degree".to_owned()).as_ref())])
            .collect()
    }

    pub fn in_degrees(self) -> PolarsResult<DataFrame> {
        self
            .edges
            .lazy()
            .groupby([col(Dst.as_ref())])
            .agg([count().alias(Custom("in_degree".to_owned()).as_ref())])
            .collect()
    }

}

impl Display for GraphFrame {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result { // TODO: beautify this :(
        write!(
            f,
            "*) VERTICES:{}\n*) EDGES:{}",
            self.vertices,
            self.edges
        )
    }

}
