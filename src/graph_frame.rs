use std::{error, fmt};
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use duckdb::{Connection};
use duckdb::arrow::array::{Array, Int32Array};
use duckdb::arrow::record_batch::RecordBatch;
use polars::prelude::*;
use polars::series::Series;
use crate::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};

pub struct GraphFrame {
    pub vertices: DataFrame,
    pub edges: DataFrame
}

type Result<T> = std::result::Result<T, GraphFrameError>;

#[derive(Debug)]
pub enum GraphFrameError {
    DuckDbError(&'static str),
    FromPolars(PolarsError),
    MissingColumn(MissingColumnError)
}

impl Display for GraphFrameError {

    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphFrameError::DuckDbError(error) => Display::fmt(error, f),
            GraphFrameError::FromPolars(error) => Display::fmt(error, f),
            GraphFrameError::MissingColumn(error) => Display::fmt(error, f),
        }
    }

}

impl error::Error for GraphFrameError {

    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            GraphFrameError::DuckDbError(_) => None,
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

    pub fn from_duckdb(path: &str) -> Result<Self> {
        let database_path = match Path::new(path).try_exists() {
            Ok(true) => Path::new(path),
            Ok(false) => return Err(GraphFrameError::DuckDbError("The provided path does not exist")),
            _ => return Err(GraphFrameError::DuckDbError("Cannot open a connection with the provided Database")),
        };
        let connection = match Connection::open(database_path) {
            Ok(connection) => connection,
            Err(_) => return Err(GraphFrameError::DuckDbError("Cannot connect to the provided Database")),
        };

        let mut statement = match connection.prepare( // TODO: include the rest of the entities
            "select src_id, property_id, dst_id from edge
            union
            select src_id, property_id, dst_id from coordinate
            union
            select src_id, property_id, dst_id from quantity
            union
            select src_id, property_id, dst_id from string
            union
            select src_id, property_id, dst_id from time"
        ) {
            Ok(statement) => statement,
            Err(_) => return Err(GraphFrameError::DuckDbError("Cannot prepare the provided statement")),
        };

        let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
            Ok(arrow) => arrow.collect(),
            Err(_) => return Err(GraphFrameError::DuckDbError("Error executing the Arrow query")),
        };

        let mut dataframe = DataFrame::default();
        for batch in batches {
            let src_id =  batch.column(0); // TODO: by name?
            let property_id =  batch.column(1);
            let src_dst = batch.column(2);

            let srcs = Series::new(
                Src.as_ref(),
                src_id.as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
            );

            let properties = Series::new(
                Custom("property_id".to_string()).as_ref(),
                property_id.as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
            );

            let dsts = Series::new(
                Dst.as_ref(),
                src_dst.as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec()
            );

            let tmp_dataframe = match DataFrame::new(vec![srcs, properties, dsts]) {
                Ok(tmp_dataframe) => tmp_dataframe,
                Err(_) => return Err(GraphFrameError::DuckDbError("Error creating the DataFrame")),
            };

            dataframe = match dataframe.vstack(&tmp_dataframe) {
                Ok(dataframe) => dataframe,
                Err(_) => return Err(GraphFrameError::DuckDbError("Error stacking the DataFrames"))
            };
        }

        GraphFrame::from_edges(dataframe)
    }

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
        write!(f, "Vertices: {}\nEdges: {}", self.vertices, self.edges)
    }

}