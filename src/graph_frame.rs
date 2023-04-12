use crate::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};
use duckdb::arrow::array::{Array, Int32Array};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use polars::prelude::*;
use polars::series::Series;
use std::fmt::{Debug, Display, Formatter};
use std::path::Path;
use std::{error, fmt};

/// The `GraphFrame` type is a struct containing two `DataFrame` fields, `vertices`
/// and `edges`.
///
/// Properties:
///
/// * `vertices`: The `vertices` property is a `DataFrame` that represents the nodes
/// in a graph. It must contain a column named Id.
///
/// * `edges`: The `edges` property is a `DataFrame` that represents the edges of a
/// graph. It must contain -- at least -- two columns: Src and Dst.
pub struct GraphFrame {
    pub vertices: DataFrame,
    pub edges: DataFrame,
}

type Result<T> = std::result::Result<T, GraphFrameError>;

/// `GraphFrameError` is an enum that represents the different types of errors that
/// can occur when working with a `GraphFrame`. It has three variants: `DuckDbError`,
/// `FromPolars` and `MissingColumn`.
#[derive(Debug)]
pub enum GraphFrameError {
    DuckDbError(&'static str),
    FromPolars(PolarsError),
    MissingColumn(MissingColumnError),
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

/// `MissingColumnError` is an enum that represents errors that occur when a
/// required column is missing from a DataFrame. The `Debug` trait allows for easy
///  debugging of the enum by printing its values in a formatted way.
#[derive(Debug)]
pub enum MissingColumnError {
    Id,
    Src,
    Dst,
}

impl Display for MissingColumnError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let message = |df, column: &str| {
            format!(
                "The provided {} must contain a column named {} for the Graph to be created",
                df, column
            )
        };

        match self {
            MissingColumnError::Id => write!(f, "{}", message("vertices", Id.as_ref())),
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
    /// The function creates a new GraphFrame object with given vertices and edges
    /// DataFrames, checking for required columns.
    ///
    /// Arguments:
    ///
    /// * `vertices`: A DataFrame containing information about the vertices of the
    /// graph, such as their IDs and attributes.
    ///
    /// * `edges`: A DataFrame containing information about the edges in the graph. It
    /// should have columns named "src" and "dst" to represent the source and
    /// destination vertices of each edge.
    ///
    /// Returns:
    ///
    /// a `Result<Self>` where `Self` is the `GraphFrame` struct. The `Ok` variant of
    /// the `Result` contains an instance of `GraphFrame` initialized with the provided
    /// `vertices` and `edges` DataFrames. If any of the required columns (`Id`, `Src`,
    /// `Dst`) are missing in the DataFrames, the function returns an `Error`.
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

    /// This function creates a new `GraphFrame` from a given set of edges by selecting
    /// source and destination vertices and concatenating them into a unique set of
    /// vertices.
    ///
    /// Arguments:
    ///
    /// * `edges`: A DataFrame containing the edges of a graph, with at least two
    /// columns named "src" and "dst" representing the source and destination vertices
    /// of each edge.
    ///
    /// Returns:
    ///
    /// The `from_edges` function returns a `Result<Self>` where `Self` is the
    /// `GraphFrame` struct.
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
            .unique(
                Some(vec![Id.as_ref().to_string()]),
                UniqueKeepStrategy::First,
            )
            .collect()?;

        GraphFrame::new(vertices, edges)
    }

    /// This function creates a `GraphFrame` from data stored in a DuckDB database.
    ///
    /// Arguments:
    ///
    /// * `path`: A string representing the path to a DuckDB database file.
    ///
    /// Returns:
    ///
    /// a `Result<Self>` where `Self` is the `GraphFrame` struct.
    pub fn from_duckdb(path: &str) -> Result<Self> {
        let database_path = match Path::new(path).try_exists() {
            Ok(true) => Path::new(path),
            Ok(false) => {
                return Err(GraphFrameError::DuckDbError(
                    "The provided path does not exist",
                ))
            }
            _ => {
                return Err(GraphFrameError::DuckDbError(
                    "Cannot open a connection with the provided Database",
                ))
            }
        };
        let connection = match Connection::open(database_path) {
            Ok(connection) => connection,
            Err(_) => {
                return Err(GraphFrameError::DuckDbError(
                    "Cannot connect to the provided Database",
                ))
            }
        };

        let mut statement = match connection.prepare(
            // TODO: include the rest of the entities
            "select src_id, property_id, dst_id from edge
            union
            select src_id, property_id, dst_id from coordinate
            union
            select src_id, property_id, dst_id from quantity
            union
            select src_id, property_id, dst_id from string
            union
            select src_id, property_id, dst_id from time",
        ) {
            Ok(statement) => statement,
            Err(_) => {
                return Err(GraphFrameError::DuckDbError(
                    "Cannot prepare the provided statement",
                ))
            }
        };

        let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
            Ok(arrow) => arrow.collect(),
            Err(_) => {
                return Err(GraphFrameError::DuckDbError(
                    "Error executing the Arrow query",
                ))
            }
        };

        let mut dataframe = DataFrame::default();
        for batch in batches {
            let src_id = batch.column(0); // TODO: by name?
            let property_id = batch.column(1);
            let src_dst = batch.column(2);

            let srcs = Series::new(
                Src.as_ref(),
                src_id
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec(),
            );

            let properties = Series::new(
                Custom("property_id".to_string()).as_ref(),
                property_id
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec(),
            );

            let dsts = Series::new(
                Dst.as_ref(),
                src_dst
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec(),
            );

            let tmp_dataframe = match DataFrame::new(vec![srcs, properties, dsts]) {
                Ok(tmp_dataframe) => tmp_dataframe,
                Err(_) => return Err(GraphFrameError::DuckDbError("Error creating the DataFrame")),
            };

            dataframe = match dataframe.vstack(&tmp_dataframe) {
                Ok(dataframe) => dataframe,
                Err(_) => {
                    return Err(GraphFrameError::DuckDbError(
                        "Error stacking the DataFrames",
                    ))
                }
            };
        }

        GraphFrame::from_edges(dataframe)
    }

    /// This function calculates the out-degree of each node in a graph represented by
    /// its edges. The out-degree of a node is defined as the number of out-going edges;
    /// that is, edges that have as a source the actual node, and as a destination any
    /// other node in a directed-graph.
    ///
    /// Returns:
    ///
    /// This function returns a `Result` containing a `DataFrame`. The `DataFrame`
    /// contains the out-degree of each node in the graph represented by the `Graph`
    /// object. The original `DataFrame` is preserved; that is, we extend it with
    /// the out-degrees of each node.
    pub fn out_degrees(self) -> PolarsResult<DataFrame> {
        self.edges
            .lazy()
            .groupby([col(Src.as_ref()).alias(Id.as_ref())])
            .agg([count().alias(Custom("out_degree".to_owned()).as_ref())])
            .collect()
    }

    /// This function calculates the in-degree of each node in a graph represented by
    /// its edges. The out-degree of a node is defined as the number of incoming edges;
    /// that is, edges that have as a source any node, and as a destination the node
    /// itself in a directed-graph.
    ///
    /// Returns:
    ///
    /// This function returns a `Result` containing a `DataFrame`. The `DataFrame`
    /// contains the in-degree of each node in the graph represented by the `Graph`
    /// object. The original `DataFrame` is preserved; that is, we extend it with
    /// the in-degrees of each node.
    pub fn in_degrees(self) -> PolarsResult<DataFrame> {
        self.edges
            .lazy()
            .groupby([col(Dst.as_ref())])
            .agg([count().alias(Custom("in_degree".to_owned()).as_ref())])
            .collect()
    }
}

impl Display for GraphFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GraphFrame:\nVertices:\n{}\nEdges:\n{}",
            self.vertices, self.edges
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::graph_frame::GraphFrame;
    use polars::prelude::*;

    #[test]
    fn test_from_edges() {
        let srcs = Series::new("src", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let dsts = Series::new("dst", [2, 3, 4, 5, 6, 7, 8, 9, 10, 1]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        let graphframe = GraphFrame::from_edges(edges).unwrap();
        assert_eq!(graphframe.vertices.height(), 10);
        assert_eq!(graphframe.edges.height(), 10);
    }

    #[test]
    fn test_in_degree() {
        let srcs = Series::new("src", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let dsts = Series::new("dst", [2, 3, 4, 5, 6, 7, 8, 9, 10, 1]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        let graphframe = GraphFrame::from_edges(edges).unwrap();
        let in_degree = graphframe.in_degrees().unwrap();
        assert_eq!(in_degree.height(), 10);
        assert_eq!(
            in_degree
                .column("in_degree")
                .unwrap()
                .u32()
                .unwrap()
                .sum()
                .unwrap(),
            10
        );
    }

    #[test]
    fn test_out_degree() {
        let srcs = Series::new("src", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let dsts = Series::new("dst", [2, 3, 4, 5, 6, 7, 8, 9, 10, 1]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        let graphframe = GraphFrame::from_edges(edges).unwrap();
        let out_degree = graphframe.out_degrees().unwrap();
        assert_eq!(out_degree.height(), 10);
        assert_eq!(
            out_degree
                .column("out_degree")
                .unwrap()
                .u32()
                .unwrap()
                .sum()
                .unwrap(),
            10
        );
    }
}
