use crate::pregel::Column::{Custom, Dst, Id, Src};
use polars::prelude::*;
use std::fmt::{Debug, Display, Formatter};
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
    /// The `vertices` property is a `DataFrame` that represents the nodes in a graph.
    pub vertices: DataFrame,
    /// The `edges` property is a `DataFrame` that represents the edges of a graph.
    pub edges: DataFrame,
}

/// A new type alias `Result<T>` that is equivalent to the
/// `std::result::Result<T, GraphFrameError>` type. This is a common pattern in
/// Rust to create a shorthand for a longer type name. The `Result<T>` type is used
/// throughout the `GraphFrame` struct to represent the result of a function that
/// can either return a value of type `T` or an error of type `GraphFrameError`.
type Result<T> = std::result::Result<T, GraphFrameError>;

/// `GraphFrameError` is an enum that represents the different types of errors that
/// can occur when working with a `GraphFrame`. It has three variants: `DuckDbError`,
/// `FromPolars` and `MissingColumn`.
#[derive(Debug)]
pub enum GraphFrameError {
    /// `FromPolars` is a variant of `GraphFrameError` that represents errors that
    /// occur when converting from a `PolarsError`.
    FromPolars(PolarsError),
    /// `MissingColumn` is a variant of `GraphFrameError` that represents errors that
    /// occur when a required column is missing from a DataFrame.
    MissingColumn(MissingColumnError),
}

/// This is an implementation of the `Display` trait for the `GraphFrameError` enum.
/// It allows instances of the `GraphFrameError` enum to be formatted as strings
/// when they need to be displayed to the user. The `fmt` method takes a mutable
/// reference to a `Formatter` object and returns a `fmt::Result`. It matches on the
/// enum variants and calls the `Display` trait's `fmt` method on the inner error
/// object to format the error message.
impl Display for GraphFrameError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GraphFrameError::FromPolars(error) => Display::fmt(error, f),
            GraphFrameError::MissingColumn(error) => Display::fmt(error, f),
        }
    }
}

/// This is an implementation of the `Error` trait for a custom error type
/// `GraphFrameError`. It defines the `source` method which returns the underlying
/// cause of the error as an optional reference to a `dyn error::Error` trait
/// object. If the error is caused by a `FromPolars` error, it returns the reference
/// to the underlying error. If the error is caused by a missing column, it returns
/// `None`.
impl error::Error for GraphFrameError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
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
    /// `Id` is a variant of `MissingColumnError` that represents the error that
    /// occurs when the `Id` column is missing from a DataFrame.
    Id,
    /// `Src` is a variant of `MissingColumnError` that represents the error that
    /// occurs when the `Src` column is missing from a DataFrame.
    Src,
    /// `Dst` is a variant of `MissingColumnError` that represents the error that
    /// occurs when the `Dst` column is missing from a DataFrame.
    Dst,
}

impl Display for MissingColumnError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let message = |df, column: &str| format!("Missing column {} in {}", column, df);

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
            .clone() // this is because cloning a DataFrame is cheap
            .lazy()
            .select([col(Src.as_ref()).alias(Id.as_ref())]);
        let dsts = edges
            .clone() // this is because cloning a DataFrame is cheap
            .lazy()
            .select([col(Dst.as_ref()).alias(Id.as_ref())]);
        let vertices = concat([srcs, dsts], true, true)?
            .unique(
                Some(vec![Id.as_ref().to_string()]),
                UniqueKeepStrategy::First,
            )
            .collect()?;

        GraphFrame::new(vertices, edges)
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
            .agg([count().alias(Custom("out_degree").as_ref())])
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
            .agg([count().alias(Custom("in_degree").as_ref())])
            .collect()
    }
}

/// This is the implementation of the `Display` trait for a `GraphFrame` struct.
/// This allows instances of the `GraphFrame` struct to be printed in a formatted
/// way using the `println!` macro or other formatting macros. The `fmt` method is
/// defined to format the output as a string that includes the vertices and edges of
/// the graph.
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

    #[test]
    fn test_new_missing_id_column() {
        let vertices = DataFrame::new(vec![Series::new("its_not_id", [1, 2, 3])]).unwrap();
        let srcs = Series::new("src", [1, 2, 3]);
        let dsts = Series::new("dst", [2, 3, 4]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        match GraphFrame::new(vertices, edges) {
            Ok(_) => panic!("Should have failed"),
            Err(e) => assert_eq!(e.to_string(), "Missing column id in vertices"),
        }
    }

    #[test]
    fn test_new_missing_src_column() {
        let vertices = DataFrame::new(vec![Series::new("id", [1, 2, 3])]).unwrap();
        let srcs = Series::new("its_not_src", [1, 2, 3]);
        let dsts = Series::new("dst", [2, 3, 4]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        match GraphFrame::new(vertices, edges) {
            Ok(_) => panic!("Should have failed"),
            Err(e) => assert_eq!(e.to_string(), "Missing column src in edges"),
        }
    }

    #[test]
    fn test_new_missing_dst_column() {
        let vertices = DataFrame::new(vec![Series::new("id", [1, 2, 3])]).unwrap();
        let srcs = Series::new("src", [1, 2, 3]);
        let dsts = Series::new("its_not_dst", [2, 3, 4]);
        let edges = DataFrame::new(vec![srcs, dsts]).unwrap();
        match GraphFrame::new(vertices, edges) {
            Ok(_) => panic!("Should have failed"),
            Err(e) => assert_eq!(e.to_string(), "Missing column dst in edges"),
        }
    }
}
