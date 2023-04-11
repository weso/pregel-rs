use crate::graph_frame::GraphFrame;
use polars::prelude::*;

/// This defines an enumeration type `ColumnIdentifier` in Rust. It  has several
/// variants: `Id`, `Src`, `Dst`, `Edge`, `Msg`, `Pregel`, and `Custom` which
/// takes a `String` parameter. This enum can be used to represent different
/// types of columns in a data structure or database table for it to be used
/// in a Pregel program.
pub enum ColumnIdentifier {
    Id,
    Src,
    Dst,
    Edge,
    Msg,
    Pregel,
    Custom(String),
}

impl From<String> for ColumnIdentifier {
    fn from(value: String) -> Self {
        match &*value {
            "id" => ColumnIdentifier::Id,
            "src" => ColumnIdentifier::Src,
            "dst" => ColumnIdentifier::Dst,
            "edge" => ColumnIdentifier::Edge,
            "msg" => ColumnIdentifier::Msg,
            "_pregel_msg_" => ColumnIdentifier::Pregel,
            _ => ColumnIdentifier::Custom(value),
        }
    }
}

impl AsRef<str> for ColumnIdentifier {
    fn as_ref(&self) -> &str {
        match self {
            ColumnIdentifier::Id => "id",
            ColumnIdentifier::Src => "src",
            ColumnIdentifier::Dst => "dst",
            ColumnIdentifier::Edge => "edge",
            ColumnIdentifier::Msg => "msg",
            ColumnIdentifier::Pregel => "_pregel_msg_",
            ColumnIdentifier::Custom(id) => id,
        }
    }
}

/// The Pregel struct represents a Pregel computation with various parameters and
/// expressions.
///
/// Properties:
///
/// * `graph`: The `graph` property is a `GraphFrame` struct that represents the
/// graph data structure used in the Pregel algorithm. It contains information about
/// the vertices and edges of the graph.
///
/// * `max_iterations`: The maximum number of iterations that the Pregel algorithm
/// will run for.
///
/// * `vertex_column`: `vertex_column` is a property of the `PregelBuilder` struct
/// that represents the name of the column in the graph's vertex DataFrame that
/// contains the vertex IDs. This column is used to identify and locate a column
/// where we apply some of the provided operations during the Pregel computation.
///
/// * `initial_message`: `initial_message` is an expression that defines the initial
/// message that each vertex in the graph will receive before the computation
/// starts. This message can be used to initialize the state of each vertex or to
/// provide some initial information to the computation.
///
/// * `send_messages`: `send_messages` is a tuple containing two expressions. The
/// first expression represents the message sending function that determines whether
/// the message will go from Src to Dst or viceversa. The second expression
/// represents the message sending function that determines which messages to send
///  from a vertex to its neighbors.
///
/// * `aggregate_messages`: `aggregate_messages` is an expression that defines how
/// messages sent to a vertex should be aggregated. In Pregel, messages are sent
/// from one vertex to another and can be aggregated before being processed by the
/// receiving vertex. The `aggregate_messages` expression specifies how these
/// messages should be combined.
///
/// * `v_prog`: `v_prog` is an expression that defines the vertex program for the
/// Pregel algorithm. It specifies the computation that each vertex performs during
/// each iteration of the algorithm. The vertex program can take as input the current
/// state of the vertex, the messages received from its neighbors or and any other
/// relevant information.
pub struct Pregel {
    graph: GraphFrame,
    max_iterations: u8,
    vertex_column: ColumnIdentifier,
    initial_message: Expr,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
    v_prog: Expr,
}

/// The `PregelBuilder` struct represents a builder for configuring the Pregel
/// algorithm to be executed on a given graph.
///
/// Properties:
///
/// * `graph`: The `graph` property is a `GraphFrame` struct that represents the
/// graph data structure used in the Pregel algorithm. It contains information about
/// the vertices and edges of the graph.
///
/// * `max_iterations`: The maximum number of iterations that the Pregel algorithm
/// will run for.
///
/// * `vertex_column`: `vertex_column` is a property of the `PregelBuilder` struct
/// that represents the name of the column in the graph's vertex DataFrame that
/// contains the vertex IDs. This column is used to identify and locate a column
/// where we apply some of the provided operations during the Pregel computation.
///
/// * `initial_message`: `initial_message` is an expression that defines the initial
/// message that each vertex in the graph will receive before the computation
/// starts. This message can be used to initialize the state of each vertex or to
/// provide some initial information to the computation.
///
/// * `send_messages`: `send_messages` is a tuple containing two expressions. The
/// first expression represents the message sending function that determines whether
/// the message will go from Src to Dst or viceversa. The second expression
/// represents the message sending function that determines which messages to send
///  from a vertex to its neighbors.
///
/// * `aggregate_messages`: `aggregate_messages` is an expression that defines how
/// messages sent to a vertex should be aggregated. In Pregel, messages are sent
/// from one vertex to another and can be aggregated before being processed by the
/// receiving vertex. The `aggregate_messages` expression specifies how these
/// messages should be combined.
///
/// * `v_prog`: `v_prog` is an expression that defines the vertex program for the
/// Pregel algorithm. It specifies the computation that each vertex performs during
/// each iteration of the algorithm. The vertex program can take as input the current
/// state of the vertex, the messages received from its neighbors or and any other
/// relevant information.
pub struct PregelBuilder {
    graph: GraphFrame,
    max_iterations: u8,
    vertex_column: ColumnIdentifier,
    initial_message: Expr,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
    v_prog: Expr,
}

/// This code is defining an enumeration type `MessageReceiver` in Rust with
/// two variants: `Src` and `Dst`. This can be used to represent the source and
/// destination of a message in a Pregel program.
pub enum MessageReceiver {
    Src,
    Dst,
}

impl From<MessageReceiver> for ColumnIdentifier {
    fn from(message_receiver: MessageReceiver) -> ColumnIdentifier {
        match message_receiver {
            MessageReceiver::Src => ColumnIdentifier::Src,
            MessageReceiver::Dst => ColumnIdentifier::Dst,
        }
    }
}

impl PregelBuilder {
    /// This function creates a new instance of a PregelBuilder with default values.
    ///
    /// Arguments:
    ///
    /// * `graph`: The graph parameter is of type GraphFrame and represents the graph on
    /// which the Pregel algorithm will be executed.
    ///
    /// Returns:
    ///
    /// A new instance of the `PregelBuilder` struct.
    pub fn new(graph: GraphFrame) -> Self {
        PregelBuilder {
            graph,
            max_iterations: 10,
            vertex_column: ColumnIdentifier::Custom("aux".to_owned()),
            initial_message: Default::default(),
            send_messages: (Default::default(), Default::default()),
            aggregate_messages: Default::default(),
            v_prog: Default::default(),
        }
    }

    /// This function sets the maximum number of iterations for the Pregel algorithm and
    /// returns the modified PregelBuilder.
    ///
    /// Arguments:
    ///
    /// * `max_iterations`: The `max_iterations` parameter is an unsigned 8-bit integer
    /// that represents the maximum number of iterations that the Pregel algorithm or
    /// process can perform. This method sets the `max_iterations` field of a struct to
    /// the provided value and returns the modified struct instance.
    ///
    /// Returns:
    ///
    /// The `max_iterations` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn max_iterations(mut self, max_iterations: u8) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    /// This function sets the vertex column identifier for a struct and returns the
    /// struct.
    ///
    /// Arguments:
    ///
    /// * `vertex_column`: `vertex_column` is a parameter of type `ColumnIdentifier`
    /// which represents the column identifier for the vertex column in a graph
    /// database. The `with_vertex_column` method takes in a `ColumnIdentifier` value
    /// and sets it as the `vertex_column` property of the struct instance. It then
    /// returns
    ///
    /// Returns:
    ///
    /// The `with_vertex_column` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn with_vertex_column(mut self, vertex_column: ColumnIdentifier) -> Self {
        self.vertex_column = vertex_column;
        self
    }

    /// This function sets the initial message for a Rust struct and returns the struct.
    ///
    /// Arguments:
    ///
    /// * `initial_message`: `initial_message` is a parameter of type `Expr` that is
    /// used in a method of a struct. The method takes ownership of `self` and returns
    /// it after setting the `initial_message` field of the struct to the value of the
    /// `initial_message` parameter. This method can be used
    ///
    /// Returns:
    ///
    /// The `initial_message` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn initial_message(mut self, initial_message: Expr) -> Self {
        self.initial_message = initial_message;
        self
    }

    /// This function sets the message sending behavior for a Pregel computation in
    /// Rust.
    ///
    /// Arguments:
    ///
    /// * `to`: `to` is a parameter of type `MessageReceiver`. It represents the
    /// destination vertex or vertices to which messages will be sent during the
    /// computation.
    /// * `send_messages`: `send_messages` is a parameter of type `Expr`. It is used to
    /// specify the function that will be applied to each vertex in the graph to send
    /// messages to its neighboring vertices. The `send_messages` function takes two
    /// arguments: the first argument is the vertex ID of the current vertex, and
    ///
    /// Returns:
    ///
    /// The `send_messages` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn send_messages(mut self, to: MessageReceiver, send_messages: Expr) -> Self {
        self.send_messages = (Pregel::edge(MessageReceiver::into(to)), send_messages);
        self
    }

    /// This function sets the aggregate_messages field of a struct to a given
    /// expression and returns the struct.
    ///
    /// Arguments:
    ///
    /// * `aggregate_messages`: `aggregate_messages` is a parameter of type `Expr` that
    /// is used in a method of a struct. The method takes ownership of the struct
    /// instance (`self`) and assigns the value of `aggregate_messages` to the
    /// corresponding field of the struct. The method then returns the modified struct
    /// instance. This
    ///
    /// Returns:
    ///
    /// The `aggregate_messages` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn aggregate_messages(mut self, aggregate_messages: Expr) -> Self {
        self.aggregate_messages = aggregate_messages;
        self
    }

    /// This function sets the value of a field in a struct and returns the struct
    /// itself.
    ///
    /// Arguments:
    ///
    /// * `v_prog`: `v_prog` is a parameter of type `Expr` that is being passed to a
    /// method called `v_prog`. The method takes ownership of `self` (which is of the
    /// same type as the struct or object that the method belongs to) and assigns the
    /// value of `v_prog` to
    ///
    /// Returns:
    ///
    /// The `v_prog` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn v_prog(mut self, v_prog: Expr) -> Self {
        self.v_prog = v_prog;
        self
    }

    /// The function returns a Pregel struct with the specified properties. This is,
    /// Pregel structs are to be created using the `Builder Pattern`, a creational
    /// design pattern that provides a way to construct complex structs in a
    /// step-by-step manner, allowing for the creation of several different configurations
    /// or variations of the same struct without directly exposing the underlying
    /// complexity. It allows for flexibility in creating different variations of
    /// objects while keeping the construction process modular and manageable.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let vertices = ...
    /// let edges = ...
    ///
    /// let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
    ///     .max_iterations(4)
    ///     .with_vertex_column(Custom("max_value".to_owned()))
    ///     .initial_message(col(Custom("value".to_owned()).as_ref()))
    ///     .send_messages(MessageReceiver::Dst, Pregel::src(Custom("max_value".to_owned())))
    ///     .aggregate_messages(Pregel::msg(None).max())
    ///     .v_prog(max_exprs([col(Custom("max_value".to_owned()).as_ref()), Pregel::msg(None)]))
    ///     .build();
    /// ```
    ///
    /// Returns:
    ///
    /// The `build` method is returning an instance of the `Pregel` struct with the
    /// values of the fields set to the values of the corresponding fields in the
    /// `Builder` struct.
    pub fn build(self) -> Pregel {
        Pregel {
            graph: self.graph,
            max_iterations: self.max_iterations,
            vertex_column: self.vertex_column,
            initial_message: self.initial_message,
            send_messages: self.send_messages,
            aggregate_messages: self.aggregate_messages,
            v_prog: self.v_prog,
        }
    }
}

impl Pregel {
    fn alias(prefix: &ColumnIdentifier, column_name: ColumnIdentifier) -> String {
        format!("{}.{}", prefix.as_ref(), column_name.as_ref())
    }

    fn prefix_columns(expr: Expr, prefix: &'static ColumnIdentifier) -> Expr {
        expr.map_alias(|column_name| {
            let column_identifier = ColumnIdentifier::from(column_name.to_string());
            Ok(Self::alias(prefix, column_identifier))
        })
    }

    /// This function returns an expression for a column identifier representing
    ///  the source vertex in a Pregel graph.
    ///
    /// Arguments:
    ///
    /// * `column_name`: `column_name` is a parameter of type `ColumnIdentifier`. It is
    /// used to identify the name of a column in a table or data source. The `src`
    /// function takes this parameter and returns an expression that represents the
    /// value of the column with the given name.
    ///
    /// Returns:
    ///
    /// The function `src` returns an `Expr` which represents a reference to the source
    /// vertex ID column in a Pregel graph computation. The `Expr` is created using the
    /// `col` function and the `alias` method of the `Pregel` struct to generate the
    /// appropriate column name.
    pub fn src(column_name: ColumnIdentifier) -> Expr {
        col(&Pregel::alias(&ColumnIdentifier::Src, column_name))
    }

    /// This function returns an expression for a column identifier representing
    ///  the destination vertex in a PRegel graph.
    ///
    /// Arguments:
    ///
    /// * `column_name`: `column_name` is a parameter of type `ColumnIdentifier` which
    /// represents the name of a column in a table. It is used as an argument to the
    /// `dst` function to create an expression that refers to the column with the given
    /// name in the context of a Pregel computation.
    ///
    /// Returns:
    ///
    /// The function `dst` returns an expression that represents the value of the column
    /// with the given `column_name` in the context of a `Pregel` graph computation. The
    /// expression is created using the `col` function and the `alias` method of the
    /// `Pregel` struct to ensure that the column name is properly qualified.
    pub fn dst(column_name: ColumnIdentifier) -> Expr {
        col(&Pregel::alias(&ColumnIdentifier::Dst, column_name))
    }

    /// This function returns an expression for a column name in a graph edge table.
    ///
    /// Arguments:
    ///
    /// * `column_name`: `column_name` is a parameter of type `ColumnIdentifier` which
    /// represents the name of a column in a graph edge table. The `edge` function
    /// returns an expression that refers to this column using the `col` function and
    /// the `alias` function from the `Pregel` struct.
    ///
    /// Returns:
    ///
    /// The function `edge` returns an `Expr` which represents a reference to a column
    /// in a graph edge table. The column name is passed as an argument to the function
    /// and is used to construct the full column identifier using the `Pregel::alias`
    /// method.
    pub fn edge(column_name: ColumnIdentifier) -> Expr {
        col(&Pregel::alias(&ColumnIdentifier::Edge, column_name))
    }

    /// This function returns an expression for a column name, either using a default
    /// value or a specified value.
    ///
    /// Arguments:
    ///
    /// * `column_name`: An optional parameter of type `ColumnIdentifier`. It represents
    /// the name of a column in a table. If it is `None`, the function returns a
    /// reference to the `Pregel` column. If it is `Some(column_name)`, the function
    /// returns a reference to a column with the name
    ///
    /// Returns:
    ///
    /// an `Expr` which is either a reference to the `ColumnIdentifier::Pregel` column
    /// if `column_name` is `None`, or a reference to a column with an alias created by
    /// `Pregel::alias` if `column_name` is `Some`.
    pub fn msg(column_name: Option<ColumnIdentifier>) -> Expr {
        match column_name {
            None => col(ColumnIdentifier::Pregel.as_ref()),
            Some(column_name) => col(&Pregel::alias(&ColumnIdentifier::Msg, column_name)),
        }
    }

    /// Represents the Pregel model for large-scale graph processing, introduced
    /// by Google in a paper titled "Pregel: A System for Large-Scale Graph
    /// Processing" in 2010.
    ///
    /// The Pregel model is a distributed computing model for processing graph data
    /// in a distributed and parallel manner. It is designed for efficiently processing
    /// large-scale graphs with billions or trillions of vertices and edges.
    ///
    /// # Components
    ///
    /// - Vertices: Represent the entities in the graph and store the local state
    ///   of each entity. Vertices can perform computations and communicate with their
    ///   neighboring vertices.
    ///
    /// - Edges: Represent the relationships between vertices and are used for
    ///   communication between vertices during computation.
    ///
    /// - Computation: Each vertex performs a user-defined computation during each
    ///   superstep, based on its local state and the messages received from its
    ///   neighboring vertices.
    ///
    /// - Messages: Vertices can send messages to their neighboring vertices during
    ///   each superstep, which are then delivered in the next superstep. Messages
    ///   are used for communication and coordination between vertices.
    ///
    /// - Aggregators: functions that can be used to collect and aggregate
    ///   information from vertices during computation. Aggregators allow for global
    ///   coordination and information gathering across the entire graph.
    ///
    /// # Usage
    ///
    /// The Pregel model follows a sequence of supersteps, where each superstep consists
    /// of computation, message exchange, and aggregation. Vertices perform their computations
    /// in parallel, and messages are exchanged between vertices to coordinate their activities.
    /// The computation continues in a series of supersteps until a termination condition is met.
    ///
    /// This Rust function provides an implementation of the Pregel model for graph processing,
    /// allowing users to run vertex-centric algorithms that operate on the local state
    /// of each vertex and communicate through messages.
    ///
    /// # Notes
    ///
    /// - This is a simplified example of the Pregel model and may require further customization
    ///   based on the specific graph processing requirements.
    ///
    /// Returns:
    ///
    /// a `PolarsResult<DataFrame>`, which is a result type that can either contain
    /// the resulting `DataFrame` or an error of type `PolarsError`.
    pub fn run(self) -> PolarsResult<DataFrame> {
        let (send_messages_ids, send_messages_msg) = self.send_messages;
        let (send_messages_ids, send_messages_msg) = (
            send_messages_ids.alias(&Self::alias(&ColumnIdentifier::Msg, ColumnIdentifier::Id)),
            send_messages_msg.alias(ColumnIdentifier::Pregel.as_ref()),
        );
        // We start the execution of the algorithm from the super-step 0; that is, all the nodes
        // are set to active, and the initial messages are sent to each vertex in the graph
        let mut current_vertices: DataFrame = match self
            .graph
            .vertices
            .clone()
            .lazy()
            .select(vec![
                all(),
                self.initial_message.alias(self.vertex_column.as_ref()),
            ])
            .collect()
        {
            Ok(current_vertices) => current_vertices,
            Err(error) => return Err(error),
        };
        let edges = self
            .graph
            .edges
            .lazy()
            .select([Self::prefix_columns(all(), &ColumnIdentifier::Edge)]);
        // After computing the super-step 0, we start the execution of the Pregel algorithm. This
        // execution is performed until all the nodes vote to halt, or the number of iterations is
        // greater than the maximum number of iterations set by the user at the initialization of
        // the model
        let mut iteration = 1;
        while iteration <= self.max_iterations {
            // TODO: check that nodes are not halted :D
            // 0. We create the triplets DataFrame
            let triplets_df = current_vertices
                .clone()
                .lazy()
                .select([Self::prefix_columns(all(), &ColumnIdentifier::Src)])
                .inner_join(
                    edges.clone(),
                    Self::src(ColumnIdentifier::Id),
                    Self::edge(ColumnIdentifier::Src),
                )
                .inner_join(
                    current_vertices
                        .clone()
                        .lazy()
                        .select([Self::prefix_columns(all(), &ColumnIdentifier::Dst)]),
                    Self::edge(ColumnIdentifier::Dst),
                    Self::dst(ColumnIdentifier::Id),
                );
            // 1. Generate the messages for the current iteration
            let message_df = triplets_df
                .select(vec![send_messages_ids.clone(), send_messages_msg.clone()])
                .groupby([Self::msg(Some(ColumnIdentifier::Id))])
                .agg([self.aggregate_messages.clone()]);
            // 2. Compute the new values for the vertices. Note that we have to check for possibly
            // null values after performing the outer join. This is, columns where the join key does
            // not exist in the source DataFrame.  In case we find any; for example, given a certain
            // having no incoming edges, we have to replace the null value by 0, meaning the sum
            // performed in the previous aggregation is 0, as no edges have as a destination such
            // a vertex.
            let vertex_columns = current_vertices
                .clone()
                .lazy()
                .outer_join(
                    message_df,
                    col(ColumnIdentifier::Id.as_ref()),
                    Self::msg(Some(ColumnIdentifier::Id)),
                )
                .with_column(
                    when(Self::msg(None).is_null())
                        .then(0)
                        .otherwise(Self::msg(None))
                        .alias(ColumnIdentifier::Pregel.as_ref()),
                )
                .select(
                    // TODO: fix this move: previous iteration of the loop. Improve?
                    vec![
                        col(ColumnIdentifier::Id.as_ref()),
                        self.v_prog.clone().alias(self.vertex_column.as_ref()),
                    ],
                );
            // 3. Send messages to the neighboring nodes. Note that we have to materialize the
            // DataFrame so the stack is does not end up overflowed
            current_vertices = match self
                .graph
                .vertices
                .clone()
                .lazy()
                .inner_join(
                    vertex_columns,
                    col(ColumnIdentifier::Id.as_ref()),
                    col(ColumnIdentifier::Id.as_ref()),
                )
                .collect()
            {
                Ok(current_vertices) => current_vertices,
                Err(error) => return Err(error),
            };

            iteration += 1; // increment the counter so we now which iteration is being executed
        }

        Ok(current_vertices)
    }
}

#[cfg(test)]
mod tests {
    use crate::graph_frame::GraphFrame;
    use crate::pregel::ColumnIdentifier::{Custom, Dst, Id, Src};
    use crate::pregel::{MessageReceiver, Pregel};
    use polars::prelude::*;
    use std::error::Error;

    fn pagerank_builder(iterations: u8) -> Result<Pregel, Box<dyn Error>> {
        let edges = df![
            Src.as_ref() => [0, 0, 1, 2, 3, 4, 4, 4],
            Dst.as_ref() => [1, 2, 2, 3, 3, 1, 2, 3],
        ]?;

        let vertices = GraphFrame::from_edges(edges.clone())?.out_degrees()?;

        let damping_factor = 0.85;
        let num_vertices: f64 = vertices.column(Id.as_ref())?.len() as f64;

        Ok(Pregel {
            graph: GraphFrame::new(vertices, edges)?,
            max_iterations: iterations,
            vertex_column: Custom("rank".to_owned()),
            initial_message: lit(1.0 / num_vertices),
            send_messages: (
                Pregel::edge(MessageReceiver::into(MessageReceiver::Dst)),
                Pregel::src(Custom("rank".to_owned()))
                    / Pregel::src(Custom("out_degree".to_owned())),
            ),
            aggregate_messages: Pregel::msg(None).sum(),
            v_prog: Pregel::msg(None) * lit(damping_factor)
                + lit((1.0 - damping_factor) / num_vertices),
        })
    }

    fn agg_pagerank(pagerank: Pregel) -> Result<f64, String> {
        let result = match pagerank.run() {
            Ok(result) => result,
            Err(error) => {
                println!("{}", error);
                return Err(String::from("Error running the PageRank algorithm"));
            }
        };
        let rank = match result.column("rank") {
            Ok(rank) => rank,
            Err(_) => {
                return Err(String::from(
                    "Error retrieving the rank column from the DataFrame",
                ))
            }
        };
        let rank_f64 = match rank.f64() {
            Ok(rank_f64) => rank_f64,
            Err(_) => return Err(String::from("Error casting the rank column to f64")),
        };

        match rank_f64.sum() {
            Some(aggregated_rank) => Ok(aggregated_rank),
            None => Err(String::from(
                "Error computing the aggregation of PageRank values",
            )),
        }
    }

    fn pagerank_test_helper(iterations: u8) -> Result<(), String> {
        let pagerank = match pagerank_builder(iterations) {
            Ok(pagerank) => pagerank,
            Err(_) => return Err(String::from("Error building the Pregel algorithm :(")),
        };

        let agg_pagerank = match agg_pagerank(pagerank) {
            Ok(agg_pagerank) => agg_pagerank,
            Err(error) => return Err(error),
        };

        if (agg_pagerank - 1.0).abs() < 10e-3 {
            Ok(())
        } else {
            Err(String::from(
                "The sum of the aggregated PageRank values should be 1",
            ))
        }
    }

    #[test]
    fn pagerank_test() -> Result<(), String> {
        for i in 1..3 {
            pagerank_test_helper(i)?;
        }

        Ok(())
    }
}
