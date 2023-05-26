use crate::graph_frame::GraphFrame;
use polars::prelude::*;

type FnBox<'a> = Box<dyn FnMut() -> Expr + 'a>;

/// This defines an enumeration type `ColumnIdentifier` in Rust. It  has several
/// variants: `Id`, `Src`, `Dst`, `Edge`, `Msg`, `Pregel`, and `Custom` which
/// takes a `String` parameter. This enum can be used to represent different
/// types of columns in a data structure or database table for it to be used
/// in a Pregel program.
pub enum Column {
    /// The `Id` variant represents the column that contains the vertex IDs.
    Id,
    /// The `Src` variant represents the column that contains the source vertex IDs.
    Src,
    /// The `Dst` variant represents the column that contains the destination vertex IDs.
    Dst,
    /// The `Edge` variant represents the column that contains the edge IDs.
    Edge,
    /// The `Msg` variant represents the column that contains the messages sent to a vertex.
    Msg,
    /// The `Pregel` variant represents the column that contains the messages sent to a vertex.
    Pregel,
    /// The `Custom` variant represents a column that is not one of the predefined columns.
    Custom(&'static str),
}

/// This is the implementation of the `AsRef` trait for the `Column` enum. This
/// allows instances of the `Column` enum to be used as references to strings. The
/// `as_ref` method returns a reference to a string that corresponds to the variant
/// of the `Column` enum. If the variant is `Custom`, the string is the custom
/// identifier provided as an argument to the variant. This allows the `Column`
/// enum to be used as a reference to a string in a Pregel program.
///
/// # Examples
///
/// ```rust
/// use polars::prelude::*;
/// use pregel_rs::pregel::Column::{Custom, Id};
///
/// let vertices = df![
///     Id.as_ref() => [0, 1, 2, 3],
///     Custom("value").as_ref() => [3, 6, 2, 1],
/// ];
///
/// ```
impl AsRef<str> for Column {
    fn as_ref(&self) -> &str {
        match self {
            Column::Id => "id",
            Column::Src => "src",
            Column::Dst => "dst",
            Column::Edge => "edge",
            Column::Msg => "msg",
            Column::Pregel => "_pregel_msg_",
            Column::Custom(id) => id,
        }
    }
}

impl Column {
    fn alias(prefix: &Column, column_name: Column) -> String {
        format!("{}.{}", prefix.as_ref(), column_name.as_ref())
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
    pub fn src(column_name: Column) -> Expr {
        col(&Self::alias(&Column::Src, column_name))
    }

    /// This function returns an expression for a column identifier representing
    ///  the destination vertex in a Pregel graph.
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
    pub fn dst(column_name: Column) -> Expr {
        col(&Self::alias(&Column::Dst, column_name))
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
    /// and is used to construct the full column identifier using the `Column::alias`
    /// method.
    pub fn edge(column_name: Column) -> Expr {
        col(&Self::alias(&Column::Edge, column_name))
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
    /// `Column::alias` if `column_name` is `Some`.
    pub fn msg(column_name: Option<Column>) -> Expr {
        match column_name {
            None => col(Column::Pregel.as_ref()),
            Some(column_name) => col(&Self::alias(&Column::Msg, column_name)),
        }
    }
}

/// This defines a struct `SendMessage` in Rust. It has two properties:
/// `message_direction` and `send_message`. The `message_direction` property
/// is the identifier for the direction of the message. The `send_message`
/// property is the function that determines which messages to send from a
/// vertex to its neighbors.
pub struct SendMessage<'a> {
    /// `message_direction` is the identifier for the direction of the message.
    pub message_direction: Expr,
    /// `send_message` is the function that determines which messages to send from a
    /// vertex to its neighbors.
    pub send_message: FnBox<'a>,
}

impl<'a> SendMessage<'a> {
    /// The function creates a new instance of the `SendMessage` struct with the
    /// specified message direction and send message expression.
    ///
    /// Arguments:
    ///
    /// * `message_direction`: An enum that specifies whether the message should be sent
    /// to the source vertex or the destination vertex of an edge.
    /// * `send_message`: `send_message` is an expression that represents the message
    /// that will be sent from a vertex to its neighbors during the Pregel computation.
    /// It can be any valid Rust expression that evaluates to a DataFrame.
    ///
    /// Returns:
    ///
    /// A new instance of the `SendMessage` struct.
    pub fn new(message_direction: MessageReceiver, send_message: FnBox) -> SendMessage {
        // We make this in this manner because we want to use the `src.id` and `edge.dst` columns
        // in the send_messages function. This is because how polars works, when joining DataFrames,
        // it will keep only the left-hand side of the joins, thus, we need to use the `src.id` and
        // `edge.dst` columns to get the correct vertex IDs.
        let message_direction = match message_direction {
            MessageReceiver::Src => Column::src(Column::Id),
            MessageReceiver::Dst => Column::edge(Column::Dst),
        };
        let send_message = Box::new(send_message);
        // Now we create the `SendMessage` struct with everything set up.
        SendMessage {
            message_direction,
            send_message,
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
/// the message will go from Src to Dst or vice-versa. The second expression
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
///
/// * `replace_nulls`: `replace_nulls` is an expression that defines how null values
/// in the vertex DataFrame should be replaced. This is useful when the vertex
/// DataFrame contains null values that need to be replaced during the execution of
/// the Pregel algorithm. As an example, when not all vertices are connected to an
/// edge, the edge DataFrame will contain null values in the `dst` column. These
/// null values need to be replaced.
///
/// * `parquet_path` is a property of the `PregelBuilder` struct that represents
/// the path to the Parquet file where the results of the Pregel computation
/// will be stored.
pub struct Pregel<'a> {
    /// The `graph` property is a `GraphFrame` struct that represents the
    /// graph data structure used in the Pregel algorithm. It contains information about
    /// the vertices and edges of the graph.
    graph: GraphFrame,
    /// The maximum number of iterations that the Pregel algorithm.
    max_iterations: u8,
    /// `vertex_column` is a property of the `PregelBuilder` struct that  identifies
    /// and locates a column where we apply some of the provided operations during
    /// the Pregel computation.
    vertex_column: Column,
    /// `initial_message` is an expression that defines the initial message that
    /// each vertex in the graph will receive before the computation starts.
    initial_message: Expr,
    /// The `send_messages` property is a vector of `SendMessage` structs that represent
    /// the message sending functions. The `SendMessage` struct contains two expressions.
    /// The first expression represents the message sending function that determines whether
    /// the message will go from Src to Dst or vice-versa. The second expression represents
    /// the message sending function that determines which messages to send from a
    /// vertex to its neighbors.
    send_messages: Vec<SendMessage<'a>>,
    /// `aggregate_messages` is an expression that defines how messages sent to a
    /// vertex should be aggregated. In Pregel, messages are sent from one vertex
    /// to another and can be aggregated before being processed by the receiving
    /// vertex. The `aggregate_messages` expression specifies how these messages
    /// should be combined.
    aggregate_messages: FnBox<'a>,
    /// `v_prog` is an expression that defines the vertex program for the Pregel
    /// algorithm. It specifies the computation that each vertex performs during
    /// each iteration of the algorithm. The vertex program can take as input the
    /// current state of the vertex, the messages received from its neighbors or
    /// and any other relevant information.
    v_prog: FnBox<'a>,
    /// `replace_nulls` is an expression that defines how null values in the vertex
    /// DataFrame should be replaced. This is useful when the vertex DataFrame
    /// contains null values that need to be replaced during the execution of the
    /// Pregel algorithm. As an example, when not all vertices are connected to an
    /// edge, the edge DataFrame will contain null values in the `dst` column. These
    /// null values need to be replaced.
    replace_nulls: Expr,
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
/// the message will go from Src to Dst or vice-versa. The second expression
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
///
/// /// * `replace_nulls`: `replace_nulls` is an expression that defines how null values
/// in the vertex DataFrame should be replaced. This is useful when the vertex
/// DataFrame contains null values that need to be replaced during the execution of
/// the Pregel algorithm. As an example, when not all vertices are connected to an
/// edge, the edge DataFrame will contain null values in the `dst` column. These
/// null values need to be replaced.
pub struct PregelBuilder<'a> {
    /// The `graph` property is a `GraphFrame` struct that represents the
    /// graph data structure used in the Pregel algorithm. It contains information about
    /// the vertices and edges of the graph.
    graph: GraphFrame,
    /// The maximum number of iterations that the Pregel algorithm.
    max_iterations: u8,
    /// `vertex_column` is a property of the `PregelBuilder` struct that  identifies
    /// and locates a column where we apply some of the provided operations during
    /// the Pregel computation.
    vertex_column: Column,
    /// `initial_message` is an expression that defines the initial message that
    /// each vertex in the graph will receive before the computation starts.
    initial_message: Expr,
    /// The `send_messages` property is a vector of `SendMessage` structs that represent
    /// the message sending functions. The `SendMessage` struct contains two expressions.
    /// The first expression represents the message sending function that determines whether
    /// the message will go from Src to Dst or vice-versa. The second expression represents
    /// the message sending function that determines which messages to send from a
    /// vertex to its neighbors.
    send_messages: Vec<SendMessage<'a>>,
    /// `aggregate_messages` is an expression that defines how messages sent to a
    /// vertex should be aggregated. In Pregel, messages are sent from one vertex
    /// to another and can be aggregated before being processed by the receiving
    /// vertex. The `aggregate_messages` expression specifies how these messages
    /// should be combined.
    aggregate_messages: FnBox<'a>,
    /// `v_prog` is an expression that defines the vertex program for the Pregel
    /// algorithm. It specifies the computation that each vertex performs during
    /// each iteration of the algorithm. The vertex program can take as input the
    /// current state of the vertex, the messages received from its neighbors or
    /// and any other relevant information.
    v_prog: FnBox<'a>,
    /// `replace_nulls` is an expression that defines how null values in the vertex
    /// DataFrame should be replaced. This is useful when the vertex DataFrame
    /// contains null values that need to be replaced during the execution of the
    /// Pregel algorithm. As an example, when not all vertices are connected to an
    /// edge, the edge DataFrame will contain null values in the `dst` column. These
    /// null values need to be replaced.
    replace_nulls: Expr,
}

/// This code is defining an enumeration type `MessageReceiver` in Rust with
/// two variants: `Src` and `Dst`. This can be used to represent the source and
/// destination of a message in a Pregel program.
pub enum MessageReceiver {
    /// The `Src` variant indicates that a message should go to the source of
    /// an edge.
    Src,
    /// The `Src` variant indicates that a message should go to the destination
    /// of an edge.
    Dst,
}

/// The above code is implementing the `From` trait for the `Column` enum, which
/// allows creating a `Column` instance from a `MessageReceiver` instance. The
/// `match` statement maps the `MessageReceiver` variants to the corresponding
/// `Column` variants.
impl From<MessageReceiver> for Column {
    fn from(message_receiver: MessageReceiver) -> Column {
        match message_receiver {
            MessageReceiver::Src => Column::Src,
            MessageReceiver::Dst => Column::Dst,
        }
    }
}

impl<'a> PregelBuilder<'a> {
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
            vertex_column: Column::Custom("aux"),
            initial_message: Default::default(),
            send_messages: Default::default(),
            aggregate_messages: Box::new(Default::default),
            v_prog: Box::new(Default::default),
            replace_nulls: Default::default(),
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
    pub fn with_vertex_column(mut self, vertex_column: Column) -> Self {
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
    /// Rust. Chaining this method allows for multiple message sending behaviors to be
    /// specified for a single Pregel computation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use polars::prelude::*;
    /// use pregel_rs::graph_frame::GraphFrame;
    /// use pregel_rs::pregel::Column;
    /// use pregel_rs::pregel::Column::{Custom, Dst, Id, Src};
    /// use pregel_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};
    /// use std::error::Error;
    ///
    /// // Simple example of a Pregel algorithm where we chain several `send_messages` calls. In
    /// // this example, we send a message to the source of an edge and then to the destination of
    /// // the same edge. It has no real use case, but it demonstrates how to chain multiple calls.
    /// fn main() -> Result<(), Box<dyn Error>> {
    ///
    ///     let edges = df![
    ///         Src.as_ref() => [0, 1, 1, 2, 2, 3],
    ///         Dst.as_ref() => [1, 0, 3, 1, 3, 2],
    ///     ]?;
    ///
    ///     let vertices = df![
    ///         Id.as_ref() => [0, 1, 2, 3],
    ///         Custom("value").as_ref() => [3, 6, 2, 1],
    ///     ]?;
    ///
    ///     let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
    ///         .max_iterations(4)
    ///         .with_vertex_column(Custom("aux"))
    ///         .initial_message(lit(0))
    ///         .send_messages(MessageReceiver::Src, lit(1))
    ///         .send_messages(MessageReceiver::Dst, lit(-1))
    ///         .aggregate_messages(Column::msg(None).sum())
    ///         .v_prog(Column::msg(None) + lit(1))
    ///         .build();
    ///
    ///     Ok(println!("{:?}", pregel.run()))
    /// }
    /// ```
    ///
    /// Arguments:
    ///
    /// * `to`: `to` is a parameter of type `MessageReceiver`. It represents the
    /// destination vertex or vertices to which messages will be sent during the
    /// computation.
    /// * `send_messages`: `send_messages` is a parameter of type `Expr`. It is used to
    /// specify the function that will be applied to each vertex in the graph to send
    /// messages to its neighboring vertices.
    ///
    /// Returns:
    ///
    /// The `send_messages` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn send_messages(mut self, to: MessageReceiver, send_messages: Expr) -> Self {
        self.send_messages.push(SendMessage::new(
            to,
            Box::new(move || send_messages.clone()),
        ));
        self
    }

    /// This is a Rust function that adds a new message to be sent to a message receiver
    /// using a closure that returns an expression.
    ///
    /// Arguments:
    ///
    /// * `to`: `to` is a parameter of type `MessageReceiver` which represents the
    /// recipient of the message being sent. It could be an email address, phone number,
    /// or any other means of communication.
    ///
    /// * `send_messages`: `send_messages` is a closure that takes no arguments and
    /// returns an `Expr`. It is used to generate the messages that will be sent to the
    /// `to` message receiver. The closure is passed as an argument to the
    /// `send_messages_function` method and is stored in a `SendMessage` struct
    ///
    /// Returns:
    ///
    /// The `Self` object is being returned, which allows for method chaining.
    pub fn send_messages_function(
        mut self,
        to: MessageReceiver,
        send_messages: impl FnMut() -> Expr + 'a,
    ) -> Self {
        self.send_messages
            .push(SendMessage::new(to, Box::new(send_messages)));
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
        self.aggregate_messages = Box::new(move || aggregate_messages.clone());
        self
    }

    /// This function sets the aggregate_messages field of a struct to a closure that
    /// returns an Expr.
    ///
    /// Arguments:
    ///
    /// * `aggregate_messages`: `aggregate_messages` is a closure that takes no
    /// arguments and returns an `Expr` object. The closure is passed as an argument to
    /// the `aggregate_messages_function` method and is stored in the
    /// `self.aggregate_messages` field. The closure is expected to be implemented by
    /// the caller and will be used
    ///
    /// Returns:
    ///
    /// The `Self` object is being returned. This allows for method chaining, where
    /// multiple methods can be called on the same object in a single expression.
    pub fn aggregate_messages_function(
        mut self,
        aggregate_messages: impl FnMut() -> Expr + 'a,
    ) -> Self {
        self.aggregate_messages = Box::new(aggregate_messages);
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
        self.v_prog = Box::new(move || v_prog.clone());
        self
    }

    /// This is a Rust function that takes a closure that returns an expression and sets
    /// it as a field in a struct.
    ///
    /// Arguments:
    ///
    /// * `v_prog`: `v_prog` is a closure that takes no arguments and returns an `Expr`
    /// object. The closure is passed as an argument to the `v_prog_function` method.
    /// The `impl FnMut() -> Expr + 'a` syntax specifies that the closure must be
    /// mutable (`FnMut`) and that it must not capture any variables from the enclosing
    /// scope (`'a`). The closure is stored in the `self.v_prog` field of the struct.
    ///
    /// Returns:
    ///
    /// The `Self` object is being returned. This allows for method chaining, where
    /// multiple methods can be called on the same object in a single expression.
    pub fn v_prog_function(mut self, v_prog: impl FnMut() -> Expr + 'a) -> Self {
        self.v_prog = Box::new(v_prog);
        self
    }

    /// This function sets the value of a field called "replace_nulls" in a struct to a
    /// given expression and returns the modified struct.
    ///
    /// Arguments:
    ///
    /// * `replace_nulls`: `replace_nulls` is a parameter of type `Expr` that is used in
    /// a method of a struct. The method takes ownership of the struct (`self`) and the
    /// `replace_nulls` parameter, and sets the `replace_nulls` field of the struct to the
    /// value of the `replace_nulls` parameter.
    ///
    /// Returns:
    ///
    /// The `replace_nulls` method returns `Self`, which refers to the same struct
    /// instance that the method was called on. This allows for method chaining, where
    /// multiple methods can be called on the same struct instance in a single
    /// expression.
    pub fn replace_nulls(mut self, replace_nulls: Expr) -> Self {
        self.replace_nulls = replace_nulls;
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
    /// ```rust
    /// use polars::prelude::*;
    /// use pregel_rs::graph_frame::GraphFrame;
    /// use pregel_rs::pregel::Column;
    /// use pregel_rs::pregel::Column::{Custom, Dst, Id, Src};
    /// use pregel_rs::pregel::{MessageReceiver, Pregel, PregelBuilder};
    /// use std::error::Error;
    ///
    /// // Simple example of a Pregel algorithm that finds the maximum value in a graph.
    /// fn main() -> Result<(), Box<dyn Error>> {
    /// let edges = df![
    ///         Src.as_ref() => [0, 1, 1, 2, 2, 3],
    ///         Dst.as_ref() => [1, 0, 3, 1, 3, 2],
    ///     ]?;
    ///
    ///     let vertices = df![
    ///         Id.as_ref() => [0, 1, 2, 3],
    ///         Custom("value").as_ref() => [3, 6, 2, 1],
    ///     ]?;
    ///
    ///     let pregel = PregelBuilder::new(GraphFrame::new(vertices, edges)?)
    ///         .max_iterations(4)
    ///         .with_vertex_column(Custom("max_value"))
    ///         .initial_message(col(Custom("value").as_ref()))
    ///         .send_messages(MessageReceiver::Dst, Column::src(Custom("max_value")))
    ///         .aggregate_messages(Column::msg(None).max())
    ///         .v_prog(max_exprs([col(Custom("max_value").as_ref()), Column::msg(None)]))
    ///         .build();
    ///
    ///     Ok(println!("{}", pregel.run()?))
    /// }
    /// ```
    ///
    /// Returns:
    ///
    /// The `build` method is returning an instance of the `Pregel` struct with the
    /// values of the fields set to the values of the corresponding fields in the
    /// `Builder` struct.
    pub fn build(self) -> Pregel<'a> {
        Pregel {
            graph: self.graph,
            max_iterations: self.max_iterations,
            vertex_column: self.vertex_column,
            initial_message: self.initial_message,
            send_messages: self.send_messages,
            aggregate_messages: self.aggregate_messages,
            v_prog: self.v_prog,
            replace_nulls: self.replace_nulls,
        }
    }
}

impl<'a> Pregel<'a> {
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
    ///   super-step, based on its local state and the messages received from its
    ///   neighboring vertices.
    ///
    /// - Messages: Vertices can send messages to their neighboring vertices during
    ///   each super-step, which are then delivered in the next super-step. Messages
    ///   are used for communication and coordination between vertices.
    ///
    /// - Aggregators: functions that can be used to collect and aggregate
    ///   information from vertices during computation. Aggregators allow for global
    ///   coordination and information gathering across the entire graph.
    ///
    /// # Usage
    ///
    /// The Pregel model follows a sequence of super-step, where each super-step consists
    /// of computation, message exchange, and aggregation. Vertices perform their computations
    /// in parallel, and messages are exchanged between vertices to coordinate their activities.
    /// The computation continues in a series of super-steps until a termination condition is met.
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
    pub fn run(mut self) -> PolarsResult<DataFrame> {
        // We create a DataFrame that contains the edges of the graph. This DataFrame is used to
        // compute the triplets of the graph, which are used to send messages to the neighboring
        // vertices of each vertex in the graph. For us to do so, we select all the columns of the
        // graph edges and we prefix them with the `Edge` column name.
        let edges = &self
            .graph
            .edges
            .lazy()
            .select([all().prefix(&format!("{}.", Column::Edge.as_ref()))]);
        // We create a DataFrame that contains the vertices of the graph
        let vertices = &self.graph.vertices.lazy();
        // We start the execution of the algorithm from the super-step 0; that is, all the nodes
        // are set to active, and the initial messages are sent to each vertex in the graph. What's more,
        // The initial messages are stored in the `initial_message` column of the `current_vertices` DataFrame.
        // We use the `lazy` method to create a lazy DataFrame. This is done to avoid the execution of
        // the DataFrame until the end of the algorithm.
        let initial_message = &self.initial_message;
        let mut current_vertices = vertices
            .to_owned()
            .select([
                all(), // we select all the columns of the graph vertices
                initial_message
                    .to_owned()
                    .alias(self.vertex_column.as_ref()), // initial message column name is set by the user
            ])
            .with_common_subplan_elimination(false)
            .with_streaming(true)
            .collect()?;
        // After computing the super-step 0, we start the execution of the Pregel algorithm. This
        // execution is performed until all the nodes vote to halt, or the number of iterations is
        // greater than the maximum number of iterations set by the user at the initialization of
        // the model (see the `Pregel::new` method). We start by setting the number of iterations to 1.
        let mut iteration = 1;
        while iteration <= self.max_iterations {
            // TODO: check that nodes are not halted.
            // We create a DataFrame that contains the triplets of the graph. Those triplets are
            // computed by joining the `current_vertices` DataFrame with the `edges` DataFrame
            // first, and with the `current_vertices` second. The first join is performed on the `src`
            // column of the `edges` DataFrame and the `id` column of the `current_vertices` DataFrame.
            // The second join is performed on the `dst` column of the resulting DataFrame from the previous
            // join and the `id` column of the `current_vertices` DataFrame.
            let current_vertices_df = &current_vertices.lazy();
            let triplets_df = current_vertices_df
                .to_owned()
                .select([all().prefix(&format!("{}.", Column::Src.as_ref()))])
                .inner_join(
                    edges.to_owned().select([all()]),
                    Column::src(Column::Id), // src column of the current_vertices DataFrame
                    Column::edge(Column::Src), // src column of the edges DataFrame
                )
                .inner_join(
                    current_vertices_df
                        .to_owned()
                        .select([all().prefix(&format!("{}.", Column::Dst.as_ref()))]),
                    Column::edge(Column::Dst), // dst column of the resulting DataFrame
                    Column::dst(Column::Id),   // id column of the current_vertices DataFrame
                );
            // We create a DataFrame that contains the messages sent by the vertices. The messages
            // are computed by performing an aggregation on the `triplets_df` DataFrame. The aggregation
            // is performed on the `msg` column of the `triplets_df` DataFrame, and the aggregation
            // function is the one set by the user at the initialization of the model.
            // We create a tuple where we store the column names of the `send_messages` DataFrame. We use
            // the `alias` method to ensure that the column names are properly qualified. We also
            // do the same for the `aggregate_messages` Expr. And the same with the `v_prog` Expr.
            let (mut send_messages_ids, mut send_messages_msg): (Vec<Expr>, Vec<Expr>) = self
                .send_messages
                .iter_mut()
                .map(|send_message| {
                    let message_direction = &send_message.message_direction;
                    let send_message_expr = &mut send_message.send_message;
                    (
                        message_direction
                            .to_owned()
                            .alias(&Column::alias(&Column::Msg, Column::Id)),
                        send_message_expr().alias(Column::Pregel.as_ref()),
                    )
                })
                .unzip();
            let send_messages = &mut send_messages_ids; // we create a mutable reference to the `send_messages_ids` Vector
            let send_messages_msg_df = &mut send_messages_msg; // we create a mutable reference to the `send_messages_msg` Vector
            send_messages.append(send_messages_msg_df); // we append the `send_messages_msg` Vector to the `send_messages` Vector
            let aggregate_messages = &mut self.aggregate_messages;
            let message_df = triplets_df
                .select(send_messages)
                .groupby([Column::msg(Some(Column::Id))])
                .agg([aggregate_messages().alias(Column::Pregel.as_ref())]);
            // We Compute the new values for the vertices. Note that we have to check for possibly
            // null values after performing the outer join. This is, columns where the join key does
            // not exist in the source DataFrame. In case we find any; for example, given a certain
            // node having no incoming edges, we have to replace the null value by 0 for the aggregation
            // to work properly.
            let v_prog = &mut self.v_prog;
            let vertex_columns = current_vertices_df
                .to_owned()
                .outer_join(
                    message_df,
                    col(Column::Id.as_ref()), // id column of the current_vertices DataFrame
                    Column::msg(Some(Column::Id)), // msg.id column of the message_df DataFrame
                )
                .with_column(Column::msg(None).fill_null(self.replace_nulls.to_owned()))
                .select(&[
                    col(Column::Id.as_ref()),
                    v_prog().alias(self.vertex_column.as_ref()),
                ]);
            // We update the `current_vertices` DataFrame with the new values for the vertices. We
            // do so by performing an inner join between the `current_vertices` DataFrame and the
            // `vertex_columns` DataFrame. The join is performed on the `id` column of the
            // `current_vertices` DataFrame and the `id` column of the `vertex_columns` DataFrame.
            current_vertices = vertices
                .to_owned()
                .inner_join(
                    vertex_columns,
                    col(Column::Id.as_ref()),
                    col(Column::Id.as_ref()),
                )
                .with_common_subplan_elimination(false)
                .with_streaming(true)
                .collect()?;

            iteration += 1; // increment the counter so we now which iteration is being executed
        }

        Ok(current_vertices)
    }
}

#[cfg(test)]
mod tests {
    use crate::graph_frame::GraphFrame;
    use crate::pregel::{Column, MessageReceiver, Pregel, PregelBuilder, SendMessage};
    use polars::prelude::*;
    use std::error::Error;

    fn pagerank_graph() -> Result<GraphFrame, String> {
        let edges = match df![
            Column::Src.as_ref() => [0, 0, 1, 2, 3, 4, 4, 4],
            Column::Dst.as_ref() => [1, 2, 2, 3, 3, 1, 2, 3],
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let graph = match GraphFrame::from_edges(edges.clone()) {
            Ok(graph) => graph,
            Err(_) => return Err(String::from("Error creating the vertices DataFrame")),
        };

        let vertices = match graph.out_degrees() {
            Ok(vertices) => vertices,
            Err(_) => {
                return Err(String::from(
                    "Error creating the vertices out degree DataFrame",
                ))
            }
        };

        match GraphFrame::new(vertices, edges) {
            Ok(graph) => Ok(graph),
            Err(_) => Err(String::from("Error creating the graph")),
        }
    }

    fn pagerank_builder<'a>(iterations: u8) -> Result<Pregel<'a>, Box<dyn Error>> {
        let graph = pagerank_graph()?;
        let damping_factor = 0.85;
        let num_vertices: f64 = graph.vertices.column(Column::Id.as_ref())?.len() as f64;

        Ok(PregelBuilder::new(graph)
            .max_iterations(iterations)
            .with_vertex_column(Column::Custom("rank"))
            .initial_message(lit(1.0 / num_vertices))
            .replace_nulls(lit(0.0))
            .send_messages(
                MessageReceiver::Dst,
                Column::src(Column::Custom("rank")) / Column::src(Column::Custom("out_degree")),
            )
            .aggregate_messages(Column::msg(None).sum())
            .v_prog(
                Column::msg(None) * lit(damping_factor)
                    + lit((1.0 - damping_factor) / num_vertices),
            )
            .build())
    }

    fn agg_pagerank(pagerank: Pregel) -> Result<f64, String> {
        let result = match pagerank.run() {
            Ok(result) => result,
            Err(_) => return Err(String::from("Error running the PageRank algorithm")),
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

    fn pagerank_helper(iterations: u8) -> Result<(), String> {
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
    fn test_pagerank() -> Result<(), String> {
        for i in 1..3 {
            pagerank_helper(i)?;
        }

        Ok(())
    }

    fn max_value_graph() -> Result<GraphFrame, String> {
        let edges = match df![
            Column::Src.as_ref() => [0, 1, 1, 2, 2, 3],
            Column::Dst.as_ref() => [1, 0, 3, 1, 3, 2],
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let vertices = match df![
            Column::Id.as_ref() => [0, 1, 2, 3],
            Column::Custom("value").as_ref() => [3, 6, 2, 1],
        ] {
            Ok(vertices) => vertices,
            Err(_) => return Err(String::from("Error creating the vertices DataFrame")),
        };

        match GraphFrame::new(vertices, edges) {
            Ok(graph) => Ok(graph),
            Err(_) => Err(String::from("Error creating the graph")),
        }
    }

    fn max_value_builder<'a>(iterations: u8) -> Result<Pregel<'a>, String> {
        Ok(Pregel {
            graph: max_value_graph()?,
            max_iterations: iterations,
            vertex_column: Column::Custom("max_value"),
            initial_message: col(Column::Custom("value").as_ref()),
            send_messages: vec![SendMessage::new(
                MessageReceiver::Dst,
                Box::new(|| Column::src(Column::Custom("value"))),
            )],
            aggregate_messages: Box::new(|| Column::msg(None).max()),
            v_prog: Box::new(|| {
                max_exprs([col(Column::Custom("max_value").as_ref()), Column::msg(None)])
            }),
            replace_nulls: lit(0),
        })
    }

    fn max_value_helper(iterations: u8) -> Result<(), String> {
        let max_value = match max_value_builder(iterations) {
            Ok(max_value) => max_value,
            Err(_) => return Err(String::from("Error building the Pregel algorithm :(")),
        };

        let result = match max_value.run() {
            Ok(result) => result,
            Err(_) => return Err(String::from("Error running the Max algorithm")),
        };

        let max = match result.column("max_value") {
            Ok(max) => max,
            Err(_) => {
                return Err(String::from(
                    "Error retrieving the max column from the DataFrame",
                ))
            }
        };

        let max_i32 = match max.i32() {
            Ok(max_i32) => max_i32,
            Err(_) => return Err(String::from("Error casting the max column to i32")),
        };

        match max_i32.max() {
            Some(max) => {
                // In case the maximum value is computed
                if max == 6 {
                    // In case MAX equals to 6, that means that the algorithm has converged
                    Ok(())
                } else {
                    // In any other case, the algorithm has not converged: ERROR
                    Err(String::from("The maximum value should be 4"))
                }
            }
            None => Err(String::from("Error computing the maximum value")),
        }
    }

    #[test]
    fn test_max_value() -> Result<(), String> {
        for i in 1..3 {
            max_value_helper(i)?;
        }

        Ok(())
    }

    #[test]
    fn test_literals() -> Result<(), String> {
        // We create a graph using the exact same vertices and edges as the one used in the
        // MaxValue algorithm. The graph itself is not important, we just need to test the
        // Pregel model.
        let graph = max_value_graph()?;

        // We create a Pregel algorithm that computes nothing, just sends literals to all the vertices
        // and then returns the same literal. Note that the algorithm computes nothing, but it is
        // useful to test the Pregel model.
        match PregelBuilder::new(graph)
            .max_iterations(4)
            .with_vertex_column(Column::Custom("aux"))
            .initial_message(lit(0)) // we pass the Undefined state to all vertices
            .send_messages(MessageReceiver::Src, lit(0))
            .aggregate_messages(lit(0))
            .v_prog(lit(0))
            .build()
            .run()
        {
            Ok(_) => Ok(()),
            Err(_) => Err(String::from("Error running the algorithm")),
        }
    }

    #[test]
    fn test_send_messages_src_dst() -> Result<(), String> {
        let graph = pagerank_graph()?;

        let pregel = match PregelBuilder::new(graph)
            .max_iterations(4)
            .with_vertex_column(Column::Custom("aux"))
            .initial_message(lit(0))
            .send_messages(MessageReceiver::Src, lit(1))
            .send_messages(MessageReceiver::Dst, lit(-1))
            .aggregate_messages(Column::msg(None).sum())
            .v_prog(Column::msg(None) + lit(1))
            .build()
            .run()
        {
            Ok(pregel) => pregel,
            Err(_) => return Err(String::from("Error running pregel")),
        };

        let sorted_pregel = match pregel.sort(&["id"], false) {
            Ok(sorted_pregel) => sorted_pregel,
            Err(_) => return Err(String::from("Error sorting the DataFrame")),
        };

        let ans = match sorted_pregel.column("aux") {
            Ok(ans) => ans,
            Err(_) => return Err(String::from("Error retrieving the column")),
        };

        let expected = Series::new("aux", [3, 2, 2, 2, 4]);

        if ans.eq(&expected) {
            Ok(())
        } else {
            Err(String::from("The resulting DataFrame is not correct"))
        }
    }
}
