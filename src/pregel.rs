use std::ops::Add;
use polars::prelude::*;
use crate::graph_frame::{DST, EDGE, GraphFrame, ID, MSG, SRC};

const PREGEL_MESSAGE_COL_NAME: &str = "_pregel_msg_";

pub struct Pregel<L: Literal> {
    graph: GraphFrame,
    max_iterations: u8,
    initial_message: L,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
}

pub struct PregelBuilder<L: Literal> {
    graph: GraphFrame,
    max_iterations: u8,
    initial_message: L,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
}

impl<L: Literal + Default> PregelBuilder<L> {

    pub fn new(graph: GraphFrame) -> Self {
        PregelBuilder {
            graph,
            max_iterations: 10,
            initial_message: Default::default(),
            send_messages: (Default::default(), Default::default()),
            aggregate_messages: Default::default(),
        }
    }

    pub fn max_iterations(mut self, max_iterations: u8) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    pub fn initial_message(mut self, initial_message: L) -> Self {
        self.initial_message = initial_message;
        self
    }

    pub fn send_messages(mut self, send_messages: Expr) -> Self {
        self.send_messages = (Pregel::<L>::src(ID), send_messages);
        self
    }

    pub fn aggregate_messages(mut self, aggregate_messages: Expr) -> Self {
        self.aggregate_messages = aggregate_messages;
        self
    }

    pub fn build(self) -> Pregel<L> {
        Pregel {
            graph: self.graph,
            max_iterations: self.max_iterations,
            initial_message: self.initial_message,
            send_messages: self.send_messages,
            aggregate_messages: self.aggregate_messages,
        }
    }

}

impl<L: Literal> Pregel<L> {

    pub fn alias(df: &str, column_name: &str) -> String {
        format!("{}.{}", df, column_name)
    }

    pub fn src(column_name: &str) -> Expr {
        col(&*Pregel::<L>::alias(SRC, column_name))
    }

    pub fn dst(column_name: &str) -> Expr {
        col(&*Pregel::<L>::alias(DST, column_name))
    }

    pub fn edge(column_name: &str) -> Expr {
        col(&*Pregel::<L>::alias(EDGE, column_name))
    }

    pub fn msg(column_name: &str) -> Expr {
        col(&*Pregel::<L>::alias(MSG, column_name))
    }

    pub fn run(self) -> DataFrame {
        let (send_messages_ids, send_messages_msg) = match self.send_messages  {
            (send_messages_ids, send_messages_msg) =>
                (
                    send_messages_ids.alias(&*Self::alias(MSG, ID)),
                    send_messages_msg.alias(&*Self::alias(MSG, MSG))
                )
        };
        // We start the execution of the algorithm from the super-step 0; that is, all the nodes
        // are set to active, and the initial messages are sent to each vertex in the graph
        let mut current_vertices = self
            .graph
            .vertices
            .clone()
            .lazy()
            .with_column(lit( self.initial_message).alias("aux"))
            .collect()
            .unwrap(); // TODO: remove this?
        // After computing the super-step 0, we start the execution of the Pregel algorithm. This
        // execution is performed until all the nodes vote to halt, or the number of iterations is
        // greater than the maximum number of iterations set by the user at the initialization of
        // the model
        let mut iteration = 1;
        while iteration <= self.max_iterations {
            // 0. We create the triplets DataFrame
            let edges = self
                .graph
                .edges
                .clone()
                .lazy()
                .select([all().map_alias(|column_name| Ok(Self::alias(EDGE, column_name)))]);
            let dst = current_vertices
                .clone()
                .lazy()
                .select([all().map_alias(|column_name| Ok(Self::alias(DST, column_name)))]);
            let triplets_df = current_vertices
                .clone()
                .lazy()
                .select([all().map_alias(|column_name| Ok(Self::alias(SRC, column_name)))])
                .join(edges, [Self::src(ID)], [Self::edge(SRC)], JoinType::Left)
                .join(dst, [Self::edge(DST)], [Self::dst(ID)], JoinType::Left);
            // 1. Generate the messages for the current iteration
            let message_df = triplets_df
                .with_columns(vec![send_messages_ids.clone(), send_messages_msg.clone()])
                .filter(Pregel::<L>::msg(ID).is_not_null())
                .groupby([col(&*Self::alias(MSG, ID))])
                .agg([self.aggregate_messages.clone().alias(PREGEL_MESSAGE_COL_NAME)]);
            // 2. Compute the new values for the vertices
            let vertices_with_messages = current_vertices
                .clone()
                .lazy()
                .join(message_df, [col(ID)], [Self::msg(ID)], JoinType::Left)
                .select([all()]);
            // 3. Send messages to the neighboring nodes
            current_vertices = self
                .graph
                .vertices
                .clone()
                .lazy()
                .join(vertices_with_messages, [col(ID)], [col(ID)], JoinType::Left)
                .collect()
                .unwrap(); // TODO: remove this?

            println!("{}", current_vertices);

            iteration += 1; // increment the counter so we now which iteration is being executed
        }

        current_vertices
    }

}