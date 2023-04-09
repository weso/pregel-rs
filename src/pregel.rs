use polars::prelude::*;
use crate::graph_frame::{DST, EDGE, GraphFrame, ID, MSG, SRC};

const PREGEL_MESSAGE_COL_NAME: &str = "_pregel_msg_";

pub struct Pregel {
    graph: GraphFrame,
    max_iterations: u8,
    vertex_column:  &'static str,
    initial_message: Expr,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
    v_prog: Expr,
}

pub struct PregelBuilder {
    graph: GraphFrame,
    max_iterations: u8,
    vertex_column:  &'static str,
    initial_message: Expr,
    send_messages: (Expr, Expr),
    aggregate_messages: Expr,
    v_prog: Expr,
}

pub enum MessageReceiver {
    Src,
    Dst
}

impl From<MessageReceiver> for &str {

    fn from(message_receiver: MessageReceiver) -> &'static str {
        match message_receiver {
            MessageReceiver::Src => SRC,
            MessageReceiver::Dst => DST,
        }
    }

}

impl PregelBuilder {

    pub fn new(graph: GraphFrame) -> Self {
        PregelBuilder {
            graph,
            max_iterations: 10,
            vertex_column: "vertex_column",
            initial_message: Default::default(),
            send_messages: (Default::default(), Default::default()),
            aggregate_messages: Default::default(),
            v_prog: Default::default(),
        }
    }

    pub fn max_iterations(mut self, max_iterations: u8) -> Self {
        self.max_iterations = max_iterations;
        self
    }

    pub fn with_vertex_column(mut self, vertex_column:  &'static str) -> Self {
        self.vertex_column = vertex_column;
        self
    }

    pub fn initial_message(mut self, initial_message: Expr) -> Self {
        self.initial_message = initial_message;
        self
    }

    pub fn send_messages(mut self, to: MessageReceiver, send_messages: Expr) -> Self {
        self.send_messages = (Pregel::edge(MessageReceiver::into(to)), send_messages); // TODO: is edge fine?
        self
    }

    pub fn aggregate_messages(mut self, aggregate_messages: Expr) -> Self {
        self.aggregate_messages = aggregate_messages;
        self
    }

    pub fn v_prog(mut self, v_prog: Expr) -> Self {
        self.v_prog = v_prog;
        self
    }

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

    fn alias(df: &str, column_name: &str) -> String {
        format!("{}.{}", df, column_name)
    }

    fn prefix_columns(expr: Expr, prefix: &'static str) -> Expr {
        expr.map_alias(|column_name| Ok(Self::alias(prefix, column_name)))
    }

    pub fn src(column_name: &str) -> Expr {
        col(&Pregel::alias(SRC, column_name))
    }

    pub fn dst(column_name: &str) -> Expr {
        col(&Pregel::alias(DST, column_name))
    }

    pub fn edge(column_name: &str) -> Expr {
        col(&Pregel::alias(EDGE, column_name))
    }

    pub fn msg(column_name: Option<&str>) -> Expr {
        match column_name {
            None => col(PREGEL_MESSAGE_COL_NAME),
            Some(column_name) => col(&Pregel::alias(MSG, column_name)),
        }
    }

    pub fn run(self) -> PolarsResult<DataFrame> {
        let (send_messages_ids, send_messages_msg) = self.send_messages;
        let (send_messages_ids, send_messages_msg) = (
            send_messages_ids.alias(&Self::alias(MSG, ID)),
            send_messages_msg.alias(PREGEL_MESSAGE_COL_NAME)
        );
        // We start the execution of the algorithm from the super-step 0; that is, all the nodes
        // are set to active, and the initial messages are sent to each vertex in the graph
        let mut current_vertices: DataFrame = match self
            .graph
            .vertices
            .clone()
            .lazy()
            .select(vec![all(), self.initial_message.alias(self.vertex_column)])
            .collect()
        {
            Ok(current_vertices) => current_vertices,
            Err(error) => return Err(error),
        };
        let edges = self
            .graph
            .edges
            .lazy()
            .select([Self::prefix_columns(all(), EDGE)]);
        // After computing the super-step 0, we start the execution of the Pregel algorithm. This
        // execution is performed until all the nodes vote to halt, or the number of iterations is
        // greater than the maximum number of iterations set by the user at the initialization of
        // the model
        let mut iteration = 1;
        while iteration <= self.max_iterations { // TODO: check that nodes are not halted :D
            // TODO: checkpointing?
            // 0. We create the triplets DataFrame
            let triplets_df = current_vertices
                .clone()
                .lazy()
                .select([Self::prefix_columns(all(), SRC)])
                .inner_join(
                    edges.clone(),
                    Self::src(ID),
                    Self::edge(SRC)
                ).inner_join(
                    current_vertices.clone().lazy().select([Self::prefix_columns(all(), DST)]),
                    Self::edge(DST),
                    Self::dst(ID)
                );
            // 1. Generate the messages for the current iteration
            let message_df = triplets_df
                .select(vec![send_messages_ids.clone(), send_messages_msg.clone()])
                .groupby([Self::msg(Some(ID))])
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
                .outer_join(message_df, col(ID), Self::msg(Some(ID)))
                .with_column(
                    when(Self::msg(None).is_null())
                        .then(0)
                        .otherwise(Self::msg(None))
                        .alias(PREGEL_MESSAGE_COL_NAME)
                ).select(vec![col(ID), self.v_prog.clone().alias(self.vertex_column)]); // TODO: fix this move: previous iteration of the loop. Improve?
            // 3. Send messages to the neighboring nodes. Note that we have to materialize the
            // DataFrame so the stack is does not end up overflowed
            current_vertices = match self
                .graph
                .vertices
                .clone()
                .lazy()
                .inner_join(vertex_columns, col(ID), col(ID))
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
    use std::error::Error;
    use polars::prelude::*;
    use crate::graph_frame::{GraphFrame, ID};
    use crate::pregel::{MessageReceiver, Pregel};

    fn pagerank_builder(iterations: u8) -> Result<Pregel, Box<dyn Error>> {
        let edges = df![
            "src" => [0, 0, 1, 2, 3, 4, 4, 4],
            "dst" => [1, 2, 2, 3, 3, 1, 2, 3],
        ]?;

        let vertices = GraphFrame::from_edges(edges.clone())?.out_degrees()?;

        let damping_factor = 0.85;
        let num_vertices: f64 = vertices.column(ID)?.len() as f64;

        Ok(
            Pregel {
                graph: GraphFrame::new(vertices, edges)?,
                max_iterations: iterations,
                vertex_column: "rank",
                initial_message: lit(1.0 / num_vertices),
                send_messages: (
                    Pregel::edge(MessageReceiver::into(MessageReceiver::Dst)),
                    Pregel::src("rank") / Pregel::src("out_degree")
                ),
                aggregate_messages: Pregel::msg(None).sum(),
                v_prog: Pregel::msg(None) * lit(damping_factor) + lit((1.0 - damping_factor) / num_vertices),
            }
        )
    }

    fn agg_pagerank(pagerank: Pregel) -> Result<f64, String> {
        let result = match pagerank.run() {
            Ok(result) => result,
            Err(_) => return Err(String::from("Error running the PageRank algorithm"))
        };
        let rank = match result.column("rank") {
            Ok(rank) => rank,
            Err(_) => return Err(String::from("Error retrieving the rank column from the DataFrame"))
        };
        let rank_f64 = match rank.f64() {
            Ok(rank_f64) => rank_f64,
            Err(_) => return Err(String::from("Error casting the rank column to f64"))
        };

        match rank_f64.sum() {
            Some(aggregated_rank) => Ok(aggregated_rank),
            None => Err(String::from("Error computing the aggregation of PageRank values")),
        }
    }

    fn pagerank_test_helper(iterations: u8) -> Result<(), String> {
        let pagerank = match pagerank_builder(iterations) {
            Ok(pagerank) => pagerank,
            Err(_) => return Err(String::from("Error building the Pregel algorithm :("))
        };

        let agg_pagerank = match agg_pagerank(pagerank) {
            Ok(agg_pagerank) => agg_pagerank,
            Err(error) => return Err(error),
        };

        if (agg_pagerank - 1.0).abs() < 10e-3 {
            Ok(())
        } else {
            Err(String::from("The sum of the aggregated PageRank values should be 1"))
        }
    }

    #[test]
    fn pagerank_test() -> Result<(), String> {
        pagerank_test_helper(1)?;
        pagerank_test_helper(2)?;
        pagerank_test_helper(3)?;

        Ok(())
    }

}