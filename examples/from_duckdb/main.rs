use pregel_rs::graph_frame::GraphFrame;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let dataframe = GraphFrame::from_duckdb("./examples/from_duckdb/example.duckdb")?;
    Ok(println!("{}", dataframe))
}
