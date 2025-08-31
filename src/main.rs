mod config;
pub use config::Config;

#[tokio::main]
async fn main() {
    println!("heyo");
    let c = Config::from_yaml_str("jobs:");
    println!("{:?}", c);
}
