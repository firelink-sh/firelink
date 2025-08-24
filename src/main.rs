mod config;
use config::Config;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::try_from_yaml_file("res/example-config.yaml")?;
    println!("Loaded {} jobs", cfg.jobs.len());

    let order = cfg.execution_order()?;

    for (idx, job) in order.iter().enumerate() {
        println!(" - {}: {}", idx, job.name);
    }

    Ok(())
}
