use std::sync::Arc;
use clap::Parser;
use tower_http::cors::CorsLayer;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "KNOT_PORT", default_value = "3901")]
    port: u16,
    #[arg(long, env = "KNOT_DATA_DIR", default_value = "./data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info,pijul_knot=debug").init();
    let args = Args::parse();

    let state = pijul_knot::KnotState {
        pijul: Arc::new(pijul_knot::PijulStore::new(&args.data_dir)),
    };

    let app = pijul_knot::router(state).layer(CorsLayer::permissive());

    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("pijul-knot listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
