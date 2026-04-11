mod routes;

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

#[derive(Clone)]
pub struct AppState {
    pub pijul: Arc<fx_pijul::PijulStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info,pijul_knot=debug").init();
    let args = Args::parse();

    let pijul = Arc::new(fx_pijul::PijulStore::new(&args.data_dir));
    let state = AppState { pijul };

    let app = routes::router(state).layer(CorsLayer::permissive());

    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("pijul-knot listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
