use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tinkiva_database::{
    api::{AppState, router},
    config::{Cli, Settings},
    db::DatabaseManager,
};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let settings = Settings::load(cli.config.as_deref())?;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("tinkiva_database=info,tower_http=info")),
        )
        .json()
        .init();

    let manager = Arc::new(DatabaseManager::new(settings.database.clone()).await?);
    let app = router(AppState::new(Arc::new(settings.clone()), manager.clone()));
    let cleanup_manager = manager.clone();
    let cleanup_interval = settings.database.cleanup_interval();
    let cleanup_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(cleanup_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = cleanup_manager.cleanup_idle().await {
                warn!(%error, "idle database cleanup failed");
            }
        }
    });

    let listener = TcpListener::bind(&settings.server.bind)
        .await
        .with_context(|| format!("failed to bind {}", settings.server.bind))?;
    info!(address = %settings.server.bind, data_directory = %settings.database.directory.display(), "TinkivaDatabase started");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("HTTP server failed")?;
    cleanup_task.abort();
    manager.close_all().await;
    info!("TinkivaDatabase stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { () = ctrl_c => {}, () = terminate => {} }
}
