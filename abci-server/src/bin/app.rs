use abci_server::kvstore::*;

use structopt::StructOpt;
use tower::ServiceBuilder;
use tower_abci::{split, Server};

#[derive(Debug, StructOpt)]
struct Opt {
    /// Bind the TCP server to this host.
    #[structopt(short, long, default_value = "127.0.0.1")]
    host: String,

    /// Bind the TCP server to this port.
    #[structopt(short, long, default_value = "26658")]
    port: u16,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    // Construct our ABCI application.
    let service = KVStore::default();

    // Split it into components.
    let (consensus, mempool, snapshot, info) = split::service(service, 1);

    // Hand those components to the ABCI server, but customize request behavior
    // for each category -- for instance, apply load-shedding only to mempool
    // and info requests, but not to consensus requests.
    let abci_server = tokio::task::spawn(
        Server::builder()
            .consensus(consensus)
            .snapshot(snapshot)
            .mempool(
                ServiceBuilder::new()
                    .load_shed()
                    .buffer(10)
                    .service(mempool),
            )
            .info(
                ServiceBuilder::new()
                    .load_shed()
                    .buffer(100)
                    .rate_limit(50, std::time::Duration::from_secs(1))
                    .service(info),
            )
            .finish()
            .unwrap()
            .listen(format!("{}:{}", opt.host, opt.port)),
    );

    // Run the ABCI server.
    tokio::select! {
        x = abci_server => x.unwrap().map_err(|e| anyhow::anyhow!(e)).unwrap(),
        // handle other tasks that might be spawned as part of your application
    };

    tracing::info!("ABCI server listening on {}::{}", opt.host, opt.port);
}
