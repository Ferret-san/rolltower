//! ABCI Application Definition
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::future::FutureExt;
use tendermint::abci::{response, Event, EventAttributeIndexExt, Request, Response};
use tower::Service;
use tower_abci::BoxError;

/// In-memory, hashmap-backed key-value store ABCI application.
#[derive(Clone, Debug, Default)]
pub struct KVStore {
    store: HashMap<String, String>,
    height: u32,
    app_hash: [u8; 8],
}

impl KVStore {
    fn info(&self) -> response::Info {
        response::Info {
            data: "tower-abci-kvstore-example".to_string(),
            version: "0.1.0".to_string(),
            app_version: 1,
            last_block_height: self.height.into(),
            last_block_app_hash: self.app_hash.to_vec().try_into().unwrap(),
        }
    }

    fn query(&self, query: Bytes) -> response::Query {
        let key = String::from_utf8(query.to_vec()).unwrap();
        let (value, log) = match self.store.get(&key) {
            Some(value) => (value.clone(), "exists".to_string()),
            None => ("".to_string(), "does not exist".to_string()),
        };

        response::Query {
            log,
            key: key.into_bytes().into(),
            value: value.into_bytes().into(),
            ..Default::default()
        }
    }

    fn deliver_tx(&mut self, tx: Bytes) -> response::DeliverTx {
        let tx = String::from_utf8(tx.to_vec()).unwrap();
        let tx_parts = tx.split('=').collect::<Vec<_>>();
        let (key, value) = match (tx_parts.first(), tx_parts.get(1)) {
            (Some(key), Some(value)) => (*key, *value),
            _ => (tx.as_ref(), tx.as_ref()),
        };
        self.store.insert(key.to_string(), value.to_string());

        response::DeliverTx {
            events: vec![Event::new(
                "app",
                vec![
                    ("key", key).index(),
                    ("index_key", "index is working").index(),
                    ("noindex_key", "index is working").no_index(),
                ],
            )],
            ..Default::default()
        }
    }

    fn commit(&mut self) -> response::Commit {
        let retain_height = self.height.into();
        // As in the other kvstore examples, just use store.len() as the "hash"
        self.app_hash = (self.store.len() as u64).to_be_bytes();
        self.height += 1;

        response::Commit {
            data: self.app_hash.to_vec().into(),
            retain_height,
        }
    }
}

impl Service<Request> for KVStore {
    type Response = Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Response, BoxError>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        tracing::info!(?req);

        let rsp = match req {
            // handled messages
            Request::Info(_) => Response::Info(self.info()),
            Request::Query(query) => Response::Query(self.query(query.data)),
            Request::DeliverTx(deliver_tx) => Response::DeliverTx(self.deliver_tx(deliver_tx.tx)),
            Request::Commit => Response::Commit(self.commit()),
            // unhandled messages
            Request::Flush => Response::Flush,
            Request::Echo(_) => Response::Echo(Default::default()),
            Request::InitChain(_) => Response::InitChain(Default::default()),
            Request::BeginBlock(_) => Response::BeginBlock(Default::default()),
            Request::CheckTx(_) => Response::CheckTx(Default::default()),
            Request::EndBlock(_) => Response::EndBlock(Default::default()),
            Request::ListSnapshots => Response::ListSnapshots(Default::default()),
            Request::OfferSnapshot(_) => Response::OfferSnapshot(Default::default()),
            Request::LoadSnapshotChunk(_) => Response::LoadSnapshotChunk(Default::default()),
            Request::ApplySnapshotChunk(_) => Response::ApplySnapshotChunk(Default::default()),
            // response::SetOption is missing a Default impl as of tm-rs 0.26
            Request::SetOption(_) => Response::SetOption(response::SetOption {
                code: 0,
                log: String::new(),
                info: String::new(),
            }),
        };
        tracing::info!(?rsp);
        async move { Ok(rsp) }.boxed()
    }
}
