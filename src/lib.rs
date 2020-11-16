//
// Copyright 2018-2019 Tamas Blummer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//!
//! # Murmel Bitcoin node
//!
//! This library implements a Simplified Payment Verification (SPV) of Bitcoin
//!

#![deny(non_upper_case_globals)]
#![deny(non_camel_case_types)]
#![deny(non_snake_case)]
#![deny(unused_mut)]
#![deny(unused_must_use)]
#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[cfg(feature = "lightning")]
mod lightning;
mod headercache;

pub mod dns;
pub mod timeout;
pub mod headerdownload;
pub mod downstream;
pub mod dispatcher;
pub mod error;
pub mod chaindb;

#[cfg(feature = "hammersbald")]
pub mod hammersbald;

pub use error::Error;


use std::{net, thread};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bitcoin::{Network};
use bitcoin::network::constants::ServiceFlags;
use bitcoin_p2p::{Config as P2PConfig, P2P, PeerType};
use rand::{RngCore, thread_rng};

use crate::hammersbald::Hammersbald;
use crate::dispatcher::Dispatcher;
use crate::dns::dns_seed;
use crate::headerdownload::HeaderDownload;
use crate::timeout::Timeout;
use crate::downstream::{DownStreamDummy, SharedDownstream};
use crate::chaindb::SharedChainDB;
#[cfg(feature = "lightning")]
use crate::lightning::LightningConnector;

const MAX_PROTOCOL_VERSION: u32 = 70001;
const USER_AGENT: &'static str = concat!("/Murmel:", env!("CARGO_PKG_VERSION"), '/');

/// The complete stack
pub struct Murmel {
    p2p: Arc<P2P>,
    /// this should be accessed by Lightning
    pub downstream: SharedDownstream
}

impl Murmel {
    /// open DBs
    pub fn open_db(path: Option<&Path>, network: Network, _birth: u64) -> Result<SharedChainDB, Error> {
        let mut chaindb = {
            if let Some(path) = path {
                #[cfg(feature = "hammersbald")]
                Hammersbald::new(path, network)?
            } else {
                #[cfg(feature = "hammersbald")]
                Hammersbald::mem(network)?
            }
		};
        chaindb.init()?;
        Ok(Arc::new(RwLock::new(chaindb)))
    }

    /// Construct the stack
    pub fn new(network: Network, listen: Vec<SocketAddr>, chaindb: SharedChainDB) -> Result<Murmel, Error> {
        let p2pconfig = P2PConfig {
            network: network,
            protocol_version: MAX_PROTOCOL_VERSION,
            services: ServiceFlags::WITNESS,
            relay: true,
			send_headers: true,
            user_agent: USER_AGENT.to_owned(),
            ..Default::default()
        };

        let p2p = P2P::new(p2pconfig)?;

        #[cfg(feature = "lightning")] let lightning = Arc::new(Mutex::new(LightningConnector::new(network, p2p_control.clone())));
        #[cfg(not(feature = "lightning"))] let lightning = Arc::new(Mutex::new(DownStreamDummy {}));

        let timeout = Arc::new(Mutex::new(Timeout::new(p2p.clone())));

        let mut dispatcher = Dispatcher::new(p2p.take_event_channel().unwrap());
        dispatcher.add_listener(
            HeaderDownload::new(chaindb.clone(), p2p.clone(), timeout.clone(), lightning.clone())
        );

        for addr in listen.into_iter() {
            let listener = net::TcpListener::bind(addr)?;
            let p2p_clone = p2p.clone();
            thread::Builder::new().name(format!("netlistener_{}", addr)).spawn(move ||{
                for stream in listener.incoming() {
                    let stream = match stream {
                        Ok(s) => s,
                        Err(e) => {
                            debug!("Error with incoming stream: {}", e);
                            continue;
                        }
                    };
                    let addr = stream.peer_addr();
                    match p2p_clone.add_peer(stream, PeerType::Inbound) {
                        //TODO(stevenroose) perhaps have a limit?
                        Ok(id) => info!("Added new inbound peer #{}: {:?}", id, addr),
                        Err(e) => error!("Failed to add new inbound peer {:?} to p2p: {}", addr, e),
                    }
                }
            }).expect("failed to start listen thread");
            info!("Started listening on {}", addr);
        }

        Ok(Murmel {
            p2p: p2p,
            downstream: lightning,
        })
    }

    /// Run the stack. This should be called AFTER registering listener of the ChainWatchInterface,
    /// so they are called as the stack catches up with the blockchain
    /// * peers - connect to these peers at startup (might be empty)
    /// * min_connections - keep connections with at least this number of peers. Peers will be randomly chosen
    /// from those discovered in earlier runs
    pub fn run(
        &mut self,
        peers: Vec<SocketAddr>,
        min_connections: usize,
    ) -> Result<(), Error> {
        for addr in &peers {
            match self.p2p.connect_peer(*addr) {
                Ok(peer) => info!("Connected to peer #{} on {}", peer, addr),
                Err(e) => error!("Error connecting to peer at {}: {}", addr, e),
            }
        }

        // Run a thread to keep a minimum number of peers.
        let p2p = self.p2p.clone();
        thread::Builder::new().name("keep_connected".to_owned()).spawn(move || {
            let dns = dns_seed(p2p.config().network);
            let mut earlier = HashSet::<SocketAddr>::new();
            loop {
                if p2p.nb_connected_peers() < min_connections {
                    let eligible = dns.iter().cloned().filter(|a| !earlier.contains(a)).collect::<Vec<_>>();
                    if eligible.len() > 0 {
                        let mut rng = thread_rng();
                        let choice = eligible[(rng.next_u32() as usize) % eligible.len()];
                        earlier.insert(choice.clone());
                        //TODO(stevenroose) start_height
                        match p2p.connect_peer(choice) {
                            Ok(peer) => info!("Connected to peer #{} on {}", peer, choice),
                            Err(e) => error!("Error connecting to peer at {}: {}", choice, e),
                        }
                    }
                }
                thread::sleep(Duration::from_secs(10));
            }
        }).expect("failed to run keep_connected thread");

        Ok(())
    }
}
