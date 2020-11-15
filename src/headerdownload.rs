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
//! # Download headers
//!

use std::thread;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use bitcoin::{BlockHash, BlockHeader};
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin_p2p::{PeerId, P2P, P2PEvent};
use log::{info, trace, debug, error};

use crate::chaindb::SharedChainDB;
use crate::error::Error;
use crate::timeout::{ExpectedReply, SharedTimeout};
use crate::downstream::SharedDownstream;

pub struct HeaderDownload {
    p2p: Arc<P2P>,
    chaindb: SharedChainDB,
    timeout: SharedTimeout<ExpectedReply>,
    downstream: SharedDownstream
}

impl HeaderDownload {
    pub fn new(
        chaindb: SharedChainDB,
        p2p: Arc<P2P>,
        timeout: SharedTimeout<ExpectedReply>,
        downstream: SharedDownstream,
    ) -> mpsc::SyncSender<P2PEvent> {
        let (sender, receiver) = mpsc::sync_channel(10);

        let mut headerdownload = HeaderDownload {
            chaindb,
            p2p,
            timeout,
            downstream: downstream,
        };

        thread::Builder::new().name("header download".to_string()).spawn(move || {
            headerdownload.run(receiver);
        }).unwrap();

        sender
    }

    fn run(&mut self, receiver: mpsc::Receiver<P2PEvent>) {
        loop {
            while let Ok(msg) = receiver.recv_timeout(Duration::from_millis(1000)) {
                if let P2PEvent::Connected(peer) = msg {
                    if self.is_serving_blocks(peer) {
                        trace!("serving blocks peer={}", peer);
                        if let Err(e) = self.get_headers(peer) {
                            error!("Error asking headers from peer {}: {}", peer, e);
                        }
                    }
                } else if let P2PEvent::Message(peer, NetworkMessage::Headers(headers)) = msg {
                    if self.is_serving_blocks(peer) {
                        if let Err(e) = self.headers(headers, peer) {
                            error!("Error handling headers from peer {}: {}", peer, e);
                        }
                    }
                    return;
                } else if let P2PEvent::Message(peer, NetworkMessage::Inv(invs)) = msg {
                    if self.is_serving_blocks(peer) {
                        if let Err(e) = self.inv(invs, peer) {
                            error!("Error handling inv from peer {}: {}", peer, e);
                        }
                    }
                }
            }
            self.timeout.lock().unwrap().check(vec!(ExpectedReply::Headers));
        }
    }

    fn is_serving_blocks(&self, peer: PeerId) -> bool {
        match self.p2p.peer_version(peer) {
            Ok(Some(ver)) => ver.services.has(ServiceFlags::NETWORK),
            Ok(None) => false,
            Err(_) => false, // peer unknown
        }
    }

    // process an incoming inventory announcement
    fn inv(&mut self, invs: Vec<Inventory>, peer: PeerId) -> Result<(), Error> {
        let mut ask_for_headers = false;
        for inventory in invs {
            // only care for blocks
			if let Inventory::Block(hash) = inventory {
                let chaindb = self.chaindb.read().unwrap();
                if chaindb.get_header(hash).is_none() {
                    debug!("received inv for new block {} peer={}", hash, peer);
                    // ask for header(s) if observing a new block
                    ask_for_headers = true;
                }
            }
        }
        if ask_for_headers {
            self.get_headers(peer)?;
        }
        Ok(())
    }

    /// get headers this peer is ahead of us
    fn get_headers(&mut self, peer: PeerId) -> Result<(), Error> {
        if self.timeout.lock().unwrap().is_busy_with(peer, ExpectedReply::Headers) {
            return Ok(());
        }
        let chaindb = self.chaindb.read().unwrap();
        let locator = chaindb.header_locators();
        if locator.len() > 0 {
            let first = if locator.len() > 0 {
                *locator.first().unwrap()
            } else {
                BlockHash::default()
            };
            self.timeout.lock().unwrap().expect(peer, 1, ExpectedReply::Headers);
            let msg = NetworkMessage::GetHeaders(GetHeadersMessage::new(locator, first));
            if let Err(e) = self.p2p.send_message(peer, msg) {
                error!("Error sending message to peer {}: {}", peer, e);
            }
        }
        Ok(())
    }

    fn headers(&mut self, headers: Vec<BlockHeader>, peer: PeerId) -> Result<(), Error> {
        self.timeout.lock().unwrap().received(peer, 1, ExpectedReply::Headers);

        if headers.len() > 0 {
            // current height
            let mut height;
            // some received headers were not yet known
            let mut some_new = false;
            let mut moved_tip = None;
            {
                let chaindb = self.chaindb.read().unwrap();

                if let Some(tip) = chaindb.header_tip() {
                    height = tip.stored.height;
                } else {
                    return Err(Error::NoTip);
                }
            }

            let mut headers_queue = VecDeque::new();
            headers_queue.extend(headers.iter());
            while !headers_queue.is_empty() {
                let mut disconnected_headers = Vec::new();
                let mut connected_headers = Vec::new();
                {
                    let mut chaindb = self.chaindb.write().unwrap();
                    while let Some(header) = headers_queue.pop_front() {
                        // add to blockchain - this also checks proof of work
                        match chaindb.add_header(&header) {
                            Ok(Some((stored, unwinds, forwards))) => {
                                connected_headers.push((stored.header.clone(), stored.height));
                                // POW is ok, stored top chaindb
                                some_new = true;

                                if let Some(forwards) = forwards {
                                    moved_tip = Some(forwards.last().unwrap().clone());
                                }
                                height = stored.height;

                                if let Some(unwinds) = unwinds {
                                    disconnected_headers.extend(unwinds.iter()
                                        .map(|h| chaindb.get_header(*h).unwrap().stored.header));
                                    break;
                                }
                            }
                            Ok(None) => {}
                            Err(Error::SpvBadProofOfWork) => {
                                info!("Incorrect POW, banning peer={}", peer);
                                if let Err(e) = self.p2p.ban_peer(peer) {
                                    error!("Error banning peer {}: {}", peer, e);
                                }
                                return Ok(());
                            }
                            Err(e) => {
                                debug!("error {} processing header {} ", e, header.block_hash());
                                return Ok(());
                            }
                        }
                    }
                    chaindb.batch()?;
                }
                // must call downstream outside of chaindb lock as it might also lock chaindb
                let mut downstream = self.downstream.lock().unwrap();
                for header in &disconnected_headers {
                    downstream.block_disconnected(header);
                }
                for (header, height) in &connected_headers {
                    downstream.header_connected(header, *height);
                }
            }

            if some_new {
                // ask if peer knows even more
                self.get_headers(peer)?;
            }

            if let Some(new_tip) = moved_tip {
                info!("received {} headers new tip={} from peer={}", headers.len(), new_tip, peer);
                self.p2p.set_height(height);
            } else {
                debug!("received {} known or orphan headers [{} .. {}] from peer={}", headers.len(), headers[0].block_hash(), headers[headers.len()-1].block_hash(), peer);
            }
        }
        Ok(())
    }
}
