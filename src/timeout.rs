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
//! # Keep track of peer timeouts
//!

use std::{cmp, fmt};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bitcoin_p2p::{PeerId, P2P};
use log::debug;

pub type SharedTimeout<Reply> = Arc<Mutex<Timeout<Reply>>>;

const TIMEOUT:u64 = 60;

#[derive(Eq, PartialEq, Hash, Debug)]
pub enum ExpectedReply {
    Block,
    Headers,
    Pong,
    FilterHeader,
    FilterCheckpoints,
    Filter,
}

pub struct Timeout<Reply: Eq + Hash + fmt::Debug> {
    p2p: Arc<P2P>,
    timeouts: HashMap<PeerId, u64>,
    expected: HashMap<PeerId, HashMap<Reply, usize>>,
}

impl<Reply: Eq + Hash + fmt::Debug> Timeout<Reply> {
    pub fn new(p2p: Arc<P2P>) -> Timeout<Reply> {
        Timeout {
            p2p,
            timeouts: HashMap::new(),
            expected: HashMap::new(),
        }
    }

    pub fn forget(&mut self, peer: PeerId) {
        self.timeouts.remove(&peer);
        self.expected.remove(&peer);
    }

    pub fn expect(&mut self, peer: PeerId, n: usize, what: Reply) {
        self.timeouts.insert(peer, Self::now() + TIMEOUT);
        *self.expected.entry(peer).or_insert(HashMap::new()).entry(what).or_insert(0) += n;
    }

    pub fn received(&mut self, peer: PeerId, n: usize, what: Reply) {
        if let Some(expected) = self.expected.get(&peer) {
            if let Some(m) = expected.get(&what) {
                if *m > 0 {
                    self.timeouts.insert(peer, Self::now() + TIMEOUT);
                }
            }
        }
        {
            let expected = self.expected.entry(peer).or_insert(HashMap::new()).entry(what).or_insert(n);
            *expected -= cmp::min(n, *expected);
        }
        if let Some(expected) = self.expected.get(&peer) {
            if expected.values().all(|v| *v == 0) {
                self.timeouts.remove(&peer);
            }
        }
    }

    pub fn is_busy(&self, peer: PeerId) -> bool {
        self.timeouts.contains_key(&peer)
    }

    pub fn is_busy_with(&self, peer: PeerId, what: Reply) -> bool {
        if self.timeouts.contains_key(&peer) {
            if let Some(expected) = self.expected.get(&peer) {
                if let Some(n) = expected.get(&what) {
                    return *n > 0;
                }
            }
        }
        false
    }

    pub fn check(&mut self, expected: Vec<Reply>) {
        let mut banned = Vec::new();
        for (peer, timeout) in &self.timeouts {
            if *timeout < Self::now() {
                let missing = expected.iter().any(|expected| {
                    if let Some(e) = self.expected.get(peer) {
                        if let Some(n) = e.get(expected) {
                            *n > 0
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                });

                if missing {
                    debug!("too slow answering {:?} requests {:?}, disconnecting peer={}", expected, self.expected.get(peer), *peer);
                    if let Err(e) = self.p2p.disconnect_peer(*peer) {
                        error!("Error disconnecting peer {}: {}", peer, e);
                    }
                    banned.push(*peer);
                }
            }
        }
        for peer in &banned {
            self.timeouts.remove(peer);
            self.expected.remove(peer);
        }
    }

    fn now() -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
    }
}
