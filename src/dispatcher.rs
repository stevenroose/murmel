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
//! # Messsage dispatcher
//!

use std::thread;
use std::sync::{mpsc, Arc, Mutex};

use bitcoin_p2p::P2PEvent;

/// Dispatcher of incoming messages
pub struct Dispatcher(Arc<Mutex<Vec<mpsc::SyncSender<P2PEvent>>>>);

impl Dispatcher {
    pub fn new(receiver: mpsc::Receiver<P2PEvent>) -> Dispatcher {
        let listeners = Arc::new(Mutex::new(Vec::<mpsc::SyncSender<P2PEvent>>::new()));

        let l2 = listeners.clone();
        thread::Builder::new().name("dispatcher".to_string()).spawn(move || {
            while let Ok(pm) = receiver.recv() {
                let list = l2.lock().unwrap();
                for listener in list.iter() {
                    listener.send(pm.clone()).expect("listener bailed");
                }
            }
            panic!("dispatcher failed");
        }).unwrap();

        Dispatcher(listeners)
    }

    pub fn add_listener(&mut self, listener: mpsc::SyncSender<P2PEvent>) {
        let mut list = self.0.lock().unwrap();
        list.push(listener);
    }
}
