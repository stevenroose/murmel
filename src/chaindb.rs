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
//! # Blockchain DB API for a node
//!

use std::io;
use std::sync::{Arc, RwLock};

use bitcoin::{BlockHash, BlockHeader};

use crate::error::Error;
use crate::headercache::CachedHeader;

/// Shared handle to a database storing the block chain
/// protected by an RwLock
pub type SharedChainDB = Arc<RwLock<Box<dyn ChainDB>>>;

/// Blockchain DB API for a client node.
pub trait ChainDB: Send + Sync {

    /// Initialize caches.
    fn init(&mut self) -> Result<(), Error>;

    /// Batch updates. Updates are permanent after finishing a batch.
    fn batch(&mut self) -> Result<(), Error>;

    /// Store a header.
    fn add_header(&mut self, header: &BlockHeader) -> Result<Option<(StoredHeader, Option<Vec<BlockHash>>, Option<Vec<BlockHash>>)>, Error>;

    /// Return position of hash on trunk if hash is on trunk.
    fn pos_on_trunk(&self, hash: BlockHash) -> Option<u32>;

    /// Iterate trunk [from .. tip].
    fn iter_trunk<'a>(&'a self, from: u32) -> Box<dyn Iterator<Item=&'a CachedHeader> + 'a>;

    /// Iterate trunk [genesis .. from] in reverse order from is the tip if not specified.
    fn iter_trunk_rev<'a>(&'a self, from: Option<u32>) -> Box<dyn Iterator<Item=&'a CachedHeader> + 'a>;

    /// Retrieve the id of the block/header with most work.
    fn header_tip(&self) -> Option<CachedHeader>;

    /// Fetch a header by its id from cache.
    fn get_header(&self, id: BlockHash) -> Option<CachedHeader>;

    /// Fetch a header by its id from cache.
    fn get_header_for_height(&self, height: u32) -> Option<CachedHeader>;

    /// Locator for getheaders message.
    fn header_locators(&self) -> Vec<BlockHash>;

    /// Store the header id with most work.
    fn store_header_tip(&mut self, tip: BlockHash) -> Result<(), Error>;

    /// Find header id with most work.
    fn fetch_header_tip(&self) -> Result<Option<BlockHash>, Error>;

    /// Read header from the DB.
    fn fetch_header(&self, id: BlockHash) -> Result<Option<StoredHeader>, Error>;
}

/// A header enriched with information about its position on the blockchain
#[derive(Debug, Clone)]
pub struct StoredHeader {
    /// header
    pub header: BlockHeader,
    /// chain height
    pub height: u32,
    /// log2 of total work
    pub log2work: f64,
}

impl StoredHeader {
	/// Get the header's block hash.
	pub fn block_hash(&self) -> BlockHash {
		self.header.block_hash()
	}
}

#[cfg(feature = "hammersbald")]
impl hammersbald::BitcoinObject<BlockHash> for StoredHeader {
    fn encode<W: io::Write>(&self, mut w: W) -> Result<usize, hammersbald::Error> {
		Ok(
			bitcoin::consensus::Encodable::consensus_encode(&self.header, &mut w)? +
			bitcoin::consensus::Encodable::consensus_encode(&self.height, &mut w)? +
			bitcoin::consensus::Encodable::consensus_encode(&self.log2work.to_bits(), &mut w)?
		)
	}

    fn decode<D: io::Read>(mut d: D) -> Result<Self, hammersbald::Error> {
		Ok(StoredHeader {
			header: bitcoin::consensus::Decodable::consensus_decode(&mut d)?,
			height: bitcoin::consensus::Decodable::consensus_decode(&mut d)?,
			log2work: {
				let bits = bitcoin::consensus::Decodable::consensus_decode(&mut d)?;
				f64::from_bits(bits)
			},
		})
	}

    fn hash(&self) -> BlockHash {
        self.block_hash()
    }
}
