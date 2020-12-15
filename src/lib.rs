// Copyright (C) 2020 GiraffeKey
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
//

#![forbid(unsafe_code)]

use std::{
    net::SocketAddr,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};
use subotai::node::{Node, State};

mod hash;
mod util;

use hash::{handle_hash_cmd, HashCmd};

pub enum Cmd {
	Hash(HashCmd),
}

fn handle_cmd(node: &mut Node, cmd: Cmd) -> Option<Vec<Option<Vec<u8>>>> {
	match cmd {
		Cmd::Hash(cmd) => handle_hash_cmd(node, cmd),
	}
}

pub struct Kadis {
    node: Node,
}

impl Kadis {
	pub fn new() -> Result<Self> {
        let node = Node::new().unwrap();
        // node.bootstrap(&origin.local_info().address).unwrap();
        node.wait_for_state(State::OnGrid);

	    Ok(Self {
            node,
	    })
	}

    pub fn address(&self) -> SocketAddr {
        self.node.local_info().address
    }

	// pub fn hdel(&mut self, key: &str, field: &str) {
	// 	let key = key.into();
	// 	let fields = vec![field.into()];
	// 	handle_cmd(Cmd::Hash(HashCmd::Del(key, fields)));
	// }

	// pub fn hdel_multiple(&mut self, key: &str, fields: &[&str]) {
	// 	let key = key.into();
	// 	let fields = fields.iter().map(|f| f.to_string()).collect();
	// 	handle_cmd(Cmd::Hash(HashCmd::Del(key, fields)));
	// }

    pub fn hget<T>(&mut self, key: &str, field: &str) -> Option<T> where T: DeserializeOwned {
        let key = key.into();
        let fields = vec![field.into()];
        let value = handle_cmd(&mut self.node, Cmd::Hash(HashCmd::Get(key, fields))).unwrap();
        let value = value.first().unwrap();
        match value {
            Some(value) => bincode::deserialize(value).unwrap(),
            None => None,
        }
    }

    pub fn hset<T>(&mut self, key: &str, field: &str, value: T) where T: Serialize {
        let key = key.into();
        let fields = vec![field.into()];
        let values = vec![bincode::serialize(&value).unwrap()];
        handle_cmd(&mut self.node, Cmd::Hash(HashCmd::Set(key, fields, values)));
    }
}
