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

use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};

mod node;
mod hash;
mod util;

use node::Node;
use hash::{handle_hash_cmd, HashCmd};
use util::EventResult;

pub enum Cmd<'a> {
	Hash(HashCmd<'a>),
}

async fn handle_cmd(node: &mut Node, cmd: Cmd<'_>) -> Option<Vec<EventResult>> {
	match cmd {
		Cmd::Hash(cmd) => handle_hash_cmd(node, cmd),
	}.await
}

fn get_result<T>(event: &EventResult) -> Result<T>
where T: DeserializeOwned {
    match event {
        EventResult::Get(res) => match res {
            Ok(value) => Ok(bincode::deserialize(&value).unwrap()),
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
    }
}

fn put_result(event: &EventResult) -> Result<()> {
    match event {
        EventResult::Put(res) => match res {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
    }
}

pub struct Kadis {
    node: Node,
}

impl Kadis {
	pub fn new(bootstraps: &[&str], port: u16) -> Result<Self> {
        let node = Node::new(bootstraps, port)?;

	    Ok(Self {
            node,
	    })
	}

	pub async fn hdel(&mut self, key: &str, field: &str) {
		let fields = &[field];
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
		handle_cmd(&mut self.node, cmd).await;
	}

	pub async fn hdel_multiple(&mut self, key: &str, fields: &[&str]) {
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
		handle_cmd(&mut self.node, cmd).await;
	}

    pub async fn hget<T>(&mut self, key: &str, field: &str) -> Result<T>
    where T: DeserializeOwned {
        let fields = &[field];
        let cmd = Cmd::Hash(HashCmd::Get(key, fields));
        let values = handle_cmd(&mut self.node, cmd).await.unwrap();
        let value = values.first().unwrap();
        get_result(value)
    }

    pub async fn hget_multiple<T>(&mut self, key: &str, fields: &[&str]) -> Vec<Result<T>>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::Get(key, fields));
        let values = handle_cmd(&mut self.node, cmd).await.unwrap();
        values.iter().map(get_result).collect()
    }

    pub async fn hset<T>(&mut self, key: &str, field: &str, value: T) -> Result<()>
    where T: Serialize {
        let fields = &[field];
        let values = vec![bincode::serialize(&value).unwrap()];
        let cmd = Cmd::Hash(HashCmd::Set(key, fields, values));
        let results = handle_cmd(&mut self.node, cmd).await.unwrap();
        let res = results.first().unwrap();
        put_result(res)
    }

    pub async fn hset_multiple<T>(&mut self, key: &str, fields: &[&str], values: &[T]) -> Vec<Result<()>>
    where T: Serialize {
        let values = values.iter().map(|v| bincode::serialize(&v).unwrap()).collect();
        let cmd = Cmd::Hash(HashCmd::Set(key, fields, values));
        let results = handle_cmd(&mut self.node, cmd).await.unwrap();
        results.iter().map(put_result).collect()
    }
}
