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

use std::collections::HashMap;

use serde::{de::DeserializeOwned, Serialize};

mod node;
mod hash;

use node::{Node, NodeInitError};
use hash::*;

pub enum Cmd<'a> {
	Hash(HashCmd<'a>),
}

pub enum CmdResult {
    Hash(HashCmdResult),
}

async fn handle_cmd(node: &mut Node, cmd: Cmd<'_>) -> CmdResult {
	match cmd {
		Cmd::Hash(cmd) => CmdResult::Hash(handle_hash_cmd(node, cmd).await),
	}
}

pub struct KadisBuilder<'a> {
    bootstraps: &'a [&'a str],
    port: u16,
    cache_lifetime: u64,
}

impl<'a> KadisBuilder<'a> {
    pub fn new(bootstraps: &'a [&'a str], port: u16, cache_lifetime: u64) -> Self {
        Self {
            bootstraps,
            port,
            cache_lifetime,
        }
    }

    pub fn default() -> Self {
        Self {
            bootstraps: &[],
            port: 0,
            cache_lifetime: 300,
        }
    }

    pub fn bootstraps(&self, bootstraps: &'a [&'a str]) -> Self {
        Self {
            bootstraps,
            port: self.port,
            cache_lifetime: self.cache_lifetime,
        }
    }

    pub fn port(&self, port: u16) -> Self {
        Self {
            bootstraps: self.bootstraps,
            port,
            cache_lifetime: self.cache_lifetime,
        }
    }

    pub fn cache_lifetime(&self, cache_lifetime: u64) -> Self {
        Self {
            bootstraps: self.bootstraps,
            port: self.port,
            cache_lifetime,
        }
    }

    pub fn init(&self) -> Result<Kadis, NodeInitError> {
        let node = Node::new(self.bootstraps, self.port, self.cache_lifetime)?;
        drop(self);

        Ok(Kadis {
            node,
        })
    }
}

pub struct Kadis {
    node: Node,
}

impl Kadis {
	pub async fn hdel(&mut self, key: &str, field: &str) -> Result<(), HDelError> {
		let fields = &[field];
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
		match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Del(res)) => res,
            _ => unreachable!(),
        }
	}

	pub async fn hdel_multiple(&mut self, key: &str, fields: &[&str]) -> Result<(), HDelError> {
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Del(res)) => res,
            _ => unreachable!(),
        }
	}

    pub async fn hexists(&mut self, key: &str, field: &str) -> Result<bool, HExistsError> {
        let cmd = Cmd::Hash(HashCmd::Exists(key, field));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Exists(res)) => res,
            _ => unreachable!(),
        }
    }

    pub async fn hget<T>(&mut self, key: &str, field: &str) -> Result<T, HGetError>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::Get(key, field));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Get(res)) => match res {
                Ok(data) => Ok(bincode::deserialize(&data).unwrap()),
                Err(err) => Err(err),
            },
            _ => unreachable!(),
        }
    }

    pub async fn hget_multiple<T>(&mut self, key: &str, fields: &[&str]) -> Result<Vec<T>, HGetError>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::GetM(key, fields));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::GetM(res)) => match res {
                Ok(data) => Ok(data.iter()
                    .map(|d| bincode::deserialize(d).unwrap())
                    .collect()),
                Err(err) => Err(err),
            },
            _ => unreachable!(),
        }
        
    }

    pub async fn hgetall<T>(&mut self, key: &str) -> Result<HashMap<String, T>, HGetAllError>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::GetAll(key));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::GetAll(res)) => match res {
                Ok(map) => {
                    let mut data = HashMap::new();

                    for (field, d) in map {
                        data.insert(field, bincode::deserialize(&d).unwrap());
                    }

                    Ok(data)
                },
                Err(err) => Err(err),
            },
            _ => unreachable!(),
        }
    }

    pub async fn hincr(&mut self, key: &str, field: &str, inc: u32) -> Result<(), HIncrError> {
        self.hincrf(key, field, inc as f32).await
    }

    pub async fn hincrf(&mut self, key: &str, field: &str, inc: f32) -> Result<(), HIncrError> {
        let cmd = Cmd::Hash(HashCmd::Incr(key, field, inc));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Incr(res)) => res,
            _ => unreachable!(),
        }
    }

    pub async fn hset<T>(&mut self, key: &str, field: &str, value: T) -> Result<(), HSetError>
    where T: Serialize {
        let value = bincode::serialize(&value).unwrap();
        let cmd = Cmd::Hash(HashCmd::Set(key, field, value));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::Set(res)) => res,
            _ => unreachable!(),
        }
    }

    pub async fn hset_multiple<T>(&mut self, key: &str, fields: &[&str], values: &[T]) -> Result<(), HSetError>
    where T: Serialize {
        let values = values.iter().map(|v| bincode::serialize(&v).unwrap()).collect();
        let cmd = Cmd::Hash(HashCmd::SetM(key, fields, values));
        match handle_cmd(&mut self.node, cmd).await {
            CmdResult::Hash(HashCmdResult::SetM(res)) => res,
            _ => unreachable!(),
        }
    }
}
