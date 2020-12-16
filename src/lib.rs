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

use anyhow::{anyhow, Result};
use serde::{de::DeserializeOwned, Serialize};

mod node;
mod hash;
mod util;

use node::Node;
use hash::{handle_hash_cmd, HashCmd};
use util::CmdResult;

pub enum Cmd<'a> {
	Hash(HashCmd<'a>),
}

async fn handle_cmd(node: &mut Node, cmd: Cmd<'_>) -> Vec<CmdResult> {
	match cmd {
		Cmd::Hash(cmd) => handle_hash_cmd(node, cmd),
	}.await
}

fn get_result<T>(res: &CmdResult) -> Result<T>
where T: DeserializeOwned {
    match res {
        CmdResult::Get(res) => match res {
            Ok(value) => Ok(bincode::deserialize(&value).unwrap()),
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
    }
}

fn getall_result<T>(res: &CmdResult) -> Result<HashMap<String, Result<T>>>
where T: DeserializeOwned {
    match res {
        CmdResult::GetAll(res) => match res {
            Ok(map) => {
                let mut new_map = HashMap::new();

                for (field, res) in map {
                    let res = match res {
                        Ok(data) => Ok(bincode::deserialize(&data).unwrap()),
                        Err(err) => Err(anyhow!("{}", err)),
                    };
                    new_map.insert(field.clone(), res);
                }

                Ok(new_map)
            },
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
    }
}

fn put_result(res: &CmdResult) -> Result<()> {
    match res {
        CmdResult::Put(res) => match res {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
    }
}

fn cond_result(res: &CmdResult) -> Result<bool> {
    match res {
        CmdResult::Cond(res) => match res {
            Ok(cond) => Ok(*cond),
            Err(err) => Err(anyhow!("{}", err)),
        },
        _ => unreachable!(),
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

    pub fn init(&self) -> Result<Kadis> {
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
	pub async fn hdel(&mut self, key: &str, field: &str) -> Result<()> {
		let fields = &[field];
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
		let results = handle_cmd(&mut self.node, cmd).await;
        let res = results.first().unwrap();
        put_result(res)
	}

	pub async fn hdel_multiple(&mut self, key: &str, fields: &[&str]) -> Result<()> {
        let cmd = Cmd::Hash(HashCmd::Del(key, fields));
		let results = handle_cmd(&mut self.node, cmd).await;
        let results = results.iter().map(put_result).collect::<Vec<Result<()>>>();
        for res in results {
            res?
        }
        Ok(())
	}

    pub async fn hexists(&mut self, key: &str, field: &str) -> Result<bool> {
        let cmd = Cmd::Hash(HashCmd::Exists(key, field));
        let values = handle_cmd(&mut self.node, cmd).await;
        let value = values.first().unwrap();
        cond_result(value)
    }

    pub async fn hget<T>(&mut self, key: &str, field: &str) -> Result<T>
    where T: DeserializeOwned {
        let fields = &[field];
        let cmd = Cmd::Hash(HashCmd::Get(key, fields));
        let values = handle_cmd(&mut self.node, cmd).await;
        let value = values.first().unwrap();
        get_result(value)
    }

    pub async fn hget_multiple<T>(&mut self, key: &str, fields: &[&str]) -> Vec<Result<T>>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::Get(key, fields));
        let values = handle_cmd(&mut self.node, cmd).await;
        values.iter().map(get_result).collect()
    }

    pub async fn hgetall<T>(&mut self, key: &str) -> Result<HashMap<String, Result<T>>>
    where T: DeserializeOwned {
        let cmd = Cmd::Hash(HashCmd::GetAll(key));
        let values = handle_cmd(&mut self.node, cmd).await;
        let value = values.first().unwrap();
        getall_result(value)
    }

    pub async fn hincr(&mut self, key: &str, field: &str, inc: u32) -> Result<()> {
        self.hincrf(key, field, inc as f32).await
    }

    pub async fn hincrf(&mut self, key: &str, field: &str, inc: f32) -> Result<()> {
        let cmd = Cmd::Hash(HashCmd::Incr(key, field, inc));
        let results = handle_cmd(&mut self.node, cmd).await;
        let res = results.first().unwrap();
        put_result(res)
    }

    pub async fn hset<T>(&mut self, key: &str, field: &str, value: T) -> Result<()>
    where T: Serialize {
        let fields = &[field];
        let values = vec![bincode::serialize(&value).unwrap()];
        let cmd = Cmd::Hash(HashCmd::Set(key, fields, values));
        let results = handle_cmd(&mut self.node, cmd).await;
        let res = results.first().unwrap();
        put_result(res)
    }

    pub async fn hset_multiple<T>(&mut self, key: &str, fields: &[&str], values: &[T]) -> Result<()>
    where T: Serialize {
        let values = values.iter().map(|v| bincode::serialize(&v).unwrap()).collect();
        let cmd = Cmd::Hash(HashCmd::Set(key, fields, values));
        let results = handle_cmd(&mut self.node, cmd).await;
        let results = results.iter().map(put_result).collect::<Vec<Result<()>>>();
        for res in results {
            res?
        }
        Ok(())
    }
}
