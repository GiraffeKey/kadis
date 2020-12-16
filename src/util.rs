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

use std::collections::HashMap;

use anyhow::{anyhow, Result};

#[derive(Debug)]
pub enum CmdResult {
	Get(Result<Vec<u8>>),
    GetAll(Result<HashMap<String, Result<Vec<u8>>>>),
	Put(Result<()>),
    Cond(Result<bool>),
}

impl Clone for CmdResult {
    fn clone(&self) -> Self {
        match self {
            CmdResult::Get(res) => CmdResult::Get(
                match res {
                    Ok(data) => Ok(data.clone()),
                    Err(err) => Err(anyhow!("{}", err)),
                }
            ),
            CmdResult::GetAll(res) => CmdResult::GetAll(
                match res {
                    Ok(map) => {
                        let mut new_map = HashMap::new();

                        for (field, res) in map {
                            let res = match res {
                                Ok(data) => Ok(data.clone()),
                                Err(err) => Err(anyhow!("{}", err)),
                            };
                            new_map.insert(field.clone(), res);
                        }

                        Ok(new_map)
                    },
                    Err(err) => Err(anyhow!("{}", err)),
                }
            ),
        	CmdResult::Put(res) => CmdResult::Put(
        		match res {
        			Ok(()) => Ok(()),
        			Err(err) => Err(anyhow!("{}", err)),
        		}
        	),
            CmdResult::Cond(res) => CmdResult::Cond(
                match res {
                    Ok(cond) => Ok(*cond),
                    Err(err) => Err(anyhow!("{}", err)),
                }
            ),
        }
    }
}
