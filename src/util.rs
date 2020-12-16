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

use anyhow::{anyhow, Result};

#[derive(Debug)]
pub enum EventResult {
	Get(Result<Vec<u8>>),
	Put(Result<()>),
    Cond(Result<bool>),
}

impl Clone for EventResult {
    fn clone(&self) -> Self {
        match self {
        	EventResult::Get(res) => EventResult::Get(
	        	match res {
	        		Ok(data) => Ok(data.clone()),
	        		Err(err) => Err(anyhow!("{}", err)),
	        	}
	        ),
        	EventResult::Put(res) => EventResult::Put(
        		match res {
        			Ok(()) => Ok(()),
        			Err(err) => Err(anyhow!("{}", err)),
        		}
        	),
            EventResult::Cond(res) => EventResult::Cond(
                match res {
                    Ok(cond) => Ok(*cond),
                    Err(err) => Err(anyhow!("{}", err)),
                }
            ),
        }
    }
}

pub fn exists_result(res: EventResult) -> EventResult {
    match res {
        EventResult::Get(res) => EventResult::Cond(
            match res {
                Ok(_) => Ok(true),
                Err(err) => if format!("{}", err) == "Not found" {
                    Ok(false)
                } else {
                    Err(anyhow!(err))
                },
            }
        ),
        _ => unreachable!(),
    }
}
