use anyhow::{anyhow, Result};

#[derive(Debug)]
pub enum EventResult {
	Get(Result<Vec<u8>>),
	Put(Result<()>),
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
        }
    }
}
