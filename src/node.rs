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
    collections::HashMap,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

use anyhow::{anyhow, Result};
use async_std::task;
use futures::prelude::*;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{
    GetRecordError,
    GetRecordOk,
    Kademlia,
    KademliaEvent,
    PutRecordError,
    PutRecordOk,
    QueryResult,
    Quorum,
    Record,
    record::Key,
};
use libp2p::{
    core::{
        muxing::StreamMuxerBox,
        upgrade::{SelectUpgrade, Version},
    },
    dns::{DnsConfig},
    mdns::{Mdns, MdnsEvent},
    mplex::MplexConfig,
    noise::{self, NoiseConfig, X25519Spec},
    swarm::{NetworkBehaviourEventProcess},
    tcp::TcpConfig,
    yamux::YamuxConfig,
    PeerId, Swarm, Transport, NetworkBehaviour,
    identity,
};

use crate::util::EventResult;

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns,
    #[behaviour(ignore)]
    event_results: HashMap<String, EventResult>,
}

impl NetworkBehaviourEventProcess<MdnsEvent> for Behaviour {
    fn inject_event(&mut self, event: MdnsEvent) {
        if let MdnsEvent::Discovered(list) = event {
            for (peer_id, multiaddr) in list {
                self.kademlia.add_address(&peer_id, multiaddr);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<KademliaEvent> for Behaviour {
    fn inject_event(&mut self, message: KademliaEvent) {
        match message {
            KademliaEvent::QueryResult { result, .. } => match result {
                QueryResult::GetRecord(Ok(GetRecordOk { records, .. })) => {
                    let record = &records.last().unwrap().record;
                    let key = std::str::from_utf8(record.key.as_ref()).unwrap();
                    let name = format!("get-{}", key);
                    let value = record.value.clone();
                    self.event_results.insert(name, EventResult::Get(Ok(value)));
                },
                QueryResult::GetRecord(Err(err)) => {
                    let (key, err) = match err {
                        GetRecordError::NotFound { key, .. } => (key, anyhow!("Not found")),
                        GetRecordError::QuorumFailed { key, .. } => (key, anyhow!("Quorum failed")),
                        GetRecordError::Timeout { key, .. } => (key, anyhow!("Timed out")),
                    };
                    let key = std::str::from_utf8(key.as_ref()).unwrap();
                    let name = format!("get-{}", key);
                    self.event_results.insert(name, EventResult::Get(Err(err)));
                },
                QueryResult::PutRecord(Ok(PutRecordOk { key })) => {
                    let key = std::str::from_utf8(key.as_ref()).unwrap();
                    let name = format!("put-{}", key);
                    self.event_results.insert(name, EventResult::Put(Ok(())));
                },
                QueryResult::PutRecord(Err(err)) => {
                    let (key, err) = match err {
                        PutRecordError::QuorumFailed { key, .. } => (key, anyhow!("Quorum failed")),
                        PutRecordError::Timeout { key, .. } => (key, anyhow!("Timed out")),
                    };
                    let key = std::str::from_utf8(key.as_ref()).unwrap();
                    let name = format!("put-{}", key);
                    self.event_results.insert(name, EventResult::Get(Err(err)));
                },
                _ => (),
            },
            _ => (),
        }
    }
}

pub struct Node {
    swarm: Arc<Mutex<Swarm<Behaviour>>>,
}

impl Node {
	pub fn new<'a>(bootstraps: &[&str], port: u16) -> Result<Self> {
	    let local_key = identity::Keypair::generate_ed25519();
	    let local_peer_id = PeerId::from(local_key.public());

	    let transport = {
		    let dh_keys = noise::Keypair::<X25519Spec>::new().into_authentic(&local_key)?;
		    let noise = NoiseConfig::xx(dh_keys).into_authenticated();
		    let tcp = TcpConfig::new();

	    	DnsConfig::new(tcp)?
		        .upgrade(Version::V1)
		        .authenticate(noise)
		        .multiplex(SelectUpgrade::new(
		            YamuxConfig::default(),
		            MplexConfig::new(),
		        ))
		        .map(|(peer, muxer), _| (peer, StreamMuxerBox::new(muxer)))
		        .boxed()
	    };

	    let mut swarm = {
	    	let store = MemoryStore::new(local_peer_id.clone());
            let event_results = HashMap::new();
		    let kademlia = Kademlia::new(local_peer_id.clone(), store);
		    let mdns = task::block_on(Mdns::new())?;
		    let behaviour = Behaviour { event_results, kademlia, mdns };
		    Swarm::new(transport, behaviour, local_peer_id)
		};

        let address = format!("/ip4/0.0.0.0/tcp/{}", port);
	    Swarm::listen_on(&mut swarm, address.parse()?)?;

        for bootstrap in bootstraps {
            Swarm::dial_addr(&mut swarm, bootstrap.parse()?)?;
        }

        let swarm = Arc::new(Mutex::new(swarm));

	    let mut listening = false;
        let swarm_clone = swarm.clone();
	    task::spawn(future::poll_fn(move |cx: &mut Context<'_>| -> Poll<Result<()>> {
	        loop {
                let mut swarm = swarm_clone.lock().unwrap();
	            match swarm.poll_next_unpin(cx) {
	                Poll::Ready(Some(event)) => log::info!("{:?}", event),
	                Poll::Ready(None) => return Poll::Ready(Ok(())),
	                Poll::Pending => {
	                    if !listening {
	                        if let Some(addr) = Swarm::listeners(&swarm).next() {
	                            log::info!("Listening on {:?}", addr);
	                            listening = true;
	                        }
	                    }
                        break;
	                }
	            }
	        }
	        Poll::Pending
	    }));

        if !bootstraps.is_empty() {
            thread::sleep(Duration::from_millis(100));
        }

	    Ok(Self {
            swarm,
	    })
	}

	pub async fn get(&mut self, key: String) -> EventResult {
        {
            let kademlia = &mut self.swarm.lock().unwrap().kademlia;
            let key = Key::new(&key);
            kademlia.get_record(&key, Quorum::One);
        }

        let name = format!("get-{}", key);
        loop {
            match self.swarm.lock().unwrap().event_results.get(&name) {
                Some(res) =>  return res.clone(),
                None => (),
            }
        }
	}

	pub async fn put(&mut self, key: String, value: Vec<u8>) -> EventResult {
        {
            let kademlia = &mut self.swarm.lock().unwrap().kademlia;
            let key = Key::new(&key);
            let record = Record {
                key,
                value,
                publisher: None,
                expires: None,
            };
            kademlia.put_record(record, Quorum::One).unwrap();
        }

        let name = format!("put-{}", key);
        loop {
            match self.swarm.lock().unwrap().event_results.get(&name) {
                Some(res) => return res.clone(),
                None => (),
            }
        }
	}

    pub fn remove(&mut self, key: String) {
        let kademlia = &mut self.swarm.lock().unwrap().kademlia;
        let key = Key::new(&key);
        kademlia.remove_record(&key);
    }
}
