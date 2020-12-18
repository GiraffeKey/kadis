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
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

use async_std::task;
use fnv::FnvHashMap;
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

#[derive(Clone)]
pub enum GetError {
    NotFound,
    QuorumFailed,
    Timeout,
}

#[derive(Clone)]
pub enum PutError {
    QuorumFailed,
    Timeout,
}

#[derive(Clone)]
pub enum EventResult {
    Get(Result<Vec<u8>, GetError>),
    Put(Result<(), PutError>),
}

#[derive(Debug)]
pub enum NodeInitError {
    ParseAddress {
        address: String,
    },
    ParseBootstrap {
        address: String,
    },
    DialAddr {
        address: String,
    },
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns,
    #[behaviour(ignore)]
    event_results: FnvHashMap<String, EventResult>,
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
                        GetRecordError::NotFound { key, .. } => (key, GetError::NotFound),
                        GetRecordError::QuorumFailed { key, .. } => (key, GetError::QuorumFailed),
                        GetRecordError::Timeout { key, .. } => (key, GetError::Timeout),
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
                        PutRecordError::QuorumFailed { key, .. } => (key, PutError::QuorumFailed),
                        PutRecordError::Timeout { key, .. } => (key, PutError::Timeout),
                    };
                    let key = std::str::from_utf8(key.as_ref()).unwrap();
                    let name = format!("put-{}", key);
                    self.event_results.insert(name, EventResult::Put(Err(err)));
                },
                _ => (),
            },
            _ => (),
        }
    }
}

pub struct Node {
    swarm: Arc<Mutex<Swarm<Behaviour>>>,
    cache: Arc<Mutex<FnvHashMap<String, Vec<u8>>>>,
}

impl Node {
	pub fn new(bootstraps: &[&str], port: u16, cache_lifetime: u64) -> Result<Self, NodeInitError> {
	    let local_key = identity::Keypair::generate_ed25519();
	    let local_peer_id = PeerId::from(local_key.public());

	    let transport = {
		    let dh_keys = noise::Keypair::<X25519Spec>::new().into_authentic(&local_key).unwrap();
		    let noise = NoiseConfig::xx(dh_keys).into_authenticated();
		    let tcp = TcpConfig::new();

	    	DnsConfig::new(tcp)
                .unwrap()
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
            let event_results = FnvHashMap::default();
		    let kademlia = Kademlia::new(local_peer_id.clone(), store);
		    let mdns = task::block_on(Mdns::new()).unwrap();
		    let behaviour = Behaviour { event_results, kademlia, mdns };
		    Swarm::new(transport, behaviour, local_peer_id)
		};

        let address = format!("/ip4/0.0.0.0/tcp/{}", port);
        let address = match address.parse() {
            Ok(address) => address,
            Err(_) => return Err(NodeInitError::ParseAddress { address }),
        };
	    Swarm::listen_on(&mut swarm, address).unwrap();

        for address in bootstraps {
            let dial_address = match address.parse() {
                Ok(address) => address,
                Err(_) => return Err(NodeInitError::ParseBootstrap { address: address.to_string() }),
            };
            match Swarm::dial_addr(&mut swarm, dial_address) {
                Ok(_) => (),
                Err(_) => return Err(NodeInitError::DialAddr { address: address.to_string() }),
            }
        }

        let swarm = Arc::new(Mutex::new(swarm));
        let cache = Arc::new(Mutex::new(FnvHashMap::default()));

	    let mut listening = false;
        let swarm_clone = swarm.clone();
	    task::spawn(future::poll_fn(move |cx: &mut Context<'_>| -> Poll<Result<(), ()>> {
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

        // Clean cache on an interval of cache_lifetime
        let cache_clone = cache.clone();
        task::spawn(async move {
            loop {
                cache_clone.lock().unwrap().clear();
                task::sleep(Duration::from_secs(cache_lifetime)).await;
            }
        });

        if !bootstraps.is_empty() {
            thread::sleep(Duration::from_millis(100));
        }

	    Ok(Self {
            swarm,
            cache,
	    })
	}

    fn wait_for_result(&self, name: String) -> EventResult {
        loop {
            let event_results = &mut self.swarm.lock().unwrap().event_results;
            match event_results.get(&name) {
                Some(res) => {
                    let res = res.clone();
                    event_results.remove(&name);
                    return res;
                },
                None => (),
            }
        }
    }

	pub async fn get(&mut self, key: &str) -> Result<Vec<u8>, GetError> {
        {
            let kademlia = &mut self.swarm.lock().unwrap().kademlia;
            let key = Key::new(&key);
            kademlia.get_record(&key, Quorum::One);
        }

        if let Some(value) = self.cache.lock().unwrap().get(key) {
            return Ok(value.clone());
        }

        let name = format!("get-{}", key);
        let res = self.wait_for_result(name);
        match res {
            EventResult::Get(res) => match res {
                Ok(value) => {
                    self.cache.lock().unwrap().insert(key.into(), value.clone());
                    Ok(value)
                },
                Err(err) => Err(err),
            },
            _ => unreachable!(),
        }
	}

	pub async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), PutError> {
        {
            let kademlia = &mut self.swarm.lock().unwrap().kademlia;
            let key = Key::new(&key);
            let record = Record {
                key,
                value: value.clone(),
                publisher: None,
                expires: None,
            };
            kademlia.put_record(record, Quorum::One).unwrap();
        }

        let name = format!("put-{}", key);
        let res = self.wait_for_result(name);
        match res {
            EventResult::Put(res) => match res {
                Ok(()) => {
                    self.cache.lock().unwrap().insert(key.into(), value);
                    Ok(())
                },
                Err(err) => Err(err),
            },
            _ => unreachable!(),
        }
	}

    pub fn remove(&mut self, key: &str) {
        {
            let kademlia = &mut self.swarm.lock().unwrap().kademlia;
            let key = Key::new(&key);
            kademlia.remove_record(&key);
        }
        self.cache.lock().unwrap().remove(key.into());
    }
}
