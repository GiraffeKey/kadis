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
};

use anyhow::Result;
use async_std::task;
use futures::prelude::*;
use libp2p::kad::record::store::MemoryStore;
use libp2p::kad::{
    AddProviderOk,
    Kademlia,
    KademliaEvent,
    PeerRecord,
    PutRecordOk,
    QueryResult,
    Record,
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

mod hash;

use hash::{handle_hash_cmd, HashCmd};

// We create a custom network behaviour that combines Kademlia and mDNS.
#[derive(NetworkBehaviour)]
struct KadisBehaviour {
    kademlia: Kademlia<MemoryStore>,
    mdns: Mdns
}

impl NetworkBehaviourEventProcess<MdnsEvent> for KadisBehaviour {
    // Called when `mdns` produces an event.
    fn inject_event(&mut self, event: MdnsEvent) {
        if let MdnsEvent::Discovered(list) = event {
            for (peer_id, multiaddr) in list {
                self.kademlia.add_address(&peer_id, multiaddr);
            }
        }
    }
}

impl NetworkBehaviourEventProcess<KademliaEvent> for KadisBehaviour {
    // Called when `kademlia` produces an event.
    fn inject_event(&mut self, message: KademliaEvent) {
        match message {
            KademliaEvent::QueryResult { result, .. } => match result {
                QueryResult::GetProviders(Ok(ok)) => {
                    for peer in ok.providers {
                        println!(
                            "Peer {:?} provides key {:?}",
                            peer,
                            std::str::from_utf8(ok.key.as_ref()).unwrap()
                        );
                    }
                }
                QueryResult::GetProviders(Err(err)) => {
                    eprintln!("Failed to get providers: {:?}", err);
                }
                QueryResult::GetRecord(Ok(ok)) => {
                    for PeerRecord { record: Record { key, value, .. }, ..} in ok.records {
                        println!(
                            "Got record {:?} {:?}",
                            std::str::from_utf8(key.as_ref()).unwrap(),
                            std::str::from_utf8(&value).unwrap(),
                        );
                    }
                }
                QueryResult::GetRecord(Err(err)) => {
                    eprintln!("Failed to get record: {:?}", err);
                }
                QueryResult::PutRecord(Ok(PutRecordOk { key })) => {
                    println!(
                        "Successfully put record {:?}",
                        std::str::from_utf8(key.as_ref()).unwrap()
                    );
                }
                QueryResult::PutRecord(Err(err)) => {
                    eprintln!("Failed to put record: {:?}", err);
                }
                QueryResult::StartProviding(Ok(AddProviderOk { key })) => {
                    println!("Successfully put provider record {:?}",
                        std::str::from_utf8(key.as_ref()).unwrap()
                    );
                }
                QueryResult::StartProviding(Err(err)) => {
                    eprintln!("Failed to put provider record: {:?}", err);
                }
                _ => {}
            }
            _ => {}
        }
    }
}

pub enum Cmd {
	Hash(HashCmd),
}

fn handle_cmd(kademlia: &mut Kademlia<MemoryStore>, cmd: &Cmd) {
	match cmd {
		Cmd::Hash(cmd) => handle_hash_cmd(kademlia, cmd),
	}
}

pub struct Kadis {
	cmd_queue: Arc<Mutex<Vec<Cmd>>>,
}

impl Kadis {
	pub fn new() -> Result<Self> {
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
		    let kademlia = Kademlia::new(local_peer_id.clone(), store);
		    let mdns = task::block_on(Mdns::new())?;
		    let behaviour = KadisBehaviour { kademlia, mdns };
		    Swarm::new(transport, behaviour, local_peer_id)
		};

		let cmd_queue = Arc::new(Mutex::new(Vec::new()));

	    Swarm::listen_on(&mut swarm, "/ip4/0.0.0.0/tcp/0".parse()?)?;

	    let mut listening = false;
	    let cmd_queue_clone = cmd_queue.clone();
	    task::spawn(future::poll_fn(move |cx: &mut Context<'_>| -> Poll<Result<()>> {
	        for cmd in cmd_queue_clone.lock().unwrap().iter() {
	        	handle_cmd(&mut swarm.kademlia, cmd)
	        }
	        loop {
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
	                    break
	                }
	            }
	        }
	        Poll::Pending
	    }));

	    Ok(Self {
	    	cmd_queue,
	    })
	}

	fn add_cmd(&mut self, cmd: Cmd) {
		self.cmd_queue.lock().unwrap().push(cmd);
	}

	pub fn hdel(&mut self, key: &str, field: &str) {
		let key = key.into();
		let fields = vec![field.into()];
		self.add_cmd(Cmd::Hash(HashCmd::Del(key, fields)))
	}

	pub fn hdel_multiple(&mut self, key: &str, fields: &[&str]) {
		let key = key.into();
		let fields = fields.iter().map(|f| f.to_string()).collect();
		self.add_cmd(Cmd::Hash(HashCmd::Del(key, fields)))
	}
}
