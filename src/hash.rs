use libp2p::kad::{
    Kademlia,
    record::{Key, store::MemoryStore},
};

pub enum HashCmd {
	Del(String, Vec<String>),
	Exists,
	Get,
	GetAll,
	IncrBy,
	Keys,
	Len,
	MGet,
	MSet,
	Set,
	SetNx,
	StrLen,
	Vals,
	Scan,
}

use HashCmd::*;

pub fn handle_hash_cmd(kademlia: &mut Kademlia<MemoryStore>, cmd: &HashCmd) {
	match cmd {
		Del(key, fields) => {
			for field in fields {
				let key = Key::new(&format!("kh-{}-{}", key, field));
				kademlia.remove_record(&key);
			}
		},
		_ => unimplemented!(),
	}
}
