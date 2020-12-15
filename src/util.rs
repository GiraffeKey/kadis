use subotai::{
	hash::SubotaiHash,
	node::{Node, StorageEntry},
};

pub fn retrieve(node: &Node, key: &str) -> Option<Vec<u8>> {
	let key = SubotaiHash::sha1(key);
	match node.retrieve(&key) {
		Ok(values) => match values.last() {
			Some(StorageEntry::Value(hash)) => Some(hash.raw.into()),
			Some(StorageEntry::Blob(data)) => Some(data.clone()),
			None => None,
		},
		Err(_) => None,
	}
}

pub fn store(node: &Node, key: &str, value: &Vec<u8>) {
	let key = SubotaiHash::sha1(key);
    match node.store(key, StorageEntry::Blob(value.clone())) {
    	Ok(_) => (),
    	Err(err) => log::error!("{}", err),
    };
}
