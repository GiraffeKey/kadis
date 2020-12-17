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

use std::str;

pub fn split_list(list: Vec<u8>) -> Vec<String> {
	str::from_utf8(&list).unwrap().split(",").map(|s| s.into()).collect()
}

#[macro_export]
macro_rules! get_list {
    ( $node:expr, $key:expr, $result:ident, $variant:ident, $error:ident ) => {
        {
            match $node.get(&$key).await {
				Ok(list) => split_list(list),
				Err(err) => return match err {
					GetError::NotFound => $result::$variant(Err($error::KeyNotFound {
						key: $key.into(),
					})),
					GetError::QuorumFailed => $result::$variant(Err($error::KeyQuorumFailed {
						key: $key.into(),
					})),
					GetError::Timeout => $result::$variant(Err($error::KeyTimeout {
						key: $key.into(),
					})),
				},
			}
        }
    };
}

#[macro_export]
macro_rules! get_list_exists {
    ( $node:expr, $key:expr, $result:ident, $variant:ident, $error:ident ) => {
        {
            match $node.get(&$key).await {
				Ok(list) => split_list(list),
				Err(err) => match err {
					GetError::NotFound => Vec::<String>::new(),
					GetError::QuorumFailed => return $result::$variant(Err($error::KeyQuorumFailed {
						key: $key.into(),
					})),
					GetError::Timeout => return $result::$variant(Err($error::KeyTimeout {
						key: $key.into(),
					})),
				},
			}
        }
    };
}

#[macro_export]
macro_rules! join_list {
    ( $node:expr, $key:expr, $list:expr, $result:ident, $variant:ident, $error:ident ) => {
        {
            let list = $list.join(",");
			let list = list.as_bytes().to_vec();

			match $node.put(&$key, list).await {
				Ok(_) => (),
				Err(err) => return match err {
					PutError::QuorumFailed => $result::$variant(Err($error::KeyQuorumFailed {
						key: $key.into(),
					})),
					PutError::Timeout => $result::$variant(Err($error::KeyTimeout {
						key: $key.into(),
					})),
				}
			}
        }
    };
}
