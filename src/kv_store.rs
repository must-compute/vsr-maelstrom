use std::{collections::HashMap, hash::Hash};

use super::message::ErrorCode;

#[derive(Default, Clone, Debug)]
pub(super) struct KeyValueStore<K, V>
where
    K: Hash + Eq + Send,
    V: PartialEq + Send,
{
    map: HashMap<K, V>,
}

impl<K, V> KeyValueStore<K, V>
where
    K: Hash + Eq + Send,
    V: PartialEq + Send,
{
    pub fn read(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn write(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    pub fn cas(&mut self, key: K, from: V, to: V) -> anyhow::Result<()> {
        let res = self.map.get_mut(&key);

        match res {
            Some(current) => {
                if *current != from {
                    return Err(anyhow::Error::new(ErrorCode::PreconditionFailed));
                }
                *current = to;
                Ok(())
            }
            None => Err(anyhow::Error::new(ErrorCode::KeyDoesNotExist)),
        }
    }
}
