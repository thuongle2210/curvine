use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentKey {
    pub block_id: i64,
    pub bdev_offset: i64,
    pub size: i64,
}

#[derive(Debug, Default)]
pub struct ExtentPinRegistry {
    pins: Mutex<HashMap<ExtentKey, usize>>,
}

impl ExtentPinRegistry {
    pub fn pin(self: &Arc<Self>, key: ExtentKey) -> ExtentPinGuard {
        let mut pins = self
            .pins
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *pins.entry(key).or_insert(0) += 1;
        ExtentPinGuard {
            registry: self.clone(),
            key,
        }
    }

    pub fn is_pinned(&self, key: ExtentKey) -> bool {
        self.pins
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
            .is_some_and(|count| *count > 0)
    }

    fn unpin(&self, key: ExtentKey) {
        let mut pins = self
            .pins
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(count) = pins.get_mut(&key) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            pins.remove(&key);
        }
    }
}

#[derive(Debug)]
pub struct ExtentPinGuard {
    registry: Arc<ExtentPinRegistry>,
    key: ExtentKey,
}

impl Drop for ExtentPinGuard {
    fn drop(&mut self) {
        self.registry.unpin(self.key);
    }
}
