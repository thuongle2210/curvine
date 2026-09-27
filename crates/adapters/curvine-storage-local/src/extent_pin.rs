use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtentKey {
    pub block_id: i64,
    pub bdev_offset: i64,
    pub size: i64,
}

#[derive(Debug, Default)]
pub struct ExtentPinRegistry {
    pins: Mutex<HashMap<ExtentKey, usize>>,
    drain_tx: Mutex<Option<mpsc::Sender<ExtentKey>>>,
}

impl ExtentPinRegistry {
    pub fn set_drain_sender(&self, tx: mpsc::Sender<ExtentKey>) {
        *self
            .drain_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(tx);
    }

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
            drop(pins);
            if let Some(tx) = self
                .drain_tx
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
            {
                let _ = tx.send(key);
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_event_emits_after_last_pin_drops() {
        let registry = Arc::new(ExtentPinRegistry::default());
        let (tx, rx) = mpsc::channel();
        registry.set_drain_sender(tx);
        let key = ExtentKey {
            block_id: 1,
            bdev_offset: 0,
            size: 4096,
        };

        let pin1 = registry.pin(key);
        let pin2 = registry.pin(key);
        drop(pin1);
        assert!(rx.try_recv().is_err());
        assert!(registry.is_pinned(key));

        drop(pin2);
        assert_eq!(rx.try_recv(), Ok(key));
        assert!(!registry.is_pinned(key));
    }
}
