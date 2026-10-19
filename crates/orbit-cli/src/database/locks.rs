use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Result;

pub fn lock_database(path: &Path) -> Result<File> {
    acquire_database_lock(path, false)
}

pub fn read_lock_database(path: &Path) -> Result<Option<File>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let lock = acquire_database_lock(path, true)?;
    Ok(path.try_exists()?.then_some(lock))
}

fn acquire_database_lock(path: &Path, shared: bool) -> Result<File> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut lock_path = path.as_os_str().to_owned();
    lock_path.push(".lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock_path))?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let result = if shared {
            file.try_lock_shared()
        } else {
            file.try_lock()
        };
        match result {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => {
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "local database {} is busy; retry when its readers or writer finish",
                    path.display()
                );
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
    }
}
