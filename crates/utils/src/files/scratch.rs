//! Where bytes live when they are not in memory: one anonymous append-only
//! file, opened on the first spill, positional writes from any thread,
//! positional reads, gone when the store is. Each blob is one independent
//! LZ4 block when asked, so reads stay random-access.

use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;

use super::limits::add_capped;
use super::{Bytes, Options, SourceError};

#[derive(Debug, Clone)]
pub(super) enum Blob {
    Memory(Bytes),
    Spilled { offset: u64, len: u64, raw_len: u64 },
}

pub(super) struct Scratch {
    dir: Option<PathBuf>,
    compress: bool,
    cap: Option<u64>,
    file: OnceLock<std::fs::File>,
    pub(super) end: AtomicU64,
}

impl Scratch {
    pub(super) fn new(options: &Options, cap: Option<u64>) -> Self {
        Self {
            dir: options.scratch_dir.clone(),
            compress: options.compress_spill,
            cap,
            file: OnceLock::new(),
            end: AtomicU64::new(0),
        }
    }

    pub(super) fn append(&self, bytes: &[u8]) -> Result<Blob, SourceError> {
        let raw_len = bytes.len() as u64;
        let compressed;
        let bytes = match self.compress {
            true => {
                compressed = lz4_flex::block::compress(bytes);
                compressed.as_slice()
            }
            false => bytes,
        };
        let len = bytes.len() as u64;
        let offset = add_capped(&self.end, "spilled_bytes", len, self.cap)?;
        self.file()?.write_all_at(bytes, offset)?;
        Ok(Blob::Spilled {
            offset,
            len,
            raw_len,
        })
    }

    pub(super) fn read(&self, offset: u64, len: u64, raw_len: u64) -> io::Result<Bytes> {
        let mut bytes = vec![0u8; len as usize];
        self.file()?.read_exact_at(&mut bytes, offset)?;
        if self.compress {
            bytes = lz4_flex::block::decompress(&bytes, raw_len as usize)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }
        Ok(bytes.into())
    }

    fn file(&self) -> io::Result<&std::fs::File> {
        if let Some(file) = self.file.get() {
            return Ok(file);
        }
        let file = match &self.dir {
            Some(dir) => tempfile::tempfile_in(dir)?,
            None => tempfile::tempfile()?,
        };
        let _ = self.file.set(file);
        Ok(self.file.get().expect("scratch file was just set"))
    }
}
