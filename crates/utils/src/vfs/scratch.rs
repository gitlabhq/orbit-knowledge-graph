//! Where bytes live when they are not in memory: one anonymous append-only
//! file, opened on the first spill, positional writes from any thread,
//! positional reads, gone when the store is. Each blob is one independent
//! LZ4 block when compression reduces its size, so reads stay random-access.

use std::io;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;

use super::limits::{LimitKind, add_capped};
use super::loading::Content;
use super::{Bytes, Options, SourceError};

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

    pub(super) fn append(&self, bytes: &[u8]) -> Result<Content, SourceError> {
        let raw_len = bytes.len() as u64;
        let compressed;
        let bytes = match self.compress {
            true => {
                compressed = lz4_flex::block::compress(bytes);
                if compressed.len() < bytes.len() {
                    compressed.as_slice()
                } else {
                    bytes
                }
            }
            false => bytes,
        };
        let len = bytes.len() as u64;
        let offset = add_capped(&self.end, LimitKind::SpilledBytes, len, self.cap)?;
        write_all_at(self.file()?, bytes, offset)?;
        Ok(Content::Spilled {
            offset,
            len,
            raw_len,
        })
    }

    pub(super) fn read(&self, offset: u64, len: u64, raw_len: u64) -> io::Result<Bytes> {
        let mut bytes = vec![0u8; len as usize];
        read_exact_at(self.file()?, &mut bytes, offset)?;
        if len < raw_len {
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

#[cfg(unix)]
fn write_all_at(file: &std::fs::File, bytes: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, bytes, offset)
}

#[cfg(unix)]
fn read_exact_at(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, bytes, offset)
}

// Windows seek_read/seek_write take an explicit offset but may transfer
// fewer bytes than asked, so loop like the unix `*_exact_at` helpers.
#[cfg(windows)]
fn write_all_at(file: &std::fs::File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !bytes.is_empty() {
        match file.seek_write(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                bytes = &bytes[written..];
                offset += written as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &std::fs::File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !bytes.is_empty() {
        match file.seek_read(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(read) => {
                bytes = &mut bytes[read..];
                offset += read as u64;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
