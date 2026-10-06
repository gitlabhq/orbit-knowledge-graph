use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use super::{Bytes, Options, SourceError, add_capped};

#[derive(Clone)]
pub(super) enum Blob {
    Memory(Bytes),
    Spilled { offset: u64, len: u64, raw_len: u64 },
}

pub(super) struct Scratch {
    dir: Option<PathBuf>,
    compress: bool,
    cap: Option<u64>,
    file: Option<std::fs::File>,
    end: u64,
}

impl Scratch {
    pub(super) fn new(options: &Options, cap: Option<u64>) -> Self {
        Self {
            dir: options.scratch_dir.clone(),
            compress: options.compress_spill,
            cap,
            file: None,
            end: 0,
        }
    }

    pub(super) fn len(&self) -> u64 {
        self.end
    }

    pub(super) fn append(&mut self, bytes: &[u8]) -> Result<Blob, SourceError> {
        let raw_len = bytes.len() as u64;
        let compressed = self.compress.then(|| lz4_flex::block::compress(bytes));
        let bytes = compressed
            .as_deref()
            .filter(|data| data.len() < bytes.len())
            .unwrap_or(bytes);
        let len = bytes.len() as u64;
        let offset = add_capped(&mut self.end, "spilled_bytes", len, self.cap)?;
        if self.file.is_none() {
            self.file = Some(match &self.dir {
                Some(dir) => tempfile::tempfile_in(dir)?,
                None => tempfile::tempfile()?,
            });
        }
        self.file.as_ref().unwrap().write_all_at(bytes, offset)?;
        Ok(Blob::Spilled {
            offset,
            len,
            raw_len,
        })
    }

    pub(super) fn read(&self, blob: &Blob) -> io::Result<Bytes> {
        let (offset, len, raw_len) = match blob {
            Blob::Memory(bytes) => return Ok(bytes.clone()),
            Blob::Spilled {
                offset,
                len,
                raw_len,
            } => (*offset, *len, *raw_len),
        };
        let mut bytes = vec![0; len as usize];
        self.file
            .as_ref()
            .unwrap()
            .read_exact_at(&mut bytes, offset)?;
        if len < raw_len {
            bytes = lz4_flex::block::decompress(&bytes, raw_len as usize)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        }
        Ok(bytes.into())
    }
}
