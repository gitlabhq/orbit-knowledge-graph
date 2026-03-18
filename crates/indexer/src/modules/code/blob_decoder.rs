use prost::Message;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ResolvedBlob {
    pub oid: String,
    pub data: Vec<u8>,
    pub size: i64,
    pub path: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListBlobsResponse {
    #[prost(message, repeated, tag = "1")]
    blobs: Vec<BlobChunk>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct BlobChunk {
    #[prost(string, tag = "1")]
    oid: String,
    #[prost(int64, tag = "2")]
    size: i64,
    #[prost(bytes = "vec", tag = "3")]
    data: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    path: Vec<u8>,
}

pub struct BlobIterator<'a> {
    data: &'a [u8],
    cursor: usize,
    pending_chunks: std::vec::IntoIter<BlobChunk>,
    current_oid: String,
    current_size: i64,
    current_path: String,
    current_data: Vec<u8>,
}

impl<'a> BlobIterator<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            cursor: 0,
            pending_chunks: Vec::new().into_iter(),
            current_oid: String::new(),
            current_size: 0,
            current_path: String::new(),
            current_data: Vec::new(),
        }
    }

    pub fn next_blob(&mut self) -> Result<Option<ResolvedBlob>, BlobDecodeError> {
        loop {
            let Some(chunk) = self.next_chunk()? else {
                if self.current_oid.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(ResolvedBlob {
                    oid: std::mem::take(&mut self.current_oid),
                    data: std::mem::take(&mut self.current_data),
                    size: self.current_size,
                    path: std::mem::take(&mut self.current_path),
                }));
            };

            if !chunk.oid.is_empty() && !self.current_oid.is_empty() {
                let completed = ResolvedBlob {
                    oid: std::mem::replace(&mut self.current_oid, chunk.oid),
                    data: std::mem::take(&mut self.current_data),
                    size: std::mem::replace(&mut self.current_size, chunk.size),
                    path: std::mem::replace(
                        &mut self.current_path,
                        String::from_utf8_lossy(&chunk.path).into_owned(),
                    ),
                };
                self.current_data.extend_from_slice(&chunk.data);
                return Ok(Some(completed));
            }

            if !chunk.oid.is_empty() {
                self.current_oid = chunk.oid;
                self.current_size = chunk.size;
                self.current_path = String::from_utf8_lossy(&chunk.path).into_owned();
            }
            self.current_data.extend_from_slice(&chunk.data);
        }
    }

    fn next_chunk(&mut self) -> Result<Option<BlobChunk>, BlobDecodeError> {
        if let Some(chunk) = self.pending_chunks.next() {
            return Ok(Some(chunk));
        }

        if self.cursor + 4 > self.data.len() {
            return Ok(None);
        }

        let frame_len =
            u32::from_be_bytes(self.data[self.cursor..self.cursor + 4].try_into().unwrap())
                as usize;
        self.cursor += 4;

        if self.cursor + frame_len > self.data.len() {
            return Err(BlobDecodeError(
                "truncated protobuf frame in blob stream".into(),
            ));
        }

        let frame = &self.data[self.cursor..self.cursor + frame_len];
        self.cursor += frame_len;

        let response = ListBlobsResponse::decode(frame)
            .map_err(|e| BlobDecodeError(format!("failed to decode ListBlobsResponse: {e}")))?;

        self.pending_chunks = response.blobs.into_iter();
        Ok(self.pending_chunks.next())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct BlobDecodeError(String);

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_frame(response: &ListBlobsResponse) -> Vec<u8> {
        let frame = response.encode_to_vec();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        buf.extend_from_slice(&frame);
        buf
    }

    fn collect_blobs(data: &[u8]) -> Result<Vec<ResolvedBlob>, BlobDecodeError> {
        let mut iter = BlobIterator::new(data);
        let mut blobs = Vec::new();
        while let Some(blob) = iter.next_blob()? {
            blobs.push(blob);
        }
        Ok(blobs)
    }

    #[test]
    fn decodes_single_blob_single_frame() {
        let response = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc123".into(),
                size: 5,
                data: b"hello".to_vec(),
                path: b"src/main.rs".to_vec(),
            }],
        };
        let data = encode_frame(&response);

        let blobs = collect_blobs(&data).unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].oid, "abc123");
        assert_eq!(blobs[0].data, b"hello");
        assert_eq!(blobs[0].size, 5);
        assert_eq!(blobs[0].path, "src/main.rs");
    }

    #[test]
    fn decodes_multi_chunk_blob() {
        let chunk1 = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc123".into(),
                size: 10,
                data: b"hello".to_vec(),
                path: b"file.rs".to_vec(),
            }],
        };
        let chunk2 = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: String::new(),
                size: 0,
                data: b"world".to_vec(),
                path: Vec::new(),
            }],
        };
        let mut data = encode_frame(&chunk1);
        data.extend_from_slice(&encode_frame(&chunk2));

        let blobs = collect_blobs(&data).unwrap();
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].oid, "abc123");
        assert_eq!(blobs[0].data, b"helloworld");
        assert_eq!(blobs[0].size, 10);
    }

    #[test]
    fn decodes_multiple_blobs() {
        let response = ListBlobsResponse {
            blobs: vec![
                BlobChunk {
                    oid: "aaa".into(),
                    size: 3,
                    data: b"foo".to_vec(),
                    path: b"a.rs".to_vec(),
                },
                BlobChunk {
                    oid: "bbb".into(),
                    size: 3,
                    data: b"bar".to_vec(),
                    path: b"b.rs".to_vec(),
                },
            ],
        };
        let data = encode_frame(&response);

        let blobs = collect_blobs(&data).unwrap();
        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].oid, "aaa");
        assert_eq!(blobs[0].data, b"foo");
        assert_eq!(blobs[1].oid, "bbb");
        assert_eq!(blobs[1].data, b"bar");
    }

    #[test]
    fn empty_stream_returns_empty() {
        let blobs = collect_blobs(&[]).unwrap();
        assert!(blobs.is_empty());
    }

    #[test]
    fn truncated_frame_returns_error() {
        let response = ListBlobsResponse {
            blobs: vec![BlobChunk {
                oid: "abc".into(),
                size: 3,
                data: b"foo".to_vec(),
                path: Vec::new(),
            }],
        };
        let mut data = encode_frame(&response);
        data.truncate(data.len() - 2);

        let err = collect_blobs(&data).unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }
}
