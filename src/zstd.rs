//! zstd compression formats.
//!
//!
//! # Examples
//!
//! ```
//! # #[cfg(feature = "zstd")] {
//! use std::{env, fs::File, io::Write};
//!
//! use gzp::{zstd::Zstd, par::compress::{ParCompress, ParCompressBuilder}, ZWriter};
//!
//! let mut writer = vec![];
//! let mut parz: ParCompress<Zstd,_> = ParCompressBuilder::new().from_writer(writer);
//! parz.write_all(b"This is a first test line\n").unwrap();
//! parz.write_all(b"This is a second test line\n").unwrap();
//! parz.finish().unwrap();
//! # }
//! ```

use std::{convert::TryInto, io::Write};

use bytes::Bytes;
use flate2::Compression;
use zstd::Encoder;

use crate::{check::PassThroughCheck, syncz::SyncZ, FormatSpec, GzpError, SyncWriter, ZWriter};

/// Zstd format.
#[derive(Copy, Clone, Debug)]
pub struct Zstd {}

impl FormatSpec for Zstd {
    type C = PassThroughCheck;
    type Compressor = zstd::bulk::Compressor<'static>;

    fn new() -> Self {
        Self {}
    }

    #[inline]
    fn create_compressor(
        &self,
        compression_level: Compression,
    ) -> Result<Self::Compressor, GzpError> {
        Ok(Self::Compressor::new(
            compression_level.level().try_into().unwrap(),
        )?)
    }

    #[inline]
    fn needs_dict(&self) -> bool {
        false
    }

    #[inline]
    #[allow(unused)]
    fn encode(
        &self,
        input: &[u8],
        encoder: &mut Self::Compressor,
        compression_level: Compression,
        dict: Option<&Bytes>,
        is_last: bool,
    ) -> Result<Vec<u8>, GzpError> {
        let compression_level = compression_level.level().try_into().unwrap();
        if let Some(dict) = dict {
            encoder.set_dictionary(compression_level, dict)?;
        } else {
            encoder.set_compression_level(compression_level)?;
        }
        Ok(encoder.compress(input)?)
    }

    fn header(&self, _compression_level: Compression) -> Vec<u8> {
        vec![]
    }

    fn footer(&self, _check: &Self::C) -> Vec<u8> {
        vec![]
    }
}

impl<W> SyncWriter<W> for Zstd
where
    W: Write,
{
    type OutputWriter = Encoder<'static, W>;

    fn sync_writer(writer: W, compression_level: Compression) -> Encoder<'static, W> {
        Encoder::new(writer, compression_level.level().try_into().unwrap()).unwrap()
    }
}

impl<W: Write> ZWriter<W> for SyncZ<Encoder<'static, W>> {
    fn finish(&mut self) -> Result<W, GzpError> {
        Ok(self.inner.take().unwrap().finish()?)
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Write};
    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    use tempfile::tempdir;
    use zstd::Decoder;

    use crate::par::compress::{ParCompress, ParCompressBuilder};
    use crate::ZWriter;

    use super::*;

    #[test]
    fn test_simple() {
        let dir = tempdir().unwrap();

        // Create output file
        let output_file = dir.path().join("output.txt");
        let out_writer = BufWriter::new(File::create(&output_file).unwrap());

        // Define input bytes
        let input = b"
        This is a longer test than normal to come up with a bunch of text.
        We'll read just a few lines at a time.
        ";

        // Compress input to output
        let mut par_zstd: ParCompress<Zstd, _> = ParCompressBuilder::new().from_writer(out_writer);
        par_zstd.write_all(input).unwrap();
        par_zstd.finish().unwrap();

        // Read output back in
        let mut reader = BufReader::new(File::open(output_file).unwrap());
        let mut result = vec![];
        reader.read_to_end(&mut result).unwrap();

        // Decompress it
        let mut gz = Decoder::new(&result[..]).unwrap();
        let mut bytes = vec![];
        gz.read_to_end(&mut bytes).unwrap();

        // Assert decompressed output is equal to input
        assert_eq!(input.to_vec(), bytes);
    }
}
