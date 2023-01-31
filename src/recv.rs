use std::ffi::OsStr;
use std::io::SeekFrom;
use std::ops::{Deref, DerefMut};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use eyre::{ensure, Result};
use filetime::FileTime;
use md4::{Digest, Md4};
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::ReadHalf;
use tracing::info;

use crate::chksum::SumHead;
use crate::envelope::EnvelopeRead;
use crate::file_list::{mod_time_eq, FileEntry};
use crate::opts::Opts;

pub struct Receiver<'a>(pub EnvelopeRead<BufReader<ReadHalf<'a>>>);

impl<'a> Deref for Receiver<'a> {
    type Target = EnvelopeRead<BufReader<ReadHalf<'a>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for Receiver<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> Receiver<'a> {
    pub async fn recv_task(
        &mut self,
        seed: i32,
        opts: &Opts,
        file_list: &[FileEntry],
    ) -> Result<()> {
        let mut phase = 0;
        loop {
            let idx = self.read_i32_le().await?;

            if idx == -1 {
                if phase == 0 {
                    phase += 1;
                    info!("recv file phase {}", phase);
                    continue;
                }
                break;
            }

            let entry = &file_list[idx as usize];
            info!("recv file #{} ({})", idx, entry.name_lossy());
            // TODO unix only
            // TODO s3 impl download file from storage in this step.
            let basis_path = opts.dest.join(Path::new(OsStr::from_bytes(&entry.name)));
            let basis_file = File::open(&basis_path)
                .await
                .map(|f| Some(f))
                .or_else(|f| {
                    if f.kind() == std::io::ErrorKind::NotFound {
                        Ok(None)
                    } else {
                        Err(f)
                    }
                })?;

            let mut target_file = BufReader::new(self.recv_data(seed, basis_file).await?);

            // TODO s3 impl upload file to storage in this step.
            let mut dest = BufWriter::new(File::create(&basis_path).await?);
            tokio::io::copy(&mut target_file, &mut dest).await?;
            let old_mod_time = tokio::fs::metadata(&basis_path).await?.modified()?;
            if !mod_time_eq(old_mod_time, entry.modify_time) {
                filetime::set_file_mtime(&basis_path, FileTime::from_system_time(entry.modify_time))
                    .expect("set mod time")
            }
        }

        info!("recv finish");
        Ok(())
    }

    async fn recv_data(&mut self, seed: i32, mut local_basis: Option<File>) -> Result<File> {
        let SumHead {
            checksum_count,
            block_len,
            checksum_len: _,
            remainder_len,
        } = SumHead::read_from(&mut self.0).await?;

        // TODO security fix: this file should not be accessible to other users.
        let mut target_file = File::from_std(tempfile()?);

        // Hasher for final file consistency check.
        let mut hasher = Md4::default();
        hasher.update(&seed.to_le_bytes());

        let (mut transferred, mut copied) = (0u64, 0u64);
        loop {
            let token = self.recv_token().await?;
            match token {
                FileToken::Data(data) => {
                    transferred += data.len() as u64;
                    hasher.update(&data);
                    target_file.write_all(&data).await?;
                }
                FileToken::Copied(block_offset) => {
                    let offset = block_offset as u64 * block_len as u64;
                    let data_len =
                        if block_offset == checksum_count as u32 - 1 && remainder_len != 0 {
                            remainder_len
                        } else {
                            block_len
                        };
                    copied += data_len as u64;

                    let mut buf = vec![0; data_len as usize];
                    let local_basis = local_basis.as_mut().expect("incremental");
                    local_basis.seek(SeekFrom::Start(offset as u64)).await?;
                    local_basis.read_exact(&mut buf).await?;

                    hasher.update(&buf);
                    target_file.write_all(&buf).await?;
                }
                FileToken::Done => break,
            }
        }

        let local_checksum = hasher.finalize();
        let mut remote_checksum = vec![0; local_checksum.len()];

        self.read_exact(&mut remote_checksum).await?;
        ensure!(&*local_checksum == remote_checksum, "checksum mismatch");

        info!(
            ratio = transferred as f64 / (transferred + copied) as f64,
            "transfer ratio"
        );

        // No need to set perms because we'll upload it to s3.

        target_file.seek(SeekFrom::Start(0)).await?;
        Ok(target_file)
    }

    async fn recv_token(&mut self) -> Result<FileToken> {
        let token = self.read_i32_le().await?;
        if token == 0 {
            Ok(FileToken::Done)
        } else if token > 0 {
            let mut buf = vec![0; token as usize];
            self.read_exact(&mut buf).await?;
            Ok(FileToken::Data(buf))
        } else {
            Ok(FileToken::Copied(-(token + 1) as u32))
        }
    }
}

enum FileToken {
    Data(Vec<u8>),
    Copied(u32),
    Done,
}
