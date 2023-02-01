use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::time::{SystemTime, UNIX_EPOCH};

use eyre::{bail, Result};
use tokio::io::AsyncReadExt;
use tracing::debug;

use crate::envelope::RsyncReadExt;
use crate::EnvelopedConn;

const XMIT_TOP_DIR: u8 = 1 << 0;
const XMIT_SAME_MODE: u8 = 1 << 1;
const XMIT_EXTENDED_FLAGS: u8 = 1 << 2;
const XMIT_SAME_RDEV_PRE28: u8 = XMIT_EXTENDED_FLAGS; /* Only in protocols < 28 */
const XMIT_SAME_UID: u8 = 1 << 3;
const XMIT_SAME_GID: u8 = 1 << 4;
const XMIT_SAME_NAME: u8 = 1 << 5;
const XMIT_LONG_NAME: u8 = 1 << 6;
const XMIT_SAME_TIME: u8 = 1 << 7;

#[derive(Clone)]
pub struct FileEntry {
    // maybe PathBuf?
    pub name: Vec<u8>,
    pub len: u64,
    pub modify_time: SystemTime,
    pub mode: u32,
    // maybe PathBuf?
    pub link_target: Option<Vec<u8>>,
    pub idx: i32,
}

impl FileEntry {
    pub fn name_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.name)
    }
}

impl Debug for FileEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileEntry")
            .field("name", &self.name_lossy())
            .field("len", &self.len)
            .field("modify_time", &self.modify_time)
            .field("mode", &self.mode)
            .field(
                "link_target",
                &(self
                    .link_target
                    .as_ref()
                    .map(|s| String::from_utf8_lossy(&s))),
            )
            .field("idx", &self.idx)
            .finish()
    }
}

impl<'a> EnvelopedConn<'a> {
    pub async fn recv_file_list(&mut self) -> Result<Vec<FileEntry>> {
        let mut list = vec![];

        let mut name_scratch = Vec::new();
        loop {
            let b = self.rx.read_u8().await?;
            if b == 0 {
                break;
            }

            let entry = self
                .recv_file_entry(b, &mut name_scratch, list.last())
                .await?;
            debug!(?entry, "recv file entry");
            list.push(entry);
        }

        list.sort_unstable_by(|x, y| x.name.cmp(&y.name));
        list.dedup_by(|x, y| x.name == y.name);

        // Now we mark their idx
        for (idx, entry) in list.iter_mut().enumerate() {
            entry.idx = i32::try_from(idx).expect("file list too long");
        }

        Ok(list)
    }

    async fn recv_file_entry(
        &mut self,
        flags: u8,
        name_scratch: &mut Vec<u8>,
        prev: Option<&FileEntry>,
    ) -> Result<FileEntry> {
        let same_name = flags & XMIT_SAME_NAME != 0;
        let long_name = flags & XMIT_LONG_NAME != 0;
        let same_time = flags & XMIT_SAME_TIME != 0;
        let same_mode = flags & XMIT_SAME_MODE != 0;

        let inherit_name_len = if same_name {
            self.rx.read_u8().await?
        } else {
            0
        };
        let name_len = if long_name {
            self.rx.read_u32_le().await?
        } else {
            self.rx.read_u8().await? as u32
        };

        const PATH_MAX: u32 = 4096;
        if name_len > PATH_MAX - inherit_name_len as u32 {
            bail!("path too long");
        }

        assert!(
            inherit_name_len as usize <= name_scratch.len(),
            "file list inconsistency"
        );
        name_scratch.resize(inherit_name_len as usize + name_len as usize, 0);
        self.rx
            .read_exact(&mut name_scratch[inherit_name_len as usize..])
            .await?;
        // TODO this only works on unix
        // TODO: does rsync’s clean_fname() and sanitize_path() combination do
        // anything more than Go’s filepath.Clean()?
        let name = name_scratch.clone();

        let len = self.rx.read_rsync_long().await? as u64;

        let modify_time = if same_time {
            prev.expect("prev must exist").modify_time
        } else {
            // To avoid Y2038 problem, newer versions of rsync daemon treat mtime as u32 when
            // speaking protocol version 27.
            let secs = self.rx.read_u32_le().await?;
            SystemTime::UNIX_EPOCH + std::time::Duration::new(secs as u64, 0)
        };

        let mode = if same_mode {
            prev.expect("prev must exist").mode
        } else {
            self.rx.read_u32_le().await?
        };

        let is_link = unix_mode::is_symlink(mode);

        // Preserve uid is disabled
        // Preserve gid is disabled
        // Preserve dev and special are disabled

        // Preserve links
        let link_target = if is_link {
            let len = self.rx.read_u32_le().await?;
            let mut buf = vec![0u8; len as usize];
            self.rx.read_exact(&mut buf).await?;
            // TODO this only works on unix
            Some(buf)
        } else {
            None
        };

        Ok(FileEntry {
            name,
            len,
            modify_time,
            mode,
            link_target,
            idx: i32::MAX, // to be filled later
        })
    }
}

pub fn mod_time_eq(x: SystemTime, y: SystemTime) -> bool {
    x.duration_since(UNIX_EPOCH)
        .expect("time before unix epoch")
        .as_secs()
        == y.duration_since(UNIX_EPOCH)
            .expect("time before unix epoch")
            .as_secs()
}
