//! TODO I saw this module in the go implementation, but when testing it doesn't seem to be used.
//! Maybe this is only needed when preserve uid (gid) is enabled?

use eyre::Result;
use tokio::io::AsyncReadExt;
use tracing::info;

use crate::EnvelopedConn;

impl<'a> EnvelopedConn<'a> {
    /// Consume id mapping. We don't need this info here.
    pub async fn consume_uid_mapping(&mut self) -> Result<()> {
        self.consume_uid_mapping_().await?; // uid
        self.consume_uid_mapping_().await?; // gid
        Ok(())
    }
    async fn consume_uid_mapping_(&mut self) -> Result<()> {
        info!("start consume uid");
        loop {
            let b = self.rx.read_u32_le().await?;
            info!(b);
            if b == 0 {
                break;
            }
            let len = self.rx.read_u8().await?;
            self.rx.read_exact(&mut vec![0; len as usize]).await?;
        }
        Ok(())
    }
}
