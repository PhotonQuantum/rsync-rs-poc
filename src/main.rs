use std::path::PathBuf;

use eyre::{bail, eyre, Context, Result};
use scan_fmt::scan_fmt;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tracing::{debug, info, instrument, warn};
use url::Url;

use crate::envelope::{EnvelopeRead, RsyncReadExt};
use crate::generator::Generator;
use crate::opts::Opts;
use crate::recv::Receiver;

mod chksum;
mod envelope;
mod file_list;
mod generator;
mod opts;
mod recv;
mod uid_list;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Hello, world!");
    tracing_subscriber::fmt::init();

    let opts = Opts {
        dest: PathBuf::from("./dest"),
    };

    let url = Url::parse("rsync://127.0.0.1/pysjtu/")?;
    // let url = Url::parse("rsync://mirrors.kernel.org/debian-cd/")?;
    // let url = Url::parse("rsync://rsync.deepin.com/deepin/")?;
    start_socket_client(url, &opts).await?;

    Ok(())
}

async fn start_socket_client(url: Url, opts: &Opts) -> Result<()> {
    let port = url.port().unwrap_or(873);
    let path = url.path().trim_start_matches('/');
    let module = path.split('/').next().unwrap_or("must have module");

    let mut stream = TcpStream::connect(format!("{}:{}", url.host_str().expect("has host"), port))
        .await
        .expect("connect success");

    let mut conn = Conn::new(&mut stream);
    conn.start_inband_exchange(module, path).await?;

    let (seed, mut enveloped_conn) = conn.handshake_done().await?;
    let file_list = enveloped_conn.recv_file_list().await?;
    info!(files = file_list.len(), "file list");

    // enveloped_conn.consume_uid_mapping().await?;
    let io_errors = enveloped_conn.rx.read_i32_le().await?;
    if io_errors != 0 {
        warn!("server reported IO errors: {}", io_errors);
    }

    let mut generator = Generator(enveloped_conn.tx);
    let mut receiver = Receiver(enveloped_conn.rx);
    // Do not receiver on generator error?
    tokio::try_join!(
        generator.generate_task(seed, opts, &file_list),
        receiver.recv_task(seed, opts, &file_list),
    )?;

    let Generator(mut tx) = generator;
    let Receiver(mut rx) = receiver;

    let read = rx.read_rsync_long().await?;
    let written = rx.read_rsync_long().await?;
    let size = rx.read_rsync_long().await?;

    info!(read, written, size, "transfer stats");

    tx.write_i32_le(-1).await?;
    tx.shutdown().await?;

    Ok(())
}

#[derive(Debug)]
struct EnvelopedConn<'a> {
    tx: WriteHalf<'a>,
    rx: EnvelopeRead<BufReader<ReadHalf<'a>>>,
}

#[derive(Debug)]
struct Conn<'a> {
    tx: WriteHalf<'a>,
    rx: BufReader<ReadHalf<'a>>,
}

impl<'a> Conn<'a> {
    fn new(stream: &'a mut TcpStream) -> Self {
        let (rx, tx) = stream.split();
        Self {
            tx,
            rx: BufReader::with_capacity(256 * 1024, rx),
        }
    }

    // TODO all readlines in this module are not safe (no length limit). Can use .take(?).
    #[instrument(skip(self))]
    async fn start_inband_exchange(&mut self, module: &str, path: &str) -> Result<()> {
        info!("start inband exchange");

        self.tx.write_all(b"@RSYNCD: 27.0\n").await?;

        let mut greeting = String::new();
        self.rx.read_line(&mut greeting).await?;
        info!(greeting, "greeting");

        let protocol_header = greeting
            .trim()
            .strip_prefix("@RSYNCD: ")
            .ok_or_else(|| eyre!("invalid greeting"))?
            .to_string();

        let remote_protocol = scan_fmt!(&protocol_header, "{}.{}", i32, i32)
            .map(|(protocol, _sub)| protocol)
            .or_else(|_| scan_fmt!(&protocol_header, "{}", i32))
            .context("invalid greeting: no server version")?;

        if remote_protocol < 27 {
            bail!("Server version too old: {}", remote_protocol);
        }

        info!(remote_protocol, local_protocol = 27, "Client Protocol");
        self.tx
            .write_all(format!("{}\n", module).as_bytes())
            .await?;

        // MOTD
        loop {
            let mut line = String::new();
            self.rx.read_line(&mut line).await?;

            if line.starts_with("@ERROR") {
                bail!("server error: {}", line);
            } else if line.starts_with("@RSYNCD: AUTHREQD ") {
                bail!("server requires authentication");
            } else if line.starts_with("@RSYNCD: OK") {
                break;
            } else {
                println!("{}", line.trim_end());
            }
        }

        // TODO daemon args, hardcoded for now. Need to modify file_list parse code if changed.
        // TODO preserve_hard_link is commented out in go rsync, why?
        // -l preserve_links -t preserve_times -r recursive -p perms
        const SERVER_OPTIONS: [&str; 4] = ["--server", "--sender", "-ltpr", "."];
        for opt in SERVER_OPTIONS {
            debug!(opt, "server option");
            self.tx.write_all(format!("{}\n", opt).as_bytes()).await?;
        }
        if path != "" {
            debug!(path, "server option");
            self.tx.write_all(format!("{}\n", path).as_bytes()).await?;
        }
        self.tx.write_all(b"\n").await?;
        debug!("options done");

        Ok(())
    }

    #[instrument(skip(self))]
    async fn handshake_done(mut self) -> Result<(i32, EnvelopedConn<'a>)> {
        let seed = self.rx.read_i32_le().await?;
        debug!(seed);

        // TODO exclusion list
        self.tx.write_i32_le(0).await?;

        Ok((
            seed,
            EnvelopedConn {
                tx: self.tx,
                rx: EnvelopeRead::new(self.rx),
            },
        ))
    }
}
