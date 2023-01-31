# Rsync wire protocol in Rust

This is a quick poc of the rsync wire protocol in Rust. It supports delta transfer.

The code is really a mess right now, and is only a poc to see if it is possible to implement the protocol in Rust.

The ultimate goal is to implement a solution to use s3 as a transfer target for rsync, with its use case tailored for
mirror sites.

This repo won't be maintained, and is only here for reference.
Further work will be done in a new crate.

The code is based on [arrsync (Rust)](https://github.com/jcaesar/ftp2mfs/tree/master/crates/arrsync) and
[gokrazy/rsync (Go)](https://github.com/gokrazy/rsync).

The supported wire protocol version is 27.