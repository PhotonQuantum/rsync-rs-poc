use std::path::PathBuf;

use crate::filter::Rule;

pub struct Opts {
    pub dest: PathBuf,
    pub filters: Vec<Rule>,
}
