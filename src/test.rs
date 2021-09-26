// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::hdfs::HdfsFs;
use crate::minidfs::{new_mini_dfs_conf, MiniDFS};
use std::panic;

fn hdfs_setup() -> MiniDFS {
    let mut conf = new_mini_dfs_conf();
    MiniDFS::start(&mut conf).unwrap()
}
fn hdfs_teardown(dfs: MiniDFS) {
    dfs.stop();
}

pub fn run_hdfs_test<T>(test: T) -> ()
where
    T: FnOnce(&MiniDFS) -> () + panic::UnwindSafe,
{
    let dfs = hdfs_setup();

    let result = panic::catch_unwind(|| test(&dfs));

    hdfs_teardown(dfs);

    assert!(result.is_ok())
}

pub fn get_hdfs(dfs: &MiniDFS) -> HdfsFs {
    let port = dfs.namenode_port().unwrap();
    let minidfs_addr = format!("hdfs://localhost:{}", port);
    HdfsFs::new(minidfs_addr.as_str()).ok().unwrap()
}
