//! MiniDfs Cluster
//!
//! MiniDFS provides a embedded HDFS cluster. It is usually for testing.
//!
//! ## Example
//!
//! ```ignore
//!  let mut conf = new_mini_dfs_conf();
//!  let dfs = MiniDFS::start(&mut conf).unwrap();
//!  let port = dfs.namenode_port();
//!  ...
//!  dfs.stop()
//! ```

use libc::{c_char, c_int};
use std::ffi;
use std::mem;
use std::str;

use crate::native::*;

pub struct MiniDFS {
    cluster: *mut MiniDfsCluster,
}

impl MiniDFS {
    pub fn start(conf: &MiniDfsConf) -> Option<MiniDFS> {
        match unsafe { nmdCreate(conf) } {
            val if !val.is_null() => Some(MiniDFS { cluster: val }),
            _ => None,
        }
    }

    pub fn stop(&self) {
        unsafe {
            nmdShutdownClean(self.cluster);
            nmdFree(self.cluster);
        }
    }

    pub fn wait_for_clusterup(&self) -> bool {
        if unsafe { nmdWaitClusterUp(self.cluster) } == 0 {
            true
        } else {
            false
        }
    }

    pub fn namenode_port(&self) -> Option<i32> {
        match unsafe { nmdGetNameNodePort(self.cluster) as i32 } {
            val if val > 0 => Some(val),
            _ => None,
        }
    }

    pub fn namenode_http_addr(&self) -> Option<(&str, i32)> {
        let mut hostname: *const c_char = unsafe { mem::zeroed() };
        let mut port: c_int = 0;

        match unsafe { nmdGetNameNodeHttpAddress(self.cluster, &mut port, &mut hostname) }
        {
            0 => {
                let slice = unsafe { ffi::CStr::from_ptr(hostname) }.to_bytes();
                let str = str::from_utf8(slice).unwrap();

                Some((str, port as i32))
            }
            _ => None,
        }
    }

    pub fn set_hdfs_builder(&self, builder: *mut hdfsBuilder) -> bool {
        (unsafe { nmdConfigureHdfsBuilder(self.cluster, builder) } == 0)
    }
}

pub fn new_mini_dfs_conf() -> MiniDfsConf {
    MiniDfsConf {
        doFormat: 1,
        webhdfsEnabled: 0,
        namenodeHttpPort: 0,
        configureShortCircuit: 0,
        numDataNodes: 0,
    }
}
