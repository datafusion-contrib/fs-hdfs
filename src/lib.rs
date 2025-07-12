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

//! fs-hdfs3 is a library for accessing to HDFS cluster.
//! Basically, it provides libhdfs FFI APIs.
//! It also provides more idiomatic and abstract Rust APIs,
//! hiding manual memory management and some thread-safety problem of libhdfs.
//! Rust APIs are highly recommended for most users.
//!
//! ## Important Note
//! The original ``libhdfs`` implementation allows only one ``HdfsFs`` instance for the
//! same namenode because ``libhdfs`` only keeps a single ``hdfsFs`` entry for each namenode.
//! As a result, a global singleton ``HdfsManager`` is introduced to control only one single ``hdfsFs`` entry created for each namenode.
//! Contrast, ``HdfsFs`` instance itself is thread-safe.
//!
//! This library mainly provides two public methods to load and unload ``HdfsFs``
//! - pub fn get_hdfs_by_full_path(path: &str) -> Result<Arc<HdfsFs>, HdfsErr>
//! - pub fn unload_hdfs_cache_by_full_path(path: &str) -> Result<Option<Arc<HdfsFs>>, HdfsErr>
//!
//! ## Usage
//! in Cargo.toml:
//!
//! ```ignore
//! [dependencies]
//! fs-hdfs3 = "0.1.12"
//! ```
//! or
//!
//! ```ignore
//! [dependencies.fs-hdfs3]
//! git = "https://github.com/datafusion-contrib/fs-hdfs"
//! ```
//!
//! Firstly, we need to add library path for the jvm related dependencies.
//! An example for MacOS,
//!
//! ```bash ignore
//! export DYLD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server
//! ```
//!
//! Here, ``$JAVA_HOME`` need to be specified and exported.
//!
//! Since our compiled libhdfs is JNI native implementation, it requires the proper ``CLASSPATH``.
//! An example,
//!
//! ```bash ignore
//! export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`
//! ```
//!
//! ## Testing
//! The test also requires the ``CLASSPATH``. In case that the java class of ``org.junit.Assert``
//! can't be found. Refine the ``$CLASSPATH`` as follows:
//!
//! ```bash ignore
//! export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`:$HADOOP_HOME/share/hadoop/tools/lib/*
//! ```
//!
//! Here, ``$HADOOP_HOME`` need to be specified and exported.
//!
//! Then you can run
//!
//! ```ignore
//! cargo test
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use hdfs::hdfs::{get_hdfs_by_full_path, unload_hdfs_cache, HdfsFs};
//!
//! let fs: Arc<HdfsFs> = get_hdfs_by_full_path("hdfs://localhost:8020/").ok().unwrap();
//! match fs.mkdir("/data") {
//!   Ok(_) => { println!("/data has been created") },
//!   Err(_)  => { panic!("/data creation has failed") }
//! };
//!
//! match unload_hdfs_cache(fs) {
//!   Ok(Some(_)) => { println!("Unloading hdfs cache succeeded") },
//!   _  => { panic!("Unloading hdfs cache failed") }
//! }
//! ```

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

#[allow(deref_nullptr)]
mod native;

pub mod err;
/// Rust APIs wrapping libhdfs API, providing better semantic and abstraction
pub mod hdfs;
#[cfg(feature = "test_util")]
/// Mainly for unit test
pub mod minidfs;
#[cfg(feature = "test_util")]
pub mod util;
/// For list files in a directory recursively
pub mod walkdir;

#[cfg(feature = "no_jvm_invocation")]
/// JNI context support for running inside JVM without invocation APIs
pub mod jni_context {
    //! This module provides support for using fs-hdfs3 within JNI native functions
    //! where the JavaVM is provided by the calling JVM, eliminating the need for
    //! JVM invocation APIs and linking to libjvm.so
    //!
    //! ## Usage in JNI Applications
    //!
    //! When implementing JNI native functions that use fs-hdfs3, call `set_java_vm()`
    //! once at application startup with the JavaVM from your JNI context:
    //!
    //! ```c
    //! // In your JNI_OnLoad function or early in your application
    //! JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    //!     if (set_java_vm_from_rust(vm) != 0) {
    //!         return JNI_ERR;
    //!     }
    //!     return JNI_VERSION_1_8;
    //! }
    //!
    //! // Later, in your JNI native functions, just use fs-hdfs3 normally
    //! JNIEXPORT jint JNICALL
    //! Java_com_example_MyClass_myNativeMethod(JNIEnv *env, jobject this) {
    //!     // No need to set JNIEnv - fs-hdfs3 will get it from the JavaVM
    //!     // Use fs-hdfs3 functions normally
    //!     return 0;
    //! }
    //! ```

    use crate::native::hdfsSetJavaVM;
    use std::ffi::c_void;

    /// Set the JavaVM for use in JNI context.
    /// This function should be called once at application startup, typically
    /// in JNI_OnLoad, to provide the JavaVM that fs-hdfs3 will use to obtain
    /// JNIEnv for each thread.
    ///
    /// # Arguments
    /// * `vm` - The JavaVM pointer from JNI context
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(())` on error
    ///
    /// # Safety
    /// This function is unsafe because it accepts a raw pointer that must be a valid JavaVM.
    /// The JavaVM must remain valid for the lifetime of the application.
    pub unsafe fn set_java_vm(vm: *mut c_void) -> Result<(), ()> {
        let result = hdfsSetJavaVM(vm);
        if result == 0 {
            Ok(())
        } else {
            Err(())
        }
    }
}
