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

use std::env;

fn main() {
    let mut vec_flags = vec![];

    // Pre check environment variables
    {
        // for jvm
        match env::var("JAVA_HOME") {
            Ok(val) => {
                vec_flags.push(format!("-I{}/include", val));
                if cfg!(target_os = "linux") {
                    vec_flags.push(format!("-I{}/include/linux", val));
                } else if cfg!(target_os = "macos") {
                    vec_flags.push(format!("-I{}/include/darwin", val));
                }
                // Tell cargo to tell rustc to link the system jvm shared library.
                println!("cargo:rustc-link-search=native={}/jre/lib/server", val);
                println!("cargo:rustc-link-lib=jvm");
            }
            Err(e) => {
                panic!("JAVA_HOME shell environment must be set: {}", e);
            }
        }

        // for hdfs
        match env::var("HADOOP_HOME") {
            Ok(val) => {
                // Tell cargo to tell rustc to link the system hdfs shared library.
                println!("cargo:rustc-link-search=native={}/lib/native", val);
                println!("cargo:rustc-link-lib=hdfs");
                vec_flags.push(format!("-I{}/include", val));
            }
            Err(e) => {
                panic!("HADOOP_HOME shell environment must be set: {}", e);
            }
        }
    }

    // build lib
    {
        let c_file = "c_src/libminidfs/native_mini_dfs.c";
        println!("cargo:rerun-if-changed={}", c_file);

        let mut builder = cc::Build::new();
        builder.file(c_file);
        for flag in &vec_flags {
            builder.flag(flag.as_str());
        }
        builder.compile("minidfs");
    }
}
