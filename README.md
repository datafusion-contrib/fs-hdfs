# fs-hdfs

It's based on the version ``0.0.4`` of http://hyunsik.github.io/hdfs-rs to provide libhdfs binding library and rust APIs which safely wraps libhdfs binding APIs.

# Current Status
* All libhdfs FFI APIs are ported.
* Safe Rust wrapping APIs to cover most of the libhdfs APIs except those related to zero-copy read.
* Compared to hdfs-rs, it removes the lifetime in HdfsFs, which will be more friendly for others to depend on.

## Documentation
* [API documentation] (https://yahonanjing.github.io/fs-hdfs)

## Requirements
* The C related files are from the branch ``2.7.3`` of hadoop repository. For rust usage, a few changes are also applied.
* No need to compile the Hadoop native library by yourself. However, the Hadoop jar dependencies are still required.

## Usage
Add this to your Cargo.toml:

```toml
[dependencies]
fs-hdfs = "0.1.4"
```

Firstly, we need to add library path for the jvm related dependencies. An example for MacOS,

```sh
export DYLD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server
```
For Centos
```sh
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server
```

Here, ``$JAVA_HOME`` need to be specified and exported.

Since our compiled libhdfs is JNI native implementation, it requires the proper ``CLASSPATH``. An example,

```sh
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`
```

## Testing
The test also requires the ``CLASSPATH``. In case that the java class of ``org.junit.Assert`` can't be found. Refine the ``$CLASSPATH`` as follows:

```sh
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`:$HADOOP_HOME/share/hadoop/tools/lib/*
```

Here, ``$HADOOP_HOME`` need to be specified and exported.

Then you can run

```bash
cargo test
```

## Example

```rust
use std::sync::Arc;
use hdfs::hdfs::{get_hdfs_by_full_path, HdfsFs};

let fs: Arc<HdfsFs> = get_hdfs_by_full_path("hdfs://localhost:8020/").ok().unwrap();
match fs.mkdir("/data") {
    Ok(_) => { println!("/data has been created") },
    Err(_)  => { panic!("/data creation has failed") }
};
```