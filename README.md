# fs-hdfs3

It's based on the version ``0.0.4`` of http://hyunsik.github.io/hdfs-rs to provide libhdfs binding library and rust APIs which safely wraps libhdfs binding APIs.

# Current Status
* All libhdfs FFI APIs are ported.
* Safe Rust wrapping APIs to cover most of the libhdfs APIs except those related to zero-copy read.
* Compared to hdfs-rs, it removes the lifetime in HdfsFs, which will be more friendly for others to depend on.

## Documentation
* [API documentation] (https://docs.rs/crate/fs-hdfs3)

## Requirements
* The C related files are from the branch ``3.1.4`` of hadoop repository. For rust usage, a few changes are also applied.
* No need to compile the Hadoop native library by yourself. However, the Hadoop jar dependencies are still required.

## Usage
Add this to your Cargo.toml:

```toml
[dependencies]
fs-hdfs3 = "0.1.12"
```

### Build

We need to specify ```$JAVA_HOME``` to make Java shared library available for building.

### Run
Since our compiled libhdfs is JNI-based implementation, 
it requires Hadoop-related classes available through ``CLASSPATH``. An example,

```sh
export CLASSPATH=$CLASSPATH:`hadoop classpath --glob`
```

Also, we need to specify the JVM dynamic library path for the application to load the JVM shared library at runtime.

For jdk8 and macOS, it's

```sh
export DYLD_LIBRARY_PATH=$JAVA_HOME/jre/lib/server
```

For jdk11 (or later jdks) and macOS, it's

```sh
export DYLD_LIBRARY_PATH=$JAVA_HOME/lib/server
```

For jdk8 and Centos
```sh
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server
```

For jdk11 (or later jdks) and Centos
```sh
export LD_LIBRARY_PATH=$JAVA_HOME/lib/server
```

### Testing
The test also requires the ``CLASSPATH`` and `DYLD_LIBRARY_PATH` (or `LD_LIBRARY_PATH`). In case that the java class of ``org.junit.Assert`` can't be found. Refine the ``$CLASSPATH`` as follows:

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

## JNI Context Support

fs-hdfs3 supports a special `no_jvm_invocation` feature for use cases where the library runs within JNI native functions and JVM invocation APIs are not needed. This is useful when implementing Java libraries with native JNI modules that use fs-hdfs3 to call Hadoop FileSystem APIs.

### Using the no_jvm_invocation Feature

To use this feature, add it to your Cargo.toml:

```toml
[dependencies.fs-hdfs3]
version = "0.1.12"
features = ["no_jvm_invocation"]
```

When this feature is enabled:
- The library does not link to `libjvm.so`
- JVM invocation APIs are disabled
- You must provide the JavaVM using the `jni_context` module functions

### JNI Native Function Example

```rust
use fs_hdfs3::jni_context::set_java_vm;
use fs_hdfs3::hdfs::get_hdfs;

// Call this once at application startup, typically in JNI_OnLoad
#[no_mangle]
pub extern "C" fn JNI_OnLoad(vm: *mut std::ffi::c_void, _reserved: *mut std::ffi::c_void) -> i32 {
    // Set the JavaVM for fs-hdfs3 to use
    if unsafe { set_java_vm(vm) }.is_err() {
        return -1; // JNI_ERR
    }
    0x00010008 // JNI_VERSION_1_8
}

// Later, in your JNI native functions, just use fs-hdfs3 normally
#[no_mangle]
pub extern "C" fn Java_com_example_MyClass_myNativeMethod(
    _env: *mut std::ffi::c_void,
    _class: *mut std::ffi::c_void,
) -> i32 {
    // No need to manage JNIEnv - fs-hdfs3 handles it automatically
    match get_hdfs() {
        Ok(fs) => {
            // Use the filesystem...
            println!("Successfully connected to HDFS");
        }
        Err(e) => {
            eprintln!("Failed to connect to HDFS: {:?}", e);
            return -1;
        }
    }
    
    0
}
```
