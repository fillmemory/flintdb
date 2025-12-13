use std::env;
use std::path::PathBuf;

fn main() {
    // Get the absolute path to the lib directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let lib_path = PathBuf::from(&manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("lib");
    
    println!("cargo:rustc-link-search=native={}", lib_path.display());
    println!("cargo:rustc-link-lib=dylib=flintdb");
    println!("cargo:rerun-if-changed=../../src/flintdb.h");
    
    // Also set rpath so the executable can find the library at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
}
