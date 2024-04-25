fn main() {
    println!("cargo:rustc-env=RUSTC_FORCE_INCREMENTAL=1");
    println!("cargo:rerun-if-changed=./proto");
    //

    tonic_build::configure()
        .build_server(false)
        .out_dir("src")
        .compile(
            &["./proto/tercen.proto"],
            &["proto"],
        ).unwrap();

    //
    // // tonic_build::compile_protos("./proto/service.proto").unwrap();
    //
    // cargo_emit::rerun_if_changed!(
    //     "./proto/tercen.proto",
    //     "Cargo.toml",
    //     "Cargo.lock",
    //     "build.rs"
    // );
}

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // tonic_build::compile_protos("proto/model.proto")?;
//
//     tonic_build::compile_protos("proto/service.proto")?;
//     Ok(())
// }
