fn main() {
    // Automatically enable features based on target OS
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

    match target_os.as_str() {
        "linux" => {
            // Linux gets both agent and manager capabilities
            println!("cargo:rustc-cfg=feature=\"agent\"");
            println!("cargo:rustc-cfg=feature=\"manager\"");
        }
        "macos" | "windows" => {
            // macOS and Windows get manager-only capabilities
            println!("cargo:rustc-cfg=feature=\"manager\"");
        }
        _ => {
            // Other platforms default to manager-only
            println!("cargo:rustc-cfg=feature=\"manager\"");
        }
    }
}
