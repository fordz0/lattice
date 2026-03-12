mod ui {
    include!("src/ui.rs");
}

fn main() {
    println!("cargo:rerun-if-changed=src/ui.rs");

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR missing");
    let site_index = std::path::Path::new(&manifest_dir)
        .join("site")
        .join("index.html");
    std::fs::write(&site_index, ui::page_html()).expect("failed to write fray/site/index.html");
}
