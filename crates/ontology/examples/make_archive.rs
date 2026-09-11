use std::collections::BTreeMap;
use std::path::Path;

use ontology::archive::OntologyArchive;

fn collect(root: &Path, dir: &Path, sources: &mut BTreeMap<String, String>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .expect("read_dir")
        .map(|e| e.expect("entry"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            collect(root, &path, sources);
            continue;
        }
        let name = path.to_string_lossy().to_string();
        if !(name.ends_with(".yaml") || name.ends_with(".sql.j2")) {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .to_string();
        sources.insert(rel, std::fs::read_to_string(&path).expect("read"));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let version: u32 = args[1].parse().expect("schema version");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);

    let mut sources = BTreeMap::new();
    collect(root, root, &mut sources);
    let archive = OntologyArchive::from_sources(version, &sources).expect("archive");
    archive.load_ontology().expect("ontology loads");
    std::fs::write(out, archive.bytes()).expect("write");
    println!(
        "wrote {} bytes, {} sources, schema {}",
        archive.bytes().len(),
        sources.len(),
        archive.schema_version()
    );
}
