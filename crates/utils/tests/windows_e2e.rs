#![cfg(windows)]
use orbit_utils::vfs::{Limits, Options, Vfs, sources::Directory};
use std::path::Path;
use std::process::Command;

fn mklink(flag: Option<&str>, link: &Path, target: &str) {
    let mut cmd = Command::new("cmd");
    cmd.args(["/C", "mklink"]);
    if let Some(flag) = flag {
        cmd.arg(flag);
    }
    let out = cmd.arg(link).arg(target).output().unwrap();
    println!(
        "mklink {flag:?} {} -> {target}: {} {}",
        link.display(),
        out.status,
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn real_windows_scenario() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("root");
    let outside = tmp.path().join("outside");
    std::fs::create_dir_all(root.join("a")).unwrap();
    std::fs::create_dir_all(root.join("sub")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(root.join("a").join("b"), b"data").unwrap();
    std::fs::write(root.join("sub").join("inner.txt"), b"inner").unwrap();
    std::fs::write(root.join("plain.txt"), b"plain").unwrap();
    std::fs::write(outside.join("o.txt"), b"out").unwrap();
    let outside_s = outside.to_str().unwrap().to_owned();

    mklink(Some("/J"), &root.join("junc_out"), &outside_s);
    mklink(
        None,
        &root.join("abs_file"),
        outside.join("o.txt").to_str().unwrap(),
    );
    mklink(Some("/D"), &root.join("abs_dir"), &outside_s);
    mklink(None, &root.join("rel_file"), r"sub\inner.txt");
    mklink(
        Some("/J"),
        &root.join("junc_in"),
        root.join("sub").to_str().unwrap(),
    );
    // Absolute target whose drive-stripped form names a file inside the vfs.
    let drive = &outside_s[..2];
    mklink(None, &root.join("abs_probe"), &format!(r"{drive}\a\b"));
    mklink(None, &root.join("unc_probe"), &format!(r"\\?\{drive}\a\b"));

    for name in [
        "junc_out",
        "abs_file",
        "abs_dir",
        "rel_file",
        "junc_in",
        "abs_probe",
        "unc_probe",
    ] {
        println!(
            "raw read_link {name}: {:?}",
            std::fs::read_link(root.join(name))
        );
    }

    let vfs = Vfs::load(Directory(&root), (), Limits::default(), Options::default())
        .expect("load must succeed with absolute links");
    let mut files: Vec<_> = vfs.files().map(|f| f.path.to_string()).collect();
    files.sort();
    println!("files: {files:?}");
    for name in [
        "junc_out",
        "abs_file",
        "abs_dir",
        "rel_file",
        "junc_in",
        "abs_probe",
        "unc_probe",
    ] {
        println!("stat {name}: {:?}", vfs.stat(name));
        assert!(
            files.iter().any(|f| f == name),
            "{name} missing from files()"
        );
    }
    for name in ["junc_out", "abs_file", "abs_dir"] {
        assert_eq!(
            vfs.stat(name).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }
    let rel = vfs.stat("rel_file").unwrap();
    assert_eq!(rel.link.as_deref(), Some("sub/inner.txt"));
    assert_eq!(&*vfs.read("rel_file").unwrap(), b"inner");
    let probe = vfs.stat("abs_probe").unwrap();
    assert_eq!(probe.link.as_deref(), Some("/a/b"));
    assert_eq!(&*vfs.read("abs_probe").unwrap(), b"data");
    let unc = vfs.stat("unc_probe").unwrap();
    assert_eq!(unc.link.as_deref(), Some("/a/b"));
}
