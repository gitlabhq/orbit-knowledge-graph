use code_graph::v2::config::{Language, detect_language_from_path};
use code_graph_incremental::treesitter::SupportLang;

#[test]
fn both_engines_agree_which_file_names_are_dockerfiles() {
    let names = [
        "Dockerfile",
        "dockerfile",
        "DOCKERFILE",
        "Containerfile",
        "Dockerfile.dev",
        "svc/api/Containerfile.prod",
        "app.Dockerfile",
        "build.Containerfile",
        "x.dockerfile",
        "Dockerfile.mri.release",
        "Dockerfile.py",
        "Dockerfile.md",
        "Dockerfile.tt",
        "Dockerfile.mri.erb",
        "Dockerfile.wip.diff",
        "Containerfile.template",
        "Dockerfile.dockerignore",
        "Dockerfile.yml",
        "Dockerfile.json",
        "Dockerfile.lock",
        "Dockerfile-notes.md",
        "MyDockerfile",
        "README.md",
        "Makefile",
    ];
    for name in names {
        let incremental = SupportLang::from_path(name) == Some(SupportLang::Dockerfile);
        let v2 = detect_language_from_path(name) == Some(Language::Dockerfile);
        assert_eq!(incremental, v2, "{name}");
    }
}
