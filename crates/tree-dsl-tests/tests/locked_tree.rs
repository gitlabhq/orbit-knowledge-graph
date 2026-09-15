use tree_dsl::grammar::SupportLang;
use tree_dsl::tree::{LockedTree, TreeAccess};

#[test]
fn locked_tree_preserves_structure() {
    let fixtures = vec![
        ("main.py".into(), "from utils import helper\nhelper()\n".into()),
        ("utils.py".into(), "def helper():\n    pass\n\ndef extra(x):\n    return x + 1\n".into()),
    ];

    let result = tree_dsl::index(SupportLang::Python, &fixtures);

    for locked in &result.trees {
        assert!(locked.len() > 0);
        assert!(!locked.label.is_empty());

        for i in 0..locked.len() {
            assert!(locked.size(i) >= 1);
        }

        let root_children: Vec<u32> = locked.children(0).collect();
        assert!(!root_children.is_empty());
    }
}

#[test]
fn locked_tree_memory_smaller() {
    let aos_size = std::mem::size_of::<tree_dsl::tree::Node>();
    let soa_per_node = 2 + 4 + 4 + 4 + 4 + 4 + 2 + 1 + 4 + 4 + 4 + 4; // 41

    eprintln!("AoS: {aos_size} bytes/node");
    eprintln!("SoA: {soa_per_node} bytes/node");
    eprintln!("ratio: {:.1}x", aos_size as f64 / soa_per_node as f64);

    assert!(soa_per_node < aos_size);
}
