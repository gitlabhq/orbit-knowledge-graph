use code_graph_incremental::tree::{Compact, Mutable, Storage, Tree};

#[test]
fn owned_payloads_preserve_identity_and_support_updates_across_storage() {
    let mut tree = Tree::<Mutable<String>>::new("root".into());
    let root = tree.storage.id(tree.root().index());
    let child = tree.append(root, "child".into());
    let child_index = <Mutable<String> as Storage>::index(child);
    tree.set_tag(child_index, 1, 2);

    let mut tree: Tree<Compact<String>> = tree.into();
    tree.storage.node_mut(child_index).push_str(" updated");
    assert_eq!(
        tree.root().children().next().unwrap().node(),
        "child updated"
    );
    assert_eq!(tree.cursor(child_index).parent().unwrap().index(), 0);
    assert_eq!(tree.cursor(child_index).tag(1), Some(2));

    let tree: Tree<Mutable<String>> = tree.into();
    assert_eq!(
        tree.root().children_rev().next().unwrap().index(),
        child_index
    );
    assert_eq!(tree.cursor(child_index).node(), "child updated");
    assert_eq!(tree.cursor(child_index).tag(1), Some(2));
}
