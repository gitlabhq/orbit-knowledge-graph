use super::types::{Edge, NONE};

pub trait TreeAccess {
    fn node_count(&self) -> u32;
    fn kind(&self, i: u32) -> u16;
    fn sym(&self, i: u32) -> u32;
    fn parent(&self, i: u32) -> u32;
    fn size(&self, i: u32) -> u32;
    fn field(&self, i: u32) -> u16;
    fn named(&self, i: u32) -> bool;
    fn dead(&self, i: u32) -> bool;
    fn start(&self, i: u32) -> u32;
    fn end(&self, i: u32) -> u32;
    fn start_row(&self, i: u32) -> u32;
    fn start_col(&self, i: u32) -> u32;
    fn end_row(&self, i: u32) -> u32;
    fn end_col(&self, i: u32) -> u32;
    fn edges(&self) -> &[Edge];
    fn label(&self) -> &str;

    #[inline]
    fn hop(&self, i: u32) -> u32 {
        i + self.size(i)
    }

    fn live(&self, mut c: u32, end: u32) -> u32 {
        while c < end && self.dead(c) {
            c = self.hop(c);
        }
        c
    }

    fn children_iter(&self, i: u32) -> ChildIter<'_, Self>
    where
        Self: Sized,
    {
        let end = self.hop(i);
        let c = self.live(i + 1, end);
        ChildIter { tree: self, c, end }
    }

    fn child_by_kind(&self, i: u32, kind: u16) -> Option<u32> {
        let end = self.hop(i);
        let mut c = i + 1;
        while c < end {
            if self.dead(c) {
                c += self.size(c).max(1);
                continue;
            }
            if self.kind(c) == kind {
                return Some(c);
            }
            c = self.hop(c);
        }
        None
    }

    fn root_sym(&self) -> u32 {
        if self.node_count() > 0 { self.sym(0) } else { 0 }
    }
}

pub struct ChildIter<'a, T: TreeAccess> {
    tree: &'a T,
    c: u32,
    end: u32,
}

impl<T: TreeAccess> Iterator for ChildIter<'_, T> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.c >= self.end {
            return None;
        }
        let r = self.c;
        self.c = self.tree.live(self.tree.hop(self.c), self.end);
        Some(r)
    }
}

impl TreeAccess for super::types::Tree {
    #[inline] fn node_count(&self) -> u32 { self.nodes.len() as u32 }
    #[inline] fn kind(&self, i: u32) -> u16 { self.nodes[i as usize].kind }
    #[inline] fn sym(&self, i: u32) -> u32 { self.nodes[i as usize].sym }
    #[inline] fn parent(&self, i: u32) -> u32 { self.nodes[i as usize].parent }
    #[inline] fn size(&self, i: u32) -> u32 { self.nodes[i as usize].size }
    #[inline] fn field(&self, i: u32) -> u16 { self.nodes[i as usize].field }
    #[inline] fn named(&self, i: u32) -> bool { self.nodes[i as usize].named }
    #[inline] fn dead(&self, i: u32) -> bool { self.nodes[i as usize].dead }
    #[inline] fn start(&self, i: u32) -> u32 { self.nodes[i as usize].start }
    #[inline] fn end(&self, i: u32) -> u32 { self.nodes[i as usize].end }
    #[inline] fn start_row(&self, i: u32) -> u32 { self.nodes[i as usize].start_row }
    #[inline] fn start_col(&self, i: u32) -> u32 { self.nodes[i as usize].start_col }
    #[inline] fn end_row(&self, i: u32) -> u32 { self.nodes[i as usize].end_row }
    #[inline] fn end_col(&self, i: u32) -> u32 { self.nodes[i as usize].end_col }
    fn edges(&self) -> &[Edge] {
        unsafe { &*self.edges_cell.as_ptr() }
    }
    fn label(&self) -> &str { &self.label }
}

impl TreeAccess for super::locked::LockedTree {
    #[inline] fn node_count(&self) -> u32 { self.kinds.len() as u32 }
    #[inline] fn kind(&self, i: u32) -> u16 { self.kinds[i as usize] }
    #[inline] fn sym(&self, i: u32) -> u32 { self.syms[i as usize] }
    #[inline] fn parent(&self, i: u32) -> u32 { self.parents[i as usize] }
    #[inline] fn size(&self, i: u32) -> u32 { self.sizes[i as usize] }
    #[inline] fn field(&self, i: u32) -> u16 { self.fields[i as usize] }
    #[inline] fn named(&self, i: u32) -> bool { self.flags[i as usize] & 1 != 0 }
    #[inline] fn dead(&self, _i: u32) -> bool { false }
    #[inline] fn start(&self, i: u32) -> u32 { self.starts[i as usize] }
    #[inline] fn end(&self, i: u32) -> u32 { self.ends[i as usize] }
    #[inline] fn start_row(&self, i: u32) -> u32 { self.start_rows[i as usize] }
    #[inline] fn start_col(&self, i: u32) -> u32 { self.start_cols[i as usize] }
    #[inline] fn end_row(&self, i: u32) -> u32 { self.end_rows[i as usize] }
    #[inline] fn end_col(&self, i: u32) -> u32 { self.end_cols[i as usize] }
    fn edges(&self) -> &[Edge] { &self.edges }
    fn label(&self) -> &str { &self.label }
}
