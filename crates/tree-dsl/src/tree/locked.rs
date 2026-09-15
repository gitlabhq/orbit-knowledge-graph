use std::cell::RefCell;
use super::types::{Edge, Node, Tree, NONE};

pub struct LockedTree {
    pub kinds: Vec<u16>,
    pub parents: Vec<u32>,
    pub syms: Vec<u32>,
    pub sizes: Vec<u32>,
    pub starts: Vec<u32>,
    pub ends: Vec<u32>,
    pub fields: Vec<u16>,
    pub flags: Vec<u8>,
    pub start_rows: Vec<u32>,
    pub start_cols: Vec<u32>,
    pub end_rows: Vec<u32>,
    pub end_cols: Vec<u32>,
    pub edges: Vec<Edge>,
    pub label: String,
}

const FLAG_NAMED: u8 = 1;
const FLAG_SYNTH: u8 = 2;

impl LockedTree {
    pub fn len(&self) -> u32 {
        self.kinds.len() as u32
    }

    #[inline]
    pub fn kind(&self, i: u32) -> u16 {
        self.kinds[i as usize]
    }

    #[inline]
    pub fn sym(&self, i: u32) -> u32 {
        self.syms[i as usize]
    }

    #[inline]
    pub fn parent(&self, i: u32) -> u32 {
        self.parents[i as usize]
    }

    #[inline]
    pub fn size(&self, i: u32) -> u32 {
        self.sizes[i as usize]
    }

    #[inline]
    pub fn hop(&self, i: u32) -> u32 {
        i + self.sizes[i as usize]
    }

    #[inline]
    pub fn field(&self, i: u32) -> u16 {
        self.fields[i as usize]
    }

    #[inline]
    pub fn named(&self, i: u32) -> bool {
        self.flags[i as usize] & FLAG_NAMED != 0
    }

    #[inline]
    pub fn start(&self, i: u32) -> u32 {
        self.starts[i as usize]
    }

    #[inline]
    pub fn end(&self, i: u32) -> u32 {
        self.ends[i as usize]
    }

    #[inline]
    pub fn start_row(&self, i: u32) -> u32 {
        self.start_rows[i as usize]
    }

    #[inline]
    pub fn start_col(&self, i: u32) -> u32 {
        self.start_cols[i as usize]
    }

    #[inline]
    pub fn end_row(&self, i: u32) -> u32 {
        self.end_rows[i as usize]
    }

    #[inline]
    pub fn end_col(&self, i: u32) -> u32 {
        self.end_cols[i as usize]
    }

    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    pub fn children(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let end = self.hop(i);
        let mut c = i + 1;
        std::iter::from_fn(move || {
            while c < end {
                let r = c;
                c = self.hop(c);
                return Some(r);
            }
            None
        })
    }
}

impl From<Tree> for LockedTree {
    fn from(t: Tree) -> Self {
        let n = t.nodes.len();
        let mut kinds = Vec::with_capacity(n);
        let mut parents = Vec::with_capacity(n);
        let mut syms = Vec::with_capacity(n);
        let mut sizes = Vec::with_capacity(n);
        let mut starts = Vec::with_capacity(n);
        let mut ends = Vec::with_capacity(n);
        let mut fields = Vec::with_capacity(n);
        let mut flags = Vec::with_capacity(n);
        let mut start_rows = Vec::with_capacity(n);
        let mut start_cols = Vec::with_capacity(n);
        let mut end_rows = Vec::with_capacity(n);
        let mut end_cols = Vec::with_capacity(n);

        for node in &t.nodes {
            kinds.push(node.kind);
            parents.push(node.parent);
            syms.push(node.sym);
            sizes.push(node.size);
            starts.push(node.start);
            ends.push(node.end);
            fields.push(node.field);
            start_rows.push(node.start_row);
            start_cols.push(node.start_col);
            end_rows.push(node.end_row);
            end_cols.push(node.end_col);
            let mut f = 0u8;
            if node.named { f |= FLAG_NAMED; }
            if node.synth { f |= FLAG_SYNTH; }
            flags.push(f);
        }

        let edges = t.edges_cell.into_inner();

        LockedTree {
            kinds,
            parents,
            syms,
            sizes,
            starts,
            ends,
            fields,
            flags,
            start_rows,
            start_cols,
            end_rows,
            end_cols,
            edges,
            label: t.label,
        }
    }
}

impl From<LockedTree> for Tree {
    fn from(lt: LockedTree) -> Self {
        let n = lt.kinds.len();
        let mut nodes = Vec::with_capacity(n);
        for i in 0..n {
            nodes.push(Node {
                kind: lt.kinds[i],
                parent: lt.parents[i],
                sym: lt.syms[i],
                size: lt.sizes[i],
                start: lt.starts[i],
                end: lt.ends[i],
                field: lt.fields[i],
                named: lt.flags[i] & 1 != 0,
                synth: lt.flags[i] & 2 != 0,
                dead: false,
                start_row: lt.start_rows[i],
                start_col: lt.start_cols[i],
                end_row: lt.end_rows[i],
                end_col: lt.end_cols[i],
                id: 0,
            });
        }
        Tree {
            nodes,
            edges_cell: RefCell::new(lt.edges),
            label: lt.label,
            ..Default::default()
        }
    }
}
