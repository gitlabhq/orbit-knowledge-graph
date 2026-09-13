use super::types::{NONE, Node, Tree};

impl Tree {
    pub fn remove(&mut self, i: u32) {
        self.nodes[i as usize].dead = true;
    }

    pub fn set_kind(&mut self, i: u32, k: u16) {
        self.nodes[i as usize].kind = k;
    }

    pub fn set_text(&mut self, i: u32, sym: u32) {
        self.nodes[i as usize].sym = sym;
    }

    pub fn flatten(&mut self, first: u32, last: u32, kind: u16, sym: u32) {
        let (end, end_span) = (self.hop(last), self.nodes[last as usize].end);
        let n = &mut self.nodes[first as usize];
        let old = n.size;
        n.kind = kind;
        n.sym = sym;
        n.end = end_span;
        n.size = end - first;
        let mut j = first + old;
        while j < end {
            self.nodes[j as usize].parent = first;
            j = self.hop(j);
        }
    }

    pub fn replace(&mut self, i: u32, sub: &[Node]) {
        let old = self.nodes[i as usize].size as usize;
        let (parent, field) = (self.nodes[i as usize].parent, self.nodes[i as usize].field);

        if sub.len() > old {
            let extra = sub.len() - old;
            for j in i..(i + old as u32) {
                self.nodes[j as usize].dead = true;
            }
            self.nodes
                .splice(i as usize..i as usize, vec![Node::default(); extra]);
            let shift = extra as u32;
            for j in (i as usize + sub.len())..self.nodes.len() {
                let p = self.nodes[j].parent;
                if p != NONE && p >= i {
                    self.nodes[j].parent = p + shift;
                }
            }
            let mut p = parent;
            while p != NONE {
                self.nodes[p as usize].size += shift;
                p = self.nodes[p as usize].parent;
            }
            for edge in self.edges.iter_mut() {
                if edge.from.node >= i {
                    edge.from.node += shift;
                }
                if edge.to.node >= i {
                    edge.to.node += shift;
                }
            }
            for a in self.appends.borrow_mut().iter_mut() {
                if a.0 >= i {
                    a.0 += shift;
                }
            }
            for ins in self.inserts.borrow_mut().iter_mut() {
                if ins.0 >= i {
                    ins.0 += shift;
                }
            }
        }

        for (k, mut n) in sub.iter().copied().enumerate() {
            n.parent = if n.parent == NONE {
                parent
            } else {
                n.parent + i
            };
            if k == 0 {
                n.field = field;
            }
            n.dead = false;
            self.nodes[i as usize + k] = n;
        }
        if sub.len() < old {
            let d = &mut self.nodes[i as usize + sub.len()];
            d.dead = true;
            d.size = (old - sub.len()) as u32;
        }
    }

    pub fn append(&self, parent: u32, leaf: Node) {
        self.appends.borrow_mut().push((parent, leaf));
    }

    pub fn insert_before(&self, i: u32, sub: &[Node]) {
        let mut buf = self.insert_buf.borrow_mut();
        let base = buf.len() as u32;
        self.inserts.borrow_mut().push((i, base, sub.len() as u32));
        for n in sub {
            buf.push(Node {
                parent: if n.parent == NONE {
                    NONE
                } else {
                    n.parent + base
                },
                ..*n
            });
        }
    }

    pub fn compact(&mut self) -> Vec<u32> {
        let mut appends = self.appends.take();
        let mut inserts = self.inserts.take();
        let insert_buf = self.insert_buf.take();
        appends.sort_by_key(|a| a.0);
        inserts.sort_by_key(|x| x.0);
        let old = std::mem::take(&mut self.nodes);
        let mut new = std::mem::take(&mut self.spare);
        new.clear();
        let mut remap = vec![NONE; old.len()];
        let mut open: Vec<(u32, u32, u32)> = Vec::new();
        let (mut i, mut ip) = (0u32, 0usize);
        loop {
            while open.last().is_some_and(|&(_, e, _)| i >= e) {
                let (o, _, oi) = open.pop().unwrap();
                let lo = appends.partition_point(|a| a.0 < oi);
                let hi = appends.partition_point(|a| a.0 <= oi);
                for &(_, leaf) in &appends[lo..hi] {
                    new.push(Node {
                        parent: o,
                        size: 1,
                        ..leaf
                    });
                }
                new[o as usize].size = new.len() as u32 - o;
            }
            if i as usize == old.len() {
                break;
            }
            let top = open.last().map_or(NONE, |&(o, _, _)| o);
            while ip < inserts.len() && inserts[ip].0 == i {
                let (_, s, l) = inserts[ip];
                let base = new.len() as u32;
                for n in &insert_buf[s as usize..(s + l) as usize] {
                    new.push(Node {
                        parent: if n.parent == NONE {
                            top
                        } else {
                            n.parent - s + base
                        },
                        ..*n
                    });
                }
                ip += 1;
            }
            let n = old[i as usize];
            if n.dead {
                i += n.size;
                continue;
            }
            remap[i as usize] = new.len() as u32;
            open.push((new.len() as u32, i + n.size, i));
            new.push(Node { parent: top, ..n });
            i += 1;
        }
        self.spare = old;
        self.nodes = new;
        for edge in &mut self.edges {
            if let Some(&new_from) = remap.get(edge.from.node as usize)
                && new_from != NONE
            {
                edge.from.node = new_from;
            }
            if let Some(&new_to) = remap.get(edge.to.node as usize)
                && new_to != NONE
            {
                edge.to.node = new_to;
            }
        }
        remap
    }
}
