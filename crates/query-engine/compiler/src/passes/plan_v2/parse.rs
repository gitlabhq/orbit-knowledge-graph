//! A small expression language so plans read as the SQL they produce:
//!
//! ```text
//! arrayConcat([tuple(f.anchor_id, 'User')], f.path_nodes) AS _gkg_path
//! e0.relationship_kind = 'AUTHORED' AND e0._deleted = false
//! e1.source_id IN (1, 2, 3)
//! x -> tupleElement(x, 1)
//! ```
//!
//! Parsed into `PExpr` when a plan is built. Anything the optimizer must
//! recognize (`Scope`, `NodeFilter`) is constructed directly, not parsed.
//!
//! Only aliases, column names, and ontology entity names are interpolated
//! into expression text. User data (ids, filter values, relationship kinds)
//! goes through the typed builders in `expr.rs`.

use super::{CmpOp, Col, Lit, Named, PExpr};

#[macro_export]
macro_rules! pe {
    ($($t:tt)*) => { $crate::passes::plan_v2::parse::pe(&format!($($t)*)) };
}

#[macro_export]
macro_rules! pn {
    ($($t:tt)*) => { $crate::passes::plan_v2::parse::pn(&format!($($t)*)) };
}

/// Parse one expression. Panics on a syntax error: plan text is code, and
/// every shape is built by the fixture tests.
pub fn pe(src: &str) -> PExpr {
    let mut p = Parser::new(src);
    let e = p.or();
    p.expect_end();
    e
}

/// Parse `expr AS alias`.
pub fn pn(src: &str) -> Named {
    let mut p = Parser::new(src);
    let e = p.or();
    p.keyword("AS");
    let alias = p.ident();
    p.expect_end();
    (e, alias)
}

/// Parse `a.x = b.y` join conditions.
pub fn on(conds: &[&str]) -> Vec<(Col, Col)> {
    conds
        .iter()
        .map(|c| match pe(c) {
            PExpr::Cmp(CmpOp::Eq, l, r) => match (*l, *r) {
                (PExpr::Col(a, b), PExpr::Col(c, d)) => ((a, b), (c, d)),
                _ => panic!("join condition must be col = col: {c}"),
            },
            _ => panic!("join condition must be col = col: {c}"),
        })
        .collect()
}

// ── Parser ────────────────────────────────────────────────────────────────────

struct Parser<'s> {
    src: &'s str,
    pos: usize,
}

impl<'s> Parser<'s> {
    fn new(src: &'s str) -> Self {
        Self { src, pos: 0 }
    }

    fn or(&mut self) -> PExpr {
        let mut xs = vec![self.and()];
        while self.keyword("OR") {
            xs.push(self.and());
        }
        if xs.len() == 1 {
            xs.pop().unwrap()
        } else {
            PExpr::Or(xs)
        }
    }

    fn and(&mut self) -> PExpr {
        let mut xs = vec![self.cmp()];
        while self.keyword("AND") {
            xs.push(self.cmp());
        }
        if xs.len() == 1 {
            xs.pop().unwrap()
        } else {
            PExpr::And(xs)
        }
    }

    fn cmp(&mut self) -> PExpr {
        let l = self.term();
        for (tok, op) in [
            ("!=", CmpOp::Ne),
            ("<=", CmpOp::Le),
            (">=", CmpOp::Ge),
            ("=", CmpOp::Eq),
            ("<", CmpOp::Lt),
            (">", CmpOp::Gt),
        ] {
            if self.eat(tok) {
                return PExpr::Cmp(op, Box::new(l), Box::new(self.term()));
            }
        }
        if self.keyword("IN") {
            self.expect("(");
            let mut vs = Vec::new();
            while !self.eat(")") {
                match self.term() {
                    PExpr::Lit(v) => vs.push(v),
                    _ => self.fail("literal in IN list"),
                }
                self.eat(",");
            }
            return PExpr::In(Box::new(l), vs);
        }
        l
    }

    fn term(&mut self) -> PExpr {
        self.skip_ws();
        if self.eat("(") {
            let e = self.or();
            self.expect(")");
            return e;
        }
        if self.eat("[") {
            return PExpr::Func("array".into(), self.args("]"));
        }
        for q in ['\'', '"'] {
            if self.eat(&q.to_string()) {
                let end = self
                    .rest()
                    .find(q)
                    .unwrap_or_else(|| self.fail("closing quote"));
                let s = self.rest()[..end].to_string();
                self.pos += end + 1;
                return PExpr::Lit(Lit::Str(s));
            }
        }
        let r = self.rest();
        if r.starts_with(|c: char| c.is_ascii_digit() || c == '-') {
            let n = r[1..]
                .find(|c: char| !c.is_ascii_digit())
                .map(|i| i + 1)
                .unwrap_or(r.len());
            self.pos += n;
            return PExpr::Lit(Lit::Int(
                r[..n].parse().unwrap_or_else(|_| self.fail("integer")),
            ));
        }
        let name = self.ident();
        match name.as_str() {
            "true" => return PExpr::Lit(Lit::Bool(true)),
            "false" => return PExpr::Lit(Lit::Bool(false)),
            _ => {}
        }
        if self.eat("->") {
            return PExpr::Lambda(name, Box::new(self.or()));
        }
        if self.eat("(") {
            return PExpr::Func(name, self.args(")"));
        }
        if self.eat(".") {
            return PExpr::Col(name, self.ident());
        }
        PExpr::Ident(name)
    }

    fn args(&mut self, close: &str) -> Vec<PExpr> {
        let mut xs = Vec::new();
        while !self.eat(close) {
            xs.push(self.or());
            self.eat(",");
        }
        xs
    }

    fn ident(&mut self) -> String {
        self.skip_ws();
        let r = self.rest();
        let n = r
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(r.len());
        if n == 0 || r.starts_with(|c: char| c.is_ascii_digit()) {
            self.fail("identifier");
        }
        self.pos += n;
        r[..n].to_string()
    }

    fn keyword(&mut self, kw: &str) -> bool {
        self.skip_ws();
        let r = self.rest();
        let matches = r.len() >= kw.len()
            && r[..kw.len()].eq_ignore_ascii_case(kw)
            && !r[kw.len()..].starts_with(|c: char| c.is_alphanumeric() || c == '_');
        if matches {
            self.pos += kw.len();
        }
        matches
    }

    fn eat(&mut self, tok: &str) -> bool {
        if self.peek(tok) {
            self.pos += tok.len();
            true
        } else {
            false
        }
    }

    fn peek(&mut self, tok: &str) -> bool {
        self.skip_ws();
        self.rest().starts_with(tok)
    }

    fn expect(&mut self, tok: &str) {
        if !self.eat(tok) {
            self.fail(tok);
        }
    }

    fn expect_end(&mut self) {
        self.skip_ws();
        if !self.rest().is_empty() {
            self.fail("end of expression");
        }
    }

    fn fail(&self, what: &str) -> ! {
        panic!("expected {what} at {:?} in {:?}", self.rest(), self.src)
    }

    fn skip_ws(&mut self) {
        self.pos += self.rest().len() - self.rest().trim_start().len();
    }

    fn rest(&self) -> &'s str {
        &self.src[self.pos..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sql_shapes() {
        assert_eq!(
            pe("a.b = 'x'"),
            PExpr::Cmp(
                CmpOp::Eq,
                Box::new(PExpr::Col("a".into(), "b".into())),
                Box::new(PExpr::Lit(Lit::Str("x".into())))
            )
        );
        assert!(matches!(
            pe("f(a.b, [1, 2]) AND x.y IN (1, 2) OR z.w >= -3"),
            PExpr::Or(_)
        ));
        assert!(matches!(pe("x -> tupleElement(x, 1)"), PExpr::Lambda(..)));
        assert_eq!(pn("plus(f.depth, b.depth) AS depth").1, "depth");
        assert_eq!(
            on(&["a.x = b.y"]),
            vec![(("a".into(), "x".into()), ("b".into(), "y".into()))]
        );
    }
}
