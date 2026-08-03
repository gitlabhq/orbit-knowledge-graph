/// Bracket nesting past this depth is faulted before oxc ever parses the file.
/// oxc recurses once per nesting level with no internal limit and overflows the
/// 8 MiB parse-worker stack around depth 4000-8000 in release (per-shape cliff
/// table in `git log` for 1b58c1886 / knowledge-graph#1114); a stack overflow
/// aborts the whole process uncatchably, taking every concurrent file down. 1000
/// keeps a 4-8x margin below the cliff and sits far above any real source (the
/// deepest nesting measured across ~192k JS-family files was 131).
pub(super) const MAX_NESTING_DEPTH: usize = 1000;

/// Maximum `()`/`[]`/`{}` nesting depth in `source`, counting every bracket byte
/// including those inside strings, comments, and regex literals. The margin to
/// [`MAX_NESTING_DEPTH`] is what makes that crude count safe: over-counting can
/// only skip one extra pathological file, while under-counting could let a real
/// stack-overflowing file through, so the count errs high on purpose. Iterative,
/// so it cannot itself overflow on the input it guards.
pub(super) fn max_bracket_nesting_depth(source: &str) -> usize {
    let (mut depth, mut max) = (0usize, 0usize);
    for b in source.bytes() {
        match b {
            b'(' | b'[' | b'{' => {
                depth += 1;
                max = max.max(depth);
            }
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    max
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_bracket_nesting_depth_counts_structural_nesting() {
        assert_eq!(max_bracket_nesting_depth("const x = 1;"), 0);
        assert_eq!(max_bracket_nesting_depth("([{}])"), 3);
        assert_eq!(max_bracket_nesting_depth("f(g(h()))"), 3);
        assert_eq!(max_bracket_nesting_depth("a()b()c()"), 1);
    }
}
