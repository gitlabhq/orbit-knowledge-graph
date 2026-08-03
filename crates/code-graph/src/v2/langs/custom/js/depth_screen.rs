/// oxc aborts the process past depth 4000-8000 (release); deepest real file
/// measured was 131. Cliff table in `git log` for 1b58c1886, knowledge-graph#1114.
pub(super) const MAX_NESTING_DEPTH: usize = 1000;

pub(super) fn bracket_depth_upper_bound(source: &str) -> usize {
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
    fn bracket_depth_counts_structural_nesting() {
        assert_eq!(bracket_depth_upper_bound("const x = 1;"), 0);
        assert_eq!(bracket_depth_upper_bound("([{}])"), 3);
        assert_eq!(bracket_depth_upper_bound("f(g(h()))"), 3);
        assert_eq!(bracket_depth_upper_bound("a()b()c()"), 1);
    }

    #[test]
    fn bracket_depth_overcounts_brackets_in_strings() {
        assert_eq!(bracket_depth_upper_bound("const s = \"(((((\";"), 5);
    }
}
