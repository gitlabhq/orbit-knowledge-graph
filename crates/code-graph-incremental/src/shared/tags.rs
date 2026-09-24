//! The only tag keys the linker and resolver branch on; every other tag a
//! language sets is passed through to export untouched.

#[derive(Clone, Copy)]
pub struct ReservedTags {
    pub callable: u32,
    pub scoped: u32,
    pub hoisted: u32,
    pub exports: u32,
    pub resolved_source: u32,
    pub visible_from: u32,
    pub implicit_self: u32,
}
