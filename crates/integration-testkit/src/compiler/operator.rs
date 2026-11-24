use std::fmt;

macro_rules! operators {
    ($($variant:ident => $label:literal $($leaf:ident)?),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Operator { $($variant),+ }

        impl Operator {
            #[cfg(test)]
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub fn has_children(self) -> bool {
                match self {
                    $(Self::$variant => operators!(@children $($leaf)?),)+
                }
            }

            pub fn parse(label: &str) -> Result<Self, String> {
                match label {
                    $($label => Ok(Self::$variant),)+
                    _ => Err(format!("unknown operator '{label}'")),
                }
            }
        }

        impl fmt::Display for Operator {
            fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
                out.write_str(match self { $(Self::$variant => $label),+ })
            }
        }
    };
    (@children leaf) => { false };
    (@children) => { true };
}

operators! {
    Hole => "_" leaf,
    Sequence => "..." leaf,
    Input => "Input",
    NodeScan => "NodeScan" leaf,
    EdgeScan => "EdgeScan" leaf,
    Filter => "Filter",
    Project => "Project",
    Aggregate => "Aggregate",
    Scan => "Scan" leaf,
    Deduplicate => "Deduplicate",
    Bind => "Bind",
    Join => "Join",
    SemiJoin => "SemiJoin",
    Union => "Union",
    With => "With",
    Cte => "CTE",
    Sort => "Sort",
    Limit => "Limit",
    Offset => "Offset",
    Distinct => "Distinct",
    Neighbors => "Neighbors",
    PathFinding => "PathFinding",
    Hydration => "Hydration",
    Insert => "Insert" leaf,
}
