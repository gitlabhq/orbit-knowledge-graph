use std::fmt;

macro_rules! operators {
    ($($variant:ident => $label:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Operator { $($variant),+ }

        impl Operator {
            #[cfg(test)]
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

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
}

operators! {
    Hole => "_",
    Sequence => "...",
    Input => "Input",
    NodeScan => "NodeScan",
    EdgeScan => "EdgeScan",
    Filter => "Filter",
    Project => "Project",
    Aggregate => "Aggregate",
    Scan => "Scan",
    Deduplicate => "Deduplicate",
    Bind => "Bind",
    Join => "Join",
    SemiJoin => "SemiJoin",
    Union => "Union",
    With => "With",
    Cte => "CTE",
    Sort => "Sort",
    Limit => "Limit",
    Distinct => "Distinct",
    Neighbors => "Neighbors",
    PathFinding => "PathFinding",
    Hydration => "Hydration",
    Insert => "Insert",
}
