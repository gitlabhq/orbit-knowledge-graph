use crate::v2::types::ssa::ArgType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Primitive {
    Byte,
    Short,
    Char,
    Int,
    Long,
    Float,
    Double,
    Boolean,
}

impl Primitive {
    fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "byte" => Self::Byte,
            "short" => Self::Short,
            "char" => Self::Char,
            "int" => Self::Int,
            "long" => Self::Long,
            "float" => Self::Float,
            "double" => Self::Double,
            "boolean" => Self::Boolean,
            _ => return None,
        })
    }

    fn from_box(name: &str) -> Option<Self> {
        Some(match name {
            "Byte" => Self::Byte,
            "Short" => Self::Short,
            "Character" => Self::Char,
            "Integer" => Self::Int,
            "Long" => Self::Long,
            "Float" => Self::Float,
            "Double" => Self::Double,
            "Boolean" => Self::Boolean,
            _ => return None,
        })
    }

    fn is_numeric(self) -> bool {
        self != Self::Boolean
    }

    fn widens_to(self, target: Self) -> bool {
        use Primitive::*;
        self == target
            || matches!(
                (self, target),
                (Byte, Short | Int | Long | Float | Double)
                    | (Short, Int | Long | Float | Double)
                    | (Char, Int | Long | Float | Double)
                    | (Int, Long | Float | Double)
                    | (Long, Float | Double)
                    | (Float, Double)
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Ty {
    Primitive(Primitive),
    Boxed(Primitive),
    Null,
    Array { depth: usize, element: String },
    Reference(String),
}

impl Ty {
    fn parse(text: &str) -> Self {
        let text = text.trim();
        let element = text.trim_end_matches("[]");
        let depth = (text.len() - element.len()) / 2;
        let element = simple_name(element);
        if depth > 0 {
            return Self::Array {
                depth,
                element: element.to_string(),
            };
        }
        if element == "null" {
            return Self::Null;
        }
        if let Some(p) = Primitive::from_name(element) {
            return Self::Primitive(p);
        }
        if let Some(p) = Primitive::from_box(element) {
            return Self::Boxed(p);
        }
        Self::Reference(element.to_string())
    }
}

fn simple_name(text: &str) -> &str {
    let text = text.split('<').next().unwrap_or(text);
    text.rsplit('.').next().unwrap_or(text)
}

fn is_type_variable(name: &str) -> bool {
    name.len() <= 2
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

fn is_box_supertype(name: &str, primitive: Primitive) -> bool {
    matches!(name, "Object" | "Serializable" | "Comparable" | "Constable")
        || (name == "Number" && primitive.is_numeric())
        || is_type_variable(name)
}

fn is_final_reference(name: &str) -> bool {
    name == "String" || Primitive::from_box(name).is_some()
}

#[derive(Debug)]
struct Param {
    ty: Ty,
    variadic: bool,
}

fn parse_signature(signature: &str) -> Vec<Param> {
    let inner = signature
        .trim()
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or_default();
    inner
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| match p.strip_suffix("...") {
            Some(element) => Param {
                ty: Ty::parse(&format!("{element}[]")),
                variadic: true,
            },
            None => Param {
                ty: Ty::parse(p),
                variadic: false,
            },
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Strict,
    Loose,
}

fn convertible(arg: &Ty, param: &Ty, phase: Phase) -> bool {
    let loose = phase == Phase::Loose;
    match (arg, param) {
        (Ty::Primitive(a), Ty::Primitive(p)) => a.widens_to(*p),
        (Ty::Primitive(a), Ty::Boxed(p)) => loose && a == p,
        (Ty::Primitive(a), Ty::Reference(p)) => loose && is_box_supertype(p, *a),
        (Ty::Primitive(_), _) => false,
        (Ty::Boxed(a), Ty::Primitive(p)) => loose && a.widens_to(*p),
        (Ty::Boxed(a), Ty::Boxed(p)) => a == p,
        (Ty::Boxed(a), Ty::Reference(p)) => is_box_supertype(p, *a),
        (Ty::Boxed(_), _) => false,
        (Ty::Null, Ty::Primitive(_)) => false,
        (Ty::Null, _) => true,
        (Ty::Array { .. }, Ty::Primitive(_) | Ty::Boxed(_) | Ty::Null) => false,
        (
            Ty::Array {
                depth: ad,
                element: ae,
            },
            Ty::Array {
                depth: pd,
                element: pe,
            },
        ) => {
            ad == pd
                && (ae == pe
                    || (Primitive::from_name(ae).is_none() && Primitive::from_name(pe).is_none()))
        }
        (Ty::Array { .. }, Ty::Reference(p)) => {
            matches!(p.as_str(), "Object" | "Cloneable" | "Serializable") || is_type_variable(p)
        }
        (Ty::Reference(_), Ty::Primitive(_) | Ty::Boxed(_) | Ty::Null) => false,
        (Ty::Reference(_), Ty::Array { .. }) => false,
        (Ty::Reference(a), Ty::Reference(p)) => {
            a == p
                || p == "Object"
                || is_type_variable(p)
                || if is_final_reference(a) {
                    matches!(p.as_str(), "CharSequence" | "Comparable" | "Serializable")
                } else {
                    !is_final_reference(p)
                }
        }
    }
}

fn subtype(sub: &Ty, sup: &Ty) -> bool {
    match (sub, sup) {
        (Ty::Primitive(a), Ty::Primitive(b)) => a.widens_to(*b),
        (Ty::Primitive(_), _) | (_, Ty::Primitive(_)) => false,
        (a, b) if a == b => true,
        (Ty::Boxed(p), Ty::Reference(name)) => {
            matches!(name.as_str(), "Object" | "Serializable" | "Comparable")
                || (name == "Number" && p.is_numeric())
        }
        (_, Ty::Reference(name)) => name == "Object",
        _ => false,
    }
}

fn param_at(params: &[Param], i: usize, expand_variadic: bool) -> Option<Ty> {
    if let Some(param) = params.get(i)
        && !(expand_variadic && param.variadic)
    {
        return Some(param.ty.clone());
    }
    let last = params.last().filter(|p| expand_variadic && p.variadic)?;
    match &last.ty {
        Ty::Array { depth: 1, element } => Some(Ty::parse(element)),
        Ty::Array { depth, element } => Some(Ty::Array {
            depth: depth - 1,
            element: element.clone(),
        }),
        other => Some(other.clone()),
    }
}

fn applicable(params: &[Param], args: &[Option<Ty>], phase: Phase, variadic_call: bool) -> bool {
    let arity_ok = if variadic_call {
        params.last().is_some_and(|p| p.variadic) && args.len() + 1 >= params.len()
    } else {
        args.len() == params.len()
    };
    arity_ok
        && args.iter().enumerate().all(|(i, arg)| {
            let Some(param) = param_at(params, i, variadic_call) else {
                return false;
            };
            arg.as_ref()
                .is_none_or(|arg| convertible(arg, &param, phase))
        })
}

fn more_specific(a: &[Param], b: &[Param], arg_count: usize, variadic_call: bool) -> bool {
    let positions = if variadic_call {
        arg_count.max(a.len()).max(b.len())
    } else {
        arg_count
    };
    (0..positions).all(
        |i| match (param_at(a, i, variadic_call), param_at(b, i, variadic_call)) {
            (Some(x), Some(y)) => subtype(&x, &y),
            _ => false,
        },
    )
}

pub fn select_overloads(signatures: &[&str], args: &[ArgType]) -> Vec<usize> {
    let candidates: Vec<Vec<Param>> = signatures.iter().map(|s| parse_signature(s)).collect();
    let arg_types: Vec<Option<Ty>> = args
        .iter()
        .map(|a| match a {
            ArgType::Named(name) => Some(Ty::parse(name)),
            ArgType::Unknown => None,
        })
        .collect();
    let all_known = arg_types.iter().all(Option::is_some);

    let viable = |phase: Phase, variadic_call: bool| -> Vec<usize> {
        (0..candidates.len())
            .filter(|&i| applicable(&candidates[i], &arg_types, phase, variadic_call))
            .collect()
    };

    if !all_known {
        let mut kept: Vec<usize> = [(Phase::Loose, false), (Phase::Loose, true)]
            .into_iter()
            .flat_map(|(phase, variadic_call)| viable(phase, variadic_call))
            .collect();
        kept.sort_unstable();
        kept.dedup();
        return kept;
    }

    for (phase, variadic_call) in [
        (Phase::Strict, false),
        (Phase::Loose, false),
        (Phase::Loose, true),
    ] {
        let found = viable(phase, variadic_call);
        if found.is_empty() {
            continue;
        }
        return found
            .iter()
            .copied()
            .filter(|&i| {
                !found.iter().any(|&j| {
                    j != i
                        && more_specific(&candidates[j], &candidates[i], args.len(), variadic_call)
                        && !more_specific(&candidates[i], &candidates[j], args.len(), variadic_call)
                })
            })
            .collect();
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(types: &[&str]) -> Vec<ArgType> {
        types.iter().map(|t| ArgType::Named((*t).into())).collect()
    }

    fn pick(signatures: &[&str], args: &[ArgType]) -> Vec<usize> {
        select_overloads(signatures, args)
    }

    const CALC: [&str; 5] = [
        "(int, int)",
        "(double, double)",
        "(String, String)",
        "(Integer, Integer)",
        "(int...)",
    ];

    #[test]
    fn exact_primitive_match() {
        assert_eq!(pick(&CALC, &named(&["int", "int"])), [0]);
        assert_eq!(pick(&CALC, &named(&["double", "double"])), [1]);
        assert_eq!(pick(&CALC, &named(&["String", "String"])), [2]);
    }

    #[test]
    fn widening_beats_boxing() {
        assert_eq!(pick(&CALC, &named(&["int", "double"])), [1]);
    }

    #[test]
    fn boxing_applies_only_when_strict_phase_finds_nothing() {
        assert_eq!(pick(&CALC, &named(&["Integer", "Integer"])), [3]);
        assert_eq!(pick(&["(long)", "(Object)"], &named(&["int"])), [0]);
        assert_eq!(pick(&["(Object)", "(String)"], &named(&["int"])), [0]);
    }

    #[test]
    fn variadic_is_the_last_phase() {
        assert_eq!(pick(&CALC, &named(&["int", "int", "int"])), [4]);
        assert_eq!(pick(&CALC, &named(&[])), [4]);
        assert_eq!(pick(&CALC, &named(&["int", "int"])), [0]);
    }

    #[test]
    fn null_fits_any_reference_parameter() {
        assert_eq!(pick(&["(int)", "(String)"], &named(&["null"])), [1]);
    }

    #[test]
    fn most_specific_wins() {
        assert_eq!(pick(&["(Object)", "(String)"], &named(&["String"])), [1]);
        assert_eq!(pick(&["(long)", "(int)"], &named(&["int"])), [1]);
    }

    #[test]
    fn unknown_reference_relation_keeps_both() {
        assert_eq!(pick(&["(Base)", "(Other)"], &named(&["Sub"])), [0, 1]);
    }

    #[test]
    fn unknown_argument_keeps_every_arity_match() {
        let args = [ArgType::Unknown, ArgType::Unknown];
        assert_eq!(pick(&CALC, &args), [0, 1, 2, 3, 4]);
        let args = [ArgType::Named("String".into()), ArgType::Unknown];
        assert_eq!(pick(&CALC, &args), [2]);
    }

    #[test]
    fn nothing_applicable_returns_empty() {
        assert!(pick(&["(int)"], &named(&["String"])).is_empty());
        assert!(pick(&["(int)"], &named(&["int", "int"])).is_empty());
    }

    #[test]
    fn generics_and_packages_are_ignored() {
        assert_eq!(
            pick(&["(List)", "(String)"], &named(&["java.util.List<String>"])),
            [0]
        );
    }
}
