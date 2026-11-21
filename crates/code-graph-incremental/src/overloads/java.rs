#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArgType {
    Named(String),
    Unknown,
}

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

    fn boxes_to_number(self) -> bool {
        !matches!(self, Self::Boolean | Self::Char)
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
        || (name == "Number" && primitive.boxes_to_number())
}

fn is_final_reference(name: &str) -> bool {
    name == "String" || Primitive::from_box(name).is_some()
}

#[derive(Debug)]
pub(super) struct Param {
    ty: Ty,
    variadic: bool,
}

pub(super) fn parse_signature(signature: &str) -> Vec<Param> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Fit {
    No,
    Maybe,
    Yes,
}

impl From<bool> for Fit {
    fn from(proven: bool) -> Self {
        if proven { Self::Yes } else { Self::No }
    }
}

fn convertible(arg: &Ty, param: &Ty, phase: Phase) -> Fit {
    let loose = phase == Phase::Loose;
    let box_supertype = |p: &str, a: Primitive| {
        if is_type_variable(p) {
            Fit::Maybe
        } else {
            is_box_supertype(p, a).into()
        }
    };
    match (arg, param) {
        (Ty::Primitive(a), Ty::Primitive(p)) => a.widens_to(*p).into(),
        (Ty::Primitive(a), Ty::Boxed(p)) => (loose && a == p).into(),
        (Ty::Primitive(a), Ty::Reference(p)) if loose => box_supertype(p, *a),
        (Ty::Primitive(_), _) => Fit::No,
        (Ty::Boxed(a), Ty::Primitive(p)) => (loose && a.widens_to(*p)).into(),
        (Ty::Boxed(a), Ty::Boxed(p)) => (a == p).into(),
        (Ty::Boxed(a), Ty::Reference(p)) => box_supertype(p, *a),
        (Ty::Boxed(_), _) => Fit::No,
        (Ty::Null, Ty::Primitive(_)) => Fit::No,
        (Ty::Null, _) => Fit::Yes,
        (Ty::Array { .. }, Ty::Primitive(_) | Ty::Boxed(_) | Ty::Null) => Fit::No,
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
            let reference_elements =
                Primitive::from_name(ae).is_none() && Primitive::from_name(pe).is_none();
            if ad > pd {
                return matches!(pe.as_str(), "Object" | "Cloneable" | "Serializable").into();
            }
            if ad < pd {
                return Fit::No;
            }
            if ae == pe || (reference_elements && pe == "Object") {
                Fit::Yes
            } else if reference_elements {
                Fit::Maybe
            } else {
                Fit::No
            }
        }
        (Ty::Array { .. }, Ty::Reference(p)) if is_type_variable(p) => Fit::Maybe,
        (Ty::Array { .. }, Ty::Reference(p)) => {
            matches!(p.as_str(), "Object" | "Cloneable" | "Serializable").into()
        }
        (Ty::Reference(_), Ty::Primitive(_) | Ty::Boxed(_) | Ty::Null) => Fit::No,
        (Ty::Reference(_), Ty::Array { .. }) => Fit::No,
        (Ty::Reference(a), Ty::Reference(p)) => {
            if a == p || p == "Object" {
                Fit::Yes
            } else if is_type_variable(p) {
                Fit::Maybe
            } else if a == "Object" {
                Fit::No
            } else if is_final_reference(a) {
                matches!(p.as_str(), "CharSequence" | "Comparable" | "Serializable").into()
            } else if is_final_reference(p) {
                Fit::No
            } else {
                Fit::Maybe
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
                || (name == "Number" && p.boxes_to_number())
        }
        (_, Ty::Reference(name)) => name == "Object" || is_type_variable(name),
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

fn applicable(params: &[Param], args: &[Option<Ty>], phase: Phase, variadic_call: bool) -> Fit {
    let arity_ok = if variadic_call {
        params.last().is_some_and(|p| p.variadic) && args.len() + 1 >= params.len()
    } else {
        args.len() == params.len()
    };
    if !arity_ok {
        return Fit::No;
    }
    let mut fit = Fit::Yes;
    for (i, arg) in args.iter().enumerate() {
        let Some(param) = param_at(params, i, variadic_call) else {
            return Fit::No;
        };
        let Some(arg) = arg else {
            fit = fit.min(Fit::Maybe);
            continue;
        };
        fit = fit.min(convertible(arg, &param, phase));
        if fit == Fit::No {
            return Fit::No;
        }
    }
    fit
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

#[cfg(test)]
fn select_overloads(signatures: &[&str], args: &[ArgType]) -> Vec<usize> {
    let candidates: Vec<Vec<Param>> = signatures.iter().map(|s| parse_signature(s)).collect();
    select_parsed(&candidates, args)
}

pub(super) fn select_parsed(candidates: &[Vec<Param>], args: &[ArgType]) -> Vec<usize> {
    let arg_types: Vec<Option<Ty>> = args
        .iter()
        .map(|a| match a {
            ArgType::Named(name) => Some(Ty::parse(name)),
            ArgType::Unknown => None,
        })
        .collect();
    let all_known = arg_types.iter().all(Option::is_some);

    let viable = |phase: Phase, variadic_call: bool| -> Vec<(usize, Fit)> {
        (0..candidates.len())
            .filter_map(
                |i| match applicable(&candidates[i], &arg_types, phase, variadic_call) {
                    Fit::No => None,
                    fit => Some((i, fit)),
                },
            )
            .collect()
    };

    if !all_known {
        let mut kept: Vec<usize> = [(Phase::Loose, false), (Phase::Loose, true)]
            .into_iter()
            .flat_map(|(phase, variadic_call)| viable(phase, variadic_call))
            .map(|(i, _)| i)
            .collect();
        kept.sort_unstable();
        kept.dedup();
        return kept;
    }

    let mut possible_only: Vec<usize> = Vec::new();
    for (phase, variadic_call) in [
        (Phase::Strict, false),
        (Phase::Loose, false),
        (Phase::Loose, true),
    ] {
        let found = viable(phase, variadic_call);
        if found.is_empty() {
            continue;
        }
        let mut viable_so_far = possible_only.clone();
        viable_so_far.extend(found.iter().map(|&(i, _)| i));
        viable_so_far.sort_unstable();
        viable_so_far.dedup();
        if viable_so_far.len() > super::MAX_FAN_OUT {
            return viable_so_far;
        }
        if found.iter().all(|&(_, fit)| fit != Fit::Yes) {
            possible_only = viable_so_far;
            continue;
        }
        let beats = |winner: usize, loser: usize| {
            more_specific(
                &candidates[winner],
                &candidates[loser],
                args.len(),
                variadic_call,
            ) && !more_specific(
                &candidates[loser],
                &candidates[winner],
                args.len(),
                variadic_call,
            )
        };
        let mut selected: Vec<usize> = found
            .iter()
            .filter(|&&(i, _)| {
                !found
                    .iter()
                    .any(|&(j, fit)| j != i && fit == Fit::Yes && beats(j, i))
            })
            .map(|&(i, _)| i)
            .collect();
        selected.extend(possible_only);
        selected.sort_unstable();
        selected.dedup();
        return selected;
    }
    possible_only
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
    fn an_object_argument_reaches_only_object() {
        assert_eq!(pick(&["(Object)", "(Foo)"], &named(&["Object"])), [0]);
        assert_eq!(pick(&["(Foo)"], &named(&["Object"])), Vec::<usize>::new());
    }

    #[test]
    fn a_possibly_applicable_overload_does_not_hide_a_certain_one() {
        assert_eq!(pick(&["(Foo)", "(Object)"], &named(&["Widget"])), [0, 1]);
        assert_eq!(pick(&["(Foo)", "(Object)"], &named(&["Foo"])), [0]);
    }

    #[test]
    fn a_short_class_name_keeps_the_exact_match_alongside_the_rest() {
        assert_eq!(pick(&["(IO)", "(Foo)"], &named(&["IO"])), [0, 1]);
    }

    #[test]
    fn possible_only_phases_carry_into_the_next_certain_one() {
        assert_eq!(pick(&["(Foo)", "(Object...)"], &named(&["Widget"])), [0, 1]);
        assert_eq!(pick(&["(IO)", "(int)"], &named(&["Integer"])), [0, 1]);
    }

    #[test]
    fn overflow_counts_distinct_candidates_and_keeps_carried_ones() {
        let mut signatures: Vec<String> = (0..5).map(|_| "(AlphaI)".to_string()).collect();
        signatures.extend((0..23).map(|_| "(BetaI...)".to_string()));
        let signatures: Vec<&str> = signatures.iter().map(String::as_str).collect();
        let picked = pick(&signatures, &named(&["Widget"]));
        assert_eq!(picked, (0..28).collect::<Vec<_>>());
    }

    #[test]
    fn a_type_variable_parameter_is_only_possible_for_arrays_and_references() {
        assert_eq!(pick(&["(IO)", "(Foo[]...)"], &named(&["Foo[]"])), [0, 1]);
        assert_eq!(pick(&["(IO)", "(Foo...)"], &named(&["Foo"])), [0, 1]);
    }

    #[test]
    fn overflow_after_a_carried_phase_keeps_every_viable_overload() {
        let mut signatures: Vec<String> = (0..17).map(|_| "(AlphaI)".to_string()).collect();
        signatures.extend((0..16).map(|_| "(BetaI...)".to_string()));
        let signatures: Vec<&str> = signatures.iter().map(String::as_str).collect();
        let picked = pick(&signatures, &named(&["Widget"]));
        assert_eq!(picked, (0..33).collect::<Vec<_>>());
    }

    #[test]
    fn char_is_not_a_number() {
        assert_eq!(pick(&["(Number)", "(Object)"], &named(&["char"])), [1]);
        assert_eq!(pick(&["(Number)", "(Object)"], &named(&["Character"])), [1]);
        assert_eq!(pick(&["(Number)", "(Object)"], &named(&["Integer"])), [0]);
        assert_eq!(pick(&["(int)", "(long)"], &named(&["char"])), [0]);
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
    fn arrays_are_not_their_elements() {
        assert_eq!(
            pick(&["(String)", "(String[])"], &named(&["String[]"])),
            [1]
        );
        assert_eq!(
            pick(&["(Object)", "(Object[])"], &named(&["String[][]"])),
            [1]
        );
    }

    #[test]
    fn type_variable_parameter_is_as_general_as_object() {
        assert_eq!(pick(&["(T)", "(String)"], &named(&["String"])), [1]);
    }

    #[test]
    fn generics_and_packages_are_ignored() {
        assert_eq!(
            pick(&["(List)", "(String)"], &named(&["java.util.List<String>"])),
            [0]
        );
    }
}
