use crate::v2::linker::MAX_SELECTED_OVERLOADS;
use crate::v2::types::ssa::ArgType;
use rustc_hash::FxHashMap;
use std::cell::RefCell;
use std::rc::Rc;

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

thread_local! {
    static PARSED_SIGNATURES: RefCell<FxHashMap<Box<str>, Rc<[Param]>>> =
        RefCell::new(FxHashMap::default());
}

const PARSED_SIGNATURE_CACHE_LIMIT: usize = 100_000;

fn parse_signature_cached(signature: &str) -> Rc<[Param]> {
    PARSED_SIGNATURES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(parsed) = cache.get(signature) {
            return Rc::clone(parsed);
        }
        if cache.len() >= PARSED_SIGNATURE_CACHE_LIMIT {
            cache.clear();
        }
        let parsed: Rc<[Param]> = parse_signature(signature).into();
        cache.insert(signature.into(), Rc::clone(&parsed));
        parsed
    })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Fit {
    No,
    Possible,
    Proven,
}

impl Fit {
    fn of(fits: bool) -> Self {
        if fits { Self::Proven } else { Self::No }
    }

    fn possible_if(fits: bool) -> Self {
        if fits { Self::Possible } else { Self::No }
    }
}

fn convertible(arg: &Ty, param: &Ty, phase: Phase) -> Fit {
    let loose = phase == Phase::Loose;
    let box_supertype = |p: &str, a: Primitive| {
        if is_type_variable(p) {
            Fit::Possible
        } else {
            Fit::of(is_box_supertype(p, a))
        }
    };
    match (arg, param) {
        (Ty::Primitive(a), Ty::Primitive(p)) => Fit::of(a.widens_to(*p)),
        (Ty::Primitive(a), Ty::Boxed(p)) => Fit::of(loose && a == p),
        (Ty::Primitive(a), Ty::Reference(p)) if loose => box_supertype(p, *a),
        (Ty::Primitive(_), _) => Fit::No,
        (Ty::Boxed(a), Ty::Primitive(p)) => Fit::of(loose && a.widens_to(*p)),
        (Ty::Boxed(a), Ty::Boxed(p)) => Fit::of(a == p),
        (Ty::Boxed(a), Ty::Reference(p)) => box_supertype(p, *a),
        (Ty::Boxed(_), _) => Fit::No,
        (Ty::Null, Ty::Primitive(_)) => Fit::No,
        (Ty::Null, _) => Fit::Proven,
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
            if ad == pd && (ae == pe || pe == "Object" && reference_elements)
                || ad > pd && matches!(pe.as_str(), "Object" | "Cloneable" | "Serializable")
            {
                Fit::Proven
            } else {
                Fit::possible_if(ad == pd && reference_elements)
            }
        }
        (Ty::Array { .. }, Ty::Reference(p)) => {
            if is_type_variable(p) {
                Fit::Possible
            } else {
                Fit::of(matches!(
                    p.as_str(),
                    "Object" | "Cloneable" | "Serializable"
                ))
            }
        }
        (Ty::Reference(_), Ty::Primitive(_) | Ty::Boxed(_) | Ty::Null) => Fit::No,
        (Ty::Reference(_), Ty::Array { .. }) => Fit::No,
        (Ty::Reference(a), Ty::Reference(p)) => {
            if a == p || p == "Object" {
                Fit::Proven
            } else if a == "Object" {
                Fit::possible_if(is_type_variable(p))
            } else if is_type_variable(p) {
                Fit::Possible
            } else if is_final_reference(a) {
                Fit::of(matches!(
                    p.as_str(),
                    "CharSequence" | "Comparable" | "Serializable"
                ))
            } else {
                Fit::possible_if(!is_final_reference(p))
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
        (Ty::Reference(_) | Ty::Array { .. }, Ty::Reference(_) | Ty::Array { .. }) => {
            convertible(sub, sup, Phase::Strict) == Fit::Proven
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

fn applicable(
    params: &[Param],
    args: &[Option<Ty>],
    phase: Phase,
    variadic_call: bool,
) -> Option<bool> {
    let arity_ok = if variadic_call {
        params.last().is_some_and(|p| p.variadic) && args.len() + 1 >= params.len()
    } else {
        args.len() == params.len()
    };
    if !arity_ok {
        return None;
    }
    let mut proven = true;
    for (i, arg) in args.iter().enumerate() {
        let param = param_at(params, i, variadic_call)?;
        let Some(arg) = arg else { continue };
        match convertible(arg, &param, phase) {
            Fit::No => return None,
            Fit::Possible => proven = false,
            Fit::Proven => {}
        }
    }
    Some(proven)
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
    let candidates: Vec<Rc<[Param]>> = signatures
        .iter()
        .map(|s| parse_signature_cached(s))
        .collect();
    let arg_types: Vec<Option<Ty>> = args
        .iter()
        .map(|a| match a {
            ArgType::Named(name) => Some(Ty::parse(name)),
            ArgType::Unknown => None,
        })
        .collect();
    let all_known = arg_types.iter().all(Option::is_some);

    let viable = |phase: Phase, variadic_call: bool| -> Vec<(usize, bool)> {
        (0..candidates.len())
            .filter_map(|i| {
                applicable(&candidates[i], &arg_types, phase, variadic_call)
                    .map(|proven| (i, proven))
            })
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

    let args_len = args.len();
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
        if viable_so_far.len() > MAX_SELECTED_OVERLOADS {
            return viable_so_far;
        }
        if found.iter().all(|&(_, proven)| !proven) {
            possible_only = viable_so_far;
            continue;
        }
        let mut selected: Vec<usize> = found
            .iter()
            .filter(|&&(i, _)| {
                !found.iter().any(|&(j, j_proven)| {
                    j != i
                        && j_proven
                        && more_specific(&candidates[j], &candidates[i], args_len, variadic_call)
                        && !more_specific(&candidates[i], &candidates[j], args_len, variadic_call)
                })
            })
            .map(|&(i, _)| i)
            .collect();
        selected.extend(possible_only);
        selected.sort_unstable();
        selected.dedup();
        return selected;
    }
    possible_only.sort_unstable();
    possible_only.dedup();
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
    fn generics_and_packages_are_ignored() {
        assert_eq!(
            pick(&["(List)", "(String)"], &named(&["java.util.List<String>"])),
            [0]
        );
    }

    #[test]
    fn object_argument_reaches_only_object_parameter() {
        assert_eq!(pick(&["(Object)", "(Foo)"], &named(&["Object"])), [0]);
    }

    #[test]
    fn possible_overload_does_not_eliminate_a_proven_one() {
        assert_eq!(pick(&["(Foo)", "(Object)"], &named(&["Widget"])), [0, 1]);
        assert_eq!(pick(&["(Foo)", "(Object)"], &named(&["Foo"])), [0]);
    }

    #[test]
    fn short_class_names_are_kept_next_to_other_overloads() {
        assert_eq!(pick(&["(IO)", "(Foo)"], &named(&["IO"])), [0, 1]);
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
    fn most_specific_uses_the_supertypes_the_fit_check_proves() {
        assert_eq!(
            pick(&["(String)", "(CharSequence)"], &named(&["String"])),
            [0]
        );
        assert_eq!(
            pick(&["(String)", "(Comparable)"], &named(&["String"])),
            [0]
        );
        assert_eq!(
            pick(&["(String[])", "(Object[])"], &named(&["String[]"])),
            [0]
        );
        assert_eq!(
            pick(&["(Integer)", "(Comparable)"], &named(&["Integer"])),
            [0]
        );
    }

    #[test]
    fn most_specific_keeps_both_when_the_relation_is_only_possible() {
        assert_eq!(pick(&["(Foo)", "(Runnable)"], &named(&["Foo"])), [0, 1]);
        assert_eq!(pick(&["(Widget)", "(Runnable)"], &named(&["Foo"])), [0, 1]);
    }

    #[test]
    fn type_variable_shaped_parameter_is_not_a_provable_supertype() {
        assert_eq!(pick(&["(T)", "(String)"], &named(&["String"])), [0, 1]);
        assert_eq!(
            pick(&["(IO)", "(Foo)"], &[ArgType::Named("null".into())]),
            [0, 1]
        );
    }
}
