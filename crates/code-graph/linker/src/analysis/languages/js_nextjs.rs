/// The directive found in a JS/TS file, if any.
/// Detected from OXC's parsed `Program.directives` (standard ECMAScript directive prologues).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsDirective {
    /// `"use server"` -- marks the file as a React Server Action module
    UseServer,
    /// `"use client"` -- marks the file as a React Client Component module
    UseClient,
}

impl JsDirective {
    pub fn as_str(&self) -> &'static str {
        match self {
            JsDirective::UseServer => "use server",
            JsDirective::UseClient => "use client",
        }
    }
}

/// Extract the React directive from OXC's parsed program directives.
/// Returns `Some(JsDirective)` if `"use server"` or `"use client"` is present.
///
/// This reads directly from OXC's `Program.directives` -- the parser already
/// handles these as standard ECMAScript directive prologues.
pub fn detect_directive<'a>(
    directives: impl IntoIterator<Item = &'a oxc::ast::ast::Directive<'a>>,
) -> Option<JsDirective> {
    for directive in directives {
        match directive.directive.as_str() {
            "use server" => return Some(JsDirective::UseServer),
            "use client" => return Some(JsDirective::UseClient),
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxc::allocator::Allocator;
    use oxc::parser::Parser;
    use oxc::span::SourceType;

    fn parse_directive(source: &str) -> Option<JsDirective> {
        let allocator = Allocator::default();
        let source_type = SourceType::ts();
        let parsed = Parser::new(&allocator, source, source_type).parse();
        detect_directive(&parsed.program.directives)
    }

    #[test]
    fn test_use_server() {
        assert_eq!(
            parse_directive("\"use server\";\nexport async function doThing() {}"),
            Some(JsDirective::UseServer)
        );
    }

    #[test]
    fn test_use_client() {
        assert_eq!(
            parse_directive("\"use client\";\nexport default function Page() {}"),
            Some(JsDirective::UseClient)
        );
    }

    #[test]
    fn test_single_quotes() {
        assert_eq!(
            parse_directive("'use server';\nexport async function doThing() {}"),
            Some(JsDirective::UseServer)
        );
    }

    #[test]
    fn test_no_directive() {
        assert_eq!(parse_directive("export default function Page() {}"), None);
    }

    #[test]
    fn test_use_strict_not_matched() {
        assert_eq!(parse_directive("\"use strict\";\nconst x = 1;"), None);
    }

    #[test]
    fn test_directive_after_comment() {
        // Directives must be before any statements, but comments are fine
        assert_eq!(
            parse_directive("// some comment\n\"use server\";\nexport async function f() {}"),
            Some(JsDirective::UseServer)
        );
    }
}
