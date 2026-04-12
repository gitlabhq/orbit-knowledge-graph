#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsDirective {
    UseServer,
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
