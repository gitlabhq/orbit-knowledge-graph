use std::path::Path;

const CONVENTION_EXTENSIONS: &[&str] = &[".ts", ".tsx", ".js", ".jsx"];

const CONVENTION_FILES: &[(&str, &str)] = &[
    ("page", "page"),
    ("route", "route"),
    ("layout", "layout"),
    ("default", "default"),
    ("not-found", "not-found"),
];

const HTTP_METHODS: &[&str] = &["GET", "HEAD", "OPTIONS", "POST", "PUT", "DELETE", "PATCH"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextjsRoute {
    pub file_path: String,
    pub route_path: String,
    pub framework_role: &'static str,
    pub http_methods: Vec<&'static str>,
    pub is_server_action: bool,
}

/// Detect Next.js routes from a list of file paths.
/// Checks for `app/` and `pages/` directories.
pub fn detect_nextjs_routes(file_paths: &[&str]) -> Vec<NextjsRoute> {
    file_paths
        .iter()
        .filter_map(|&fp| detect_single_route(fp))
        .collect()
}

fn detect_single_route(file_path: &str) -> Option<NextjsRoute> {
    let normalized = file_path.replace('\\', "/");

    let app_prefix = find_segment_prefix(&normalized, "app/")?;
    let relative = &normalized[app_prefix..];

    let (stem, framework_role) = match_convention_file(relative)?;

    let route_path = normalize_route_path(stem);

    Some(NextjsRoute {
        file_path: file_path.to_string(),
        route_path,
        framework_role,
        http_methods: Vec::new(),
        is_server_action: false,
    })
}

fn find_segment_prefix(path: &str, segment: &str) -> Option<usize> {
    // Find the last occurrence of the segment as a path boundary.
    let mut search_from = 0;
    let mut last_match = None;
    while let Some(pos) = path[search_from..].find(segment) {
        let abs = search_from + pos;
        if abs == 0 || path.as_bytes()[abs - 1] == b'/' {
            last_match = Some(abs + segment.len());
        }
        search_from = abs + 1;
    }
    last_match
}

fn match_convention_file(relative: &str) -> Option<(&str, &'static str)> {
    let file_name = Path::new(relative).file_name()?.to_str()?;

    for &(stem, role) in CONVENTION_FILES {
        for ext in CONVENTION_EXTENSIONS {
            let convention = format!("{stem}{ext}");
            if file_name == convention {
                let prefix_len = relative.len() - file_name.len();
                let dir_part = &relative[..prefix_len];
                return Some((dir_part, role));
            }
        }
    }
    None
}

fn normalize_route_path(dir_part: &str) -> String {
    let segments: Vec<&str> = dir_part
        .split('/')
        .filter(|s| !s.is_empty())
        .filter(|s| !is_route_group(s) && !is_parallel_slot(s))
        .collect();

    if segments.is_empty() {
        return "/".to_string();
    }

    format!("/{}", segments.join("/"))
}

fn is_route_group(segment: &str) -> bool {
    segment.starts_with('(') && segment.ends_with(')')
}

fn is_parallel_slot(segment: &str) -> bool {
    segment.starts_with('@')
}

/// Detect which HTTP methods a route handler exports.
/// `exported_bindings` should contain the names of all exported identifiers.
pub fn detect_http_methods(exported_bindings: &[&str]) -> Vec<&'static str> {
    HTTP_METHODS
        .iter()
        .copied()
        .filter(|&m| exported_bindings.contains(&m))
        .collect()
}

/// Returns `true` if the source starts with a `"use server"` directive
/// (first non-comment, non-whitespace statement).
pub fn is_server_action(source: &str) -> bool {
    let trimmed = skip_leading_comments(source);
    trimmed.starts_with("\"use server\"") || trimmed.starts_with("'use server'")
}

fn skip_leading_comments(source: &str) -> &str {
    let mut s = source.trim_start();
    loop {
        if s.starts_with("//") {
            match s.find('\n') {
                Some(pos) => s = s[pos + 1..].trim_start(),
                None => return "",
            }
        } else if s.starts_with("/*") {
            match s.find("*/") {
                Some(pos) => s = s[pos + 2..].trim_start(),
                None => return "",
            }
        } else {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_page_route() {
        let paths = vec!["app/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/");
        assert_eq!(routes[0].framework_role, "page");
    }

    #[test]
    fn test_nested_page_route() {
        let paths = vec!["app/dashboard/settings/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/dashboard/settings");
        assert_eq!(routes[0].framework_role, "page");
    }

    #[test]
    fn test_dynamic_segment() {
        let paths = vec!["app/users/[id]/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/users/[id]");
    }

    #[test]
    fn test_catch_all_segment() {
        let paths = vec!["app/docs/[...slug]/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/docs/[...slug]");
    }

    #[test]
    fn test_optional_catch_all() {
        let paths = vec!["app/shop/[[...categories]]/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/shop/[[...categories]]");
    }

    #[test]
    fn test_route_group_stripping() {
        let paths = vec!["app/(dashboard)/user/[id]/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/user/[id]");
    }

    #[test]
    fn test_nested_route_groups() {
        let paths = vec!["app/(marketing)/(auth)/login/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/login");
    }

    #[test]
    fn test_parallel_route_stripping() {
        let paths = vec!["app/@modal/login/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/login");
    }

    #[test]
    fn test_parallel_and_group_combined() {
        let paths = vec!["app/(shop)/@sidebar/categories/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/categories");
    }

    #[test]
    fn test_route_handler() {
        let paths = vec!["app/api/users/route.ts"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/api/users");
        assert_eq!(routes[0].framework_role, "route");
    }

    #[test]
    fn test_layout_file() {
        let paths = vec!["app/layout.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/");
        assert_eq!(routes[0].framework_role, "layout");
    }

    #[test]
    fn test_default_file() {
        let paths = vec!["app/@modal/default.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].framework_role, "default");
    }

    #[test]
    fn test_not_found_file() {
        let paths = vec!["app/not-found.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].framework_role, "not-found");
    }

    #[test]
    fn test_all_extensions() {
        for ext in &[".ts", ".tsx", ".js", ".jsx"] {
            let path_str = format!("app/page{ext}");
            let paths = vec![path_str.as_str()];
            let routes = detect_nextjs_routes(&paths);
            assert_eq!(routes.len(), 1, "Should match extension {ext}");
        }
    }

    #[test]
    fn test_non_convention_file_ignored() {
        let paths = vec![
            "app/components/button.tsx",
            "app/lib/utils.ts",
            "app/hooks/useAuth.ts",
        ];
        let routes = detect_nextjs_routes(&paths);
        assert!(routes.is_empty());
    }

    #[test]
    fn test_multiple_routes() {
        let paths = vec![
            "app/page.tsx",
            "app/about/page.tsx",
            "app/api/users/route.ts",
            "app/layout.tsx",
            "app/components/header.tsx",
        ];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 4);
    }

    #[test]
    fn test_src_app_prefix() {
        let paths = vec!["src/app/dashboard/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/dashboard");
    }

    #[test]
    fn test_detect_http_methods() {
        let bindings = vec!["GET", "POST", "handler", "middleware"];
        let methods = detect_http_methods(&bindings);
        assert_eq!(methods, vec!["GET", "POST"]);
    }

    #[test]
    fn test_detect_all_http_methods() {
        let bindings = vec!["GET", "HEAD", "OPTIONS", "POST", "PUT", "DELETE", "PATCH"];
        let methods = detect_http_methods(&bindings);
        assert_eq!(methods.len(), 7);
    }

    #[test]
    fn test_detect_no_http_methods() {
        let bindings: Vec<&str> = vec!["handler", "middleware", "config"];
        let methods = detect_http_methods(&bindings);
        assert!(methods.is_empty());
    }

    #[test]
    fn test_server_action_double_quotes() {
        assert!(is_server_action(
            "\"use server\"\n\nexport async fn doThing() {}"
        ));
    }

    #[test]
    fn test_server_action_single_quotes() {
        assert!(is_server_action(
            "'use server'\n\nexport async fn doThing() {}"
        ));
    }

    #[test]
    fn test_server_action_with_leading_comment() {
        let source = r#"// comment
/* multi
   line */
"use server"
"#;
        assert!(is_server_action(source));
    }

    #[test]
    fn test_not_server_action() {
        assert!(!is_server_action("export function handler() {}"));
    }

    #[test]
    fn test_not_server_action_use_client() {
        assert!(!is_server_action(
            "\"use client\"\n\nexport default function Page() {}"
        ));
    }

    #[test]
    fn test_server_action_with_whitespace() {
        assert!(is_server_action("  \n  \"use server\"\n"));
    }

    #[test]
    fn test_path_with_prefix() {
        let paths = vec!["packages/web/app/dashboard/page.tsx"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/dashboard");
    }

    #[test]
    fn test_api_dynamic_route() {
        let paths = vec!["app/api/users/[id]/route.ts"];
        let routes = detect_nextjs_routes(&paths);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route_path, "/api/users/[id]");
        assert_eq!(routes[0].framework_role, "route");
    }
}
