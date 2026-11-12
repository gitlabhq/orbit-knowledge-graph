import json
import os
import re
import sys

import tree_sitter_java as tsj
from tree_sitter import Language, Parser

PARSER = Parser(Language(tsj.language()))
SKIP_FIELDS = {"serialVersionUID", "serialPersistentFields"}
SERIAL_METHODS = {"readObject", "writeObject", "readResolve", "writeReplace", "readObjectNoData"}
SUPPRESS_ALL = {"unused", "all"}
LOMBOK_CONSTRUCTORS = {"RequiredArgsConstructor", "AllArgsConstructor", "lombok.RequiredArgsConstructor", "lombok.AllArgsConstructor"}
LOMBOK_READERS = {"Getter", "Data", "Value", "ToString", "EqualsAndHashCode", "lombok.Getter", "lombok.Data", "lombok.Value", "lombok.ToString", "lombok.EqualsAndHashCode"}
TYPE_BODIES = {"class_body", "enum_body", "interface_body", "record_declaration_body", "annotation_type_body"}
TYPE_DECLS = {"class_declaration", "enum_declaration", "interface_declaration", "record_declaration", "annotation_type_declaration"}
SCOPES = {"block", "method_declaration", "constructor_declaration", "lambda_expression", "for_statement",
          "enhanced_for_statement", "catch_clause", "try_with_resources_statement", "switch_block_statement_group",
          "switch_rule", "compact_constructor_declaration"}
LITERAL_TYPES = {
    "decimal_integer_literal": "int", "hex_integer_literal": "int", "octal_integer_literal": "int",
    "binary_integer_literal": "int", "decimal_floating_point_literal": "double", "string_literal": "String",
    "character_literal": "char", "true": "boolean", "false": "boolean", "text_block": "String",
}
NUMERIC = {"int", "long", "short", "byte", "double", "float", "Integer", "Long", "Short", "Byte", "Double", "Float", "Number"}


def text(node, src):
    return src[node.start_byte:node.end_byte].decode("utf-8", "replace")


def walk(node):
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        stack.extend(reversed(n.children))


def annotations_of(node, src):
    out = []
    for child in node.children:
        if child.type == "modifiers":
            for c in child.children:
                if c.type in ("annotation", "marker_annotation"):
                    n = c.child_by_field_name("name")
                    args = c.child_by_field_name("arguments")
                    out.append((text(n, src) if n is not None else "", text(args, src) if args is not None else ""))
    return out


def suppressed(node, src, rule):
    while node is not None:
        if node.type in TYPE_DECLS or node.type in ("field_declaration", "method_declaration", "constructor_declaration"):
            for name, args in annotations_of(node, src):
                if name.split(".")[-1] != "SuppressWarnings":
                    continue
                keys = set(re.findall(r'"([^"]+)"', args))
                if keys & SUPPRESS_ALL or rule in keys or rule.split(":")[-1] in keys:
                    return True
        node = node.parent
    return False


def lombok_reads(node, src):
    owner = node.parent
    while owner is not None and owner.type not in TYPE_DECLS:
        owner = owner.parent
    if owner is None:
        return False
    if owner.type == "record_declaration":
        return True
    owner_annotations = {name for name, _ in annotations_of(owner, src)}
    if owner_annotations & LOMBOK_READERS:
        return True
    if owner_annotations & LOMBOK_CONSTRUCTORS:
        return True
    return any(name in LOMBOK_READERS for name, _ in annotations_of(node, src))


def name_referenced_in_strings(tree, src, name, method):
    root = top_class(method) or tree.root_node
    for n in walk(root):
        if n.type == "annotation":
            args = n.child_by_field_name("arguments")
            if args is not None and re.search(r'"' + re.escape(name) + r'"', text(args, src)):
                return True
    return False


def modifiers(node, src):
    for child in node.children:
        if child.type == "modifiers":
            words = set(text(child, src).split())
            annotated = any(c.type in ("annotation", "marker_annotation") for c in child.children)
            return words, annotated
    return set(), False


def top_class(node):
    top = None
    while node is not None:
        if node.type in TYPE_DECLS:
            top = node
        node = node.parent
    return top


def declares(scope, name, src):
    if scope.type in ("method_declaration", "constructor_declaration", "lambda_expression", "compact_constructor_declaration"):
        params = scope.child_by_field_name("parameters")
        if params is not None:
            if params.type == "identifier":
                return text(params, src) == name
            for p in walk(params):
                if p.type in ("formal_parameter", "spread_parameter", "inferred_parameters") or p.parent == params:
                    n = p.child_by_field_name("name")
                    if n is not None and text(n, src) == name:
                        return True
                    if p.type == "identifier" and p.parent is params and text(p, src) == name:
                        return True
    if scope.type == "catch_clause":
        for p in walk(scope):
            if p.type == "catch_formal_parameter":
                n = p.child_by_field_name("name")
                if n is not None and text(n, src) == name:
                    return True
    if scope.type == "enhanced_for_statement":
        n = scope.child_by_field_name("name")
        if n is not None and text(n, src) == name:
            return True
    for child in scope.children:
        target = child
        if child.type in ("local_variable_declaration", "resource"):
            for d in walk(child):
                if d.type == "variable_declarator" or d.type == "resource":
                    n = d.child_by_field_name("name")
                    if n is not None and text(n, src) == name:
                        return True
        if scope.type in ("for_statement",) and child.type == "local_variable_declaration":
            continue
        if child.type == "resource_specification":
            for d in walk(child):
                if d.type == "resource":
                    n = d.child_by_field_name("name")
                    if n is not None and text(n, src) == name:
                        return True
    return False


def shadowed(ident, name, src, owner_body):
    node = ident.parent
    while node is not None and node != owner_body:
        if node.type in SCOPES and declares(node, name, src):
            return True
        if node.type in TYPE_BODIES:
            for member in node.children:
                if member.type == "field_declaration":
                    for d in member.children:
                        if d.type == "variable_declarator":
                            n = d.child_by_field_name("name")
                            if n is not None and text(n, src) == name and node != owner_body:
                                return True
        node = node.parent
    return False


def is_write_only(ident, src):
    node = ident
    parent = node.parent
    if parent.type == "field_access":
        if parent.child_by_field_name("field") != node:
            return False
        obj = parent.child_by_field_name("object")
        node = parent
        parent = node.parent
    if parent.type == "assignment_expression" and parent.child_by_field_name("left") == node:
        op = text(parent, src)[node.end_byte - parent.start_byte:].lstrip()[:2]
        return op.startswith("=") and not op.startswith("==")
    return False


def field_reads(src, tree, field_node, name, owner_body):
    root = top_class(field_node) or tree.root_node
    for n in walk(root):
        if n.type != "identifier" or text(n, src) != name:
            continue
        p = n.parent
        if p is None:
            continue
        if p.type == "variable_declarator" and p.child_by_field_name("name") == n:
            continue
        if p.type in ("method_declaration", "method_invocation") and p.child_by_field_name("name") == n:
            continue
        if p.type in ("formal_parameter", "catch_formal_parameter", "enhanced_for_statement") and p.child_by_field_name("name") == n:
            continue
        if p.type in TYPE_DECLS or p.type in ("constructor_declaration", "enum_constant", "labeled_statement", "break_statement", "continue_statement"):
            continue
        if p.type == "inferred_parameters" or (p.type == "lambda_expression" and p.child_by_field_name("parameters") == n):
            continue
        if p.type in ("scoped_identifier", "scoped_type_identifier", "type_identifier", "import_declaration", "package_declaration"):
            continue
        if p.type == "method_reference":
            parts = [c for c in p.children if c.is_named]
            if parts and parts[0] == n:
                return True
            continue
        if p.type == "field_access" and p.child_by_field_name("field") == n:
            obj = p.child_by_field_name("object")
            if obj is not None and obj.type == "this":
                enclosing = p.parent
                while enclosing is not None and enclosing.type not in TYPE_BODIES:
                    enclosing = enclosing.parent
                if enclosing is not None and enclosing != owner_body:
                    inner = enclosing
                    declares_own = False
                    while inner is not None and inner != owner_body:
                        if inner.type in TYPE_BODIES and any(
                            m.type == "field_declaration" and any(
                                d.type == "variable_declarator" and text(d.child_by_field_name("name"), src) == name
                                for d in m.children
                            )
                            for m in inner.children
                        ):
                            declares_own = True
                            break
                        inner = inner.parent
                    if declares_own:
                        continue
        elif shadowed(n, name, src, owner_body):
            continue
        if is_write_only(n, src):
            continue
        return True
    return False


def split_args(arg_list):
    return [c for c in arg_list.children if c.is_named and c.type not in ("line_comment", "block_comment")]


def declared_type(ident, src):
    name = text(ident, src)
    node = ident.parent
    while node is not None:
        candidates = []
        if node.type in ("method_declaration", "constructor_declaration", "lambda_expression"):
            params = node.child_by_field_name("parameters")
            if params is not None:
                candidates += [c for c in params.children if c.type in ("formal_parameter", "spread_parameter")]
        if node.type == "catch_clause":
            candidates += [c for c in walk(node) if c.type == "catch_formal_parameter"]
        if node.type == "enhanced_for_statement":
            t = node.child_by_field_name("type"); n = node.child_by_field_name("name")
            if n is not None and text(n, src) == name and t is not None:
                return text(t, src)
        for child in node.children:
            if child.type in ("local_variable_declaration", "field_declaration") and child.start_byte < ident.start_byte or child.type == "field_declaration":
                candidates.append(child)
        for c in candidates:
            t = c.child_by_field_name("type")
            if c.type in ("formal_parameter", "spread_parameter", "catch_formal_parameter"):
                n = c.child_by_field_name("name")
                if n is not None and text(n, src) == name and t is not None:
                    return text(t, src)
                continue
            for d in c.children:
                if d.type == "variable_declarator":
                    n = d.child_by_field_name("name")
                    if n is not None and text(n, src) == name and t is not None:
                        ty = text(t, src)
                        return None if ty == "var" else ty
        node = node.parent
    return None


BOXED = {"Integer": "int", "Long": "long", "Short": "short", "Byte": "byte", "Double": "double", "Float": "float", "Boolean": "boolean", "Character": "char"}


def expr_type(node, src):
    t = LITERAL_TYPES.get(node.type)
    if t:
        return t
    if node.type == "parenthesized_expression":
        inner = [c for c in node.children if c.is_named]
        return expr_type(inner[0], src) if inner else None
    if node.type in ("update_expression", "unary_expression"):
        inner = [c for c in node.children if c.is_named]
        return expr_type(inner[0], src) if inner else None
    if node.type == "cast_expression":
        t = node.child_by_field_name("type")
        return text(t, src) if t is not None else None
    if node.type == "object_creation_expression":
        t = node.child_by_field_name("type")
        return re.sub(r"<.*", "", text(t, src)) if t is not None else None
    if node.type == "identifier":
        t = declared_type(node, src)
        return re.sub(r"<.*", "", t) if t else None
    if node.type == "binary_expression":
        op = text(node, src)[node.child_by_field_name("left").end_byte - node.start_byte:node.child_by_field_name("right").start_byte - node.start_byte].strip()
        if op in ("==", "!=", "<", ">", "<=", ">=", "&&", "||", "instanceof"):
            return "boolean"
        left = expr_type(node.child_by_field_name("left"), src)
        right = expr_type(node.child_by_field_name("right"), src)
        if op == "+" and "String" in (left, right):
            return "String"
    return None


PRIMITIVE = {"int", "long", "short", "byte", "double", "float", "boolean", "char"}


def literal_type(node):
    return LITERAL_TYPES.get(node.type)


def param_types(params, src):
    out = []
    spread = False
    for p in params.children:
        if p.type == "formal_parameter":
            t = p.child_by_field_name("type")
            out.append(text(t, src) if t is not None else None)
        elif p.type == "spread_parameter":
            spread = True
            out.append(None)
    return out, spread


def compatible(param, arg):
    if arg is None or param is None:
        return True
    param = re.sub(r"<.*", "", param).strip()
    arg = BOXED.get(arg, arg) if arg in BOXED and param in PRIMITIVE else arg
    if arg in PRIMITIVE and BOXED.get(param) == arg:
        return True
    if arg not in PRIMITIVE and arg not in ("String",) and param not in PRIMITIVE:
        return True
    if param == arg or param in ("Object",) or param.startswith(("T", "E", "K", "V")) and len(param) == 1:
        return True
    if arg == "int" and param in NUMERIC | {"char"}:
        return True
    if arg == "double" and param in ("double", "float", "Double", "Float", "Number"):
        return True
    if arg == "String" and param in ("String", "CharSequence", "Comparable<String>"):
        return True
    if arg == "boolean" and param in ("boolean", "Boolean"):
        return True
    if arg == "char" and param in ("char", "Character", "int", "long"):
        return True
    return False


def call_matches(args, types, spread, src):
    if spread:
        return len(args) >= len(types) - 1
    if len(args) != len(types):
        return False
    return all(compatible(t, expr_type(a, src)) for t, a in zip(types, args))


def method_used(src, tree, method, name, is_ctor, owner_name):
    root = top_class(method) or tree.root_node
    params = method.child_by_field_name("parameters")
    types, spread = param_types(params, src) if params is not None else ([], False)
    for n in walk(root):
        if n == method:
            continue
        if is_ctor:
            if n.type == "object_creation_expression":
                t = n.child_by_field_name("type")
                if t is None or re.sub(r"<.*", "", text(t, src)).split(".")[-1] != owner_name:
                    continue
                args = n.child_by_field_name("arguments")
                if call_matches(split_args(args), types, spread, src):
                    return True
            elif n.type == "explicit_constructor_invocation":
                ctor = n.child_by_field_name("constructor")
                if ctor is None:
                    continue
                target = None
                if ctor.type == "this":
                    target = owner_name
                elif ctor.type == "super":
                    sub = n.parent
                    while sub is not None and sub.type not in TYPE_DECLS:
                        sub = sub.parent
                    sup = sub.child_by_field_name("superclass") if sub is not None else None
                    if sup is not None:
                        target = re.sub(r"<.*", "", text(sup, src).replace("extends", "").strip()).split(".")[-1]
                if target != owner_name:
                    continue
                args = n.child_by_field_name("arguments")
                if call_matches(split_args(args), types, spread, src):
                    return True
        else:
            if n.type == "method_invocation":
                nm = n.child_by_field_name("name")
                if nm is None or text(nm, src) != name:
                    continue
                if n.start_byte >= method.start_byte and n.end_byte <= method.end_byte:
                    continue
                args = n.child_by_field_name("arguments")
                if call_matches(split_args(args), types, spread, src):
                    return True
            elif n.type == "method_reference":
                if text(n, src).rsplit("::", 1)[-1].strip() == name:
                    return True
    return False


def analyze(path, rel):
    src = open(path, "rb").read()
    tree = PARSER.parse(src)
    findings = []
    for node in walk(tree.root_node):
        if node.type == "field_declaration":
            mods, annotated = modifiers(node, src)
            if "private" not in mods or annotated:
                continue
            body = node.parent
            for d in node.children:
                if d.type != "variable_declarator":
                    continue
                name_node = d.child_by_field_name("name")
                name = text(name_node, src)
                if name in SKIP_FIELDS:
                    continue
                if name == "logger" and {"static", "final"} <= mods:
                    continue
                if suppressed(node, src, "java:S1068") or lombok_reads(node, src):
                    continue
                if not field_reads(src, tree, node, name, body):
                    findings.append({"rule": "java:S1068", "file": rel, "line": name_node.start_point[0] + 1, "name": name})
        elif node.type in ("method_declaration", "constructor_declaration"):
            mods, annotated = modifiers(node, src)
            if "private" not in mods or annotated:
                continue
            name_node = node.child_by_field_name("name")
            name = text(name_node, src)
            is_ctor = node.type == "constructor_declaration"
            owner = node.parent
            while owner is not None and owner.type not in TYPE_DECLS:
                owner = owner.parent
            owner_name = text(owner.child_by_field_name("name"), src) if owner is not None and owner.child_by_field_name("name") is not None else ""
            if is_ctor:
                params = node.child_by_field_name("parameters")
                if params is not None and not [p for p in params.children if p.is_named]:
                    continue
                if owner is not None and owner.type == "enum_declaration":
                    continue
            elif name in SERIAL_METHODS:
                continue
            if suppressed(node, src, "java:S1144"):
                continue
            if name_referenced_in_strings(tree, src, name, node):
                continue
            if not method_used(src, tree, node, name, is_ctor, owner_name):
                findings.append({"rule": "java:S1144", "file": rel, "line": node.start_point[0] + 1, "name": name})
    return findings


def glob_regex(pattern):
    out = ""
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out += "(?:.*/)?"; i += 3
        elif pattern.startswith("**", i):
            out += ".*"; i += 2
        elif pattern[i] == "*":
            out += "[^/]*"; i += 1
        elif pattern[i] == "?":
            out += "[^/]"; i += 1
        else:
            out += re.escape(pattern[i]); i += 1
    return re.compile("^" + out + "$")


def load_ignores(path):
    if not path:
        return []
    out = []
    for rule in json.load(open(path)):
        out.append((glob_regex(rule["resourceKey"]), glob_regex(rule["ruleKey"].replace(":", "\\:")) if False else re.compile("^" + re.escape(rule["ruleKey"]).replace("\\*", ".*") + "$")))
    return out


def main():
    repo = sys.argv[1]
    ignores = load_ignores(sys.argv[2] if len(sys.argv) > 2 else None)
    out = []
    for dirpath, dirs, files in os.walk(repo):
        dirs[:] = [d for d in dirs if d not in (".git", "target", "node_modules", "build")]
        for f in files:
            if not f.endswith(".java"):
                continue
            path = os.path.join(dirpath, f)
            rel = os.path.relpath(path, repo)
            for finding in analyze(path, rel):
                if any(res.match(rel) and rule.match(finding["rule"]) for res, rule in ignores):
                    continue
                out.append(finding)
    json.dump(out, sys.stdout, indent=1)


main()
