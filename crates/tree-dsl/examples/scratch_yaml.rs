use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{DEAD, Lang, NAMED, NONE, SYNTH};
use tree_dsl::rules;
use tree_dsl::tree::Tree;

fn dump(tree: &Tree, lang: &Lang) {
    let mut depth = vec![0u32; tree.nodes.len()];
    for (i, n) in tree.nodes.iter().enumerate() {
        if i > 0 && (n.parent as usize) < depth.len() {
            depth[i] = depth[n.parent as usize] + 1;
        }
    }
    for (i, n) in tree.nodes.iter().enumerate() {
        if n.flags & DEAD != 0 {
            continue;
        }
        if n.flags & NAMED == 0 && n.sym == 0 {
            continue;
        }
        let d = depth[i] as usize;
        let indent = "  ".repeat(d);
        let kind = lang.kinds.resolve((n.kind & !SYNTH) as u32);
        let pre = if n.kind & SYNTH != 0 { "__" } else { "" };
        let field = if n.field != 0 {
            format!("{}:", lang.fields.resolve(n.field as u32))
        } else {
            String::new()
        };
        let sym = if n.sym != 0 {
            let s = lang.syms.resolve(n.sym);
            if s.len() > 60 {
                format!(" {:?}...", &s[..60])
            } else {
                format!(" {:?}", s)
            }
        } else {
            String::new()
        };
        println!("{indent}{field}{pre}{kind}{sym}");
    }
}

const PYTHON_RULES: &str = r#"
stages:
  - name: normalize
    rules:
      - match: '(aliased_import name: $N)'
        set_text: '$N'
      - match: '(aliased_import alias: $A)'
        append: '(__alias @$A)'
      - match: '(attribute object: (identifier "self") attribute: $A)'
        replace: '(__ivar @$A)'
      - match: '(attribute object: (identifier "cls") attribute: $A)'
        replace: '(__ivar @$A)'
      - match: '(class_definition superclasses: (argument_list $$$SUPERS:identifier|attribute|call))'
        append_under:
          target: ROOT
          each: SUPERS
          kind: __supertype
          tf: 'field=function'
      - match: '(decorated_definition $$$DECOS:decorator definition: $D)'
        append_under:
          target: D
          each: DECOS
          kind: __decorator
          tf: 'strip=@'

  - name: retag
    rules:
      - match: '(attribute object: $O attribute: $M)'
        retag:
          kind: __member
          fields: { O: object, M: member }
      - match: '(call function: $F arguments: $A)'
        retag:
          kind: __call
          fields: { F: callee, A: args }

  - name: imports
    rules:
      - match: '(import_from_statement module_name: $M $$$NAMES:dotted_name|aliased_import)'
        replace: '(__import (__source @$M) $$$NAMES=>__name)'
      - match: '(import_from_statement module_name: $M $W:wildcard_import)'
        replace: '(__import (__source @$M) (__name @$W))'
      - match: '(import_statement name: $N:dotted_name)'
        replace: '(__import (__source @$N) (__name @$N))'
      - match: '(import_statement name: $N:aliased_import)'
        replace: '(__import (__source @$N) (__name @$N))'
      - match: '(future_import_statement $$$NAMES:dotted_name)'
        replace: '(__import (__source "__future__") $$$NAMES=>__name)'

  - name: classify
    rules:
      - match: '(class_definition)'
        append: '(__deftype "Class") (__scope)'
      - match: '(function_definition)'
        append: '(__deftype "Function") (__scope)'
"#;

fn main() {
    let src = r#"
from models import User, Admin
from ..utils import helper
from models import *
import os
import numpy as np
from models import User as U, Admin as A

class Animal:
    sound = "generic"
    def speak(self):
        return self.sound

class Dog(Animal):
    sound = "woof"

@login_required
def greet(name: str) -> str:
    return f"hello {name}"

x = 42
y = greet("world")
z = Dog()
z.speak()
"#;

    let mut lang = Lang::new();
    let mut tree = grammar::parse(src, SupportLang::Python, &mut lang, "test.py");

    let stages = rules::load_rules(PYTHON_RULES, &mut lang);

    for (i, stage) in stages.iter().enumerate() {
        tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, stage);
        tree.compact();
    }

    println!("=== After YAML-defined rewrites ===");
    dump(&tree, &lang);
}
