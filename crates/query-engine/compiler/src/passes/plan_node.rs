const INLINE_WIDTH: usize = 90;

#[derive(Debug, Clone, PartialEq)]
pub struct PlanNode {
    pub label: String,
    pub head: String,
    pub items: Vec<String>,
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    pub fn render(&self, depth: usize) -> String {
        let indent = "  ".repeat(depth);
        let mut output = format!("{indent}({}", self.label);
        if !self.head.is_empty() {
            output.push(' ');
            output.push_str(&self.head);
        }
        let inline_items = self.items.join(", ");
        if self.items.len() <= 3 && inline_items.len() <= INLINE_WIDTH {
            if !inline_items.is_empty() {
                output.push(' ');
                output.push_str(&inline_items);
            }
        } else {
            for item in &self.items {
                output.push_str(&format!("\n{indent}    {item}"));
            }
        }
        for child in &self.children {
            output.push('\n');
            output.push_str(&child.render(depth + 1));
        }
        output.push(')');
        output
    }
}
