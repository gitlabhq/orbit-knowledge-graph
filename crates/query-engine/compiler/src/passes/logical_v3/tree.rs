#[derive(Debug, Clone, PartialEq)]
pub struct Plan<Op> {
    pub op: Op,
    pub inputs: Vec<Self>,
}

impl<Op> Plan<Op> {
    pub fn leaf(op: Op) -> Self {
        Self {
            op,
            inputs: Vec::new(),
        }
    }

    pub fn unary(op: Op, input: Self) -> Self {
        Self {
            op,
            inputs: vec![input],
        }
    }

    pub fn nary(op: Op, inputs: impl IntoIterator<Item = Self>) -> Self {
        Self {
            op,
            inputs: inputs.into_iter().collect(),
        }
    }

    pub fn transform_up(mut self, rewrite: &mut impl FnMut(Self) -> Self) -> Self {
        self.inputs = self
            .inputs
            .into_iter()
            .map(|input| input.transform_up(rewrite))
            .collect();
        rewrite(self)
    }

    pub fn visit(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        self.inputs.iter().for_each(|input| input.visit(visitor));
    }

    pub fn map_children(mut self, map: &mut impl FnMut(Self) -> Self) -> Self {
        self.inputs = self.inputs.into_iter().map(map).collect();
        self
    }
}
