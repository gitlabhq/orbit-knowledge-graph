#[derive(Debug, Clone, PartialEq)]
pub enum Boolean<T> {
    Leaf(T),
    And(Vec<Self>),
    Not(Box<Self>),
}

impl<T> Boolean<T> {
    pub fn leaves(&self) -> impl Iterator<Item = &T> {
        self.fold(
            &|leaf| vec![leaf],
            &|children| children.concat(),
            &|child| child,
        )
        .into_iter()
    }

    pub fn try_map<U, E>(self, map: &mut impl FnMut(T) -> Result<U, E>) -> Result<Boolean<U>, E> {
        Ok(match self {
            Self::Leaf(leaf) => Boolean::Leaf(map(leaf)?),
            Self::And(children) => Boolean::And(
                children
                    .into_iter()
                    .map(|child| child.try_map(map))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Not(child) => Boolean::Not(Box::new(child.try_map(map)?)),
        })
    }

    pub fn fold<'s, R>(
        &'s self,
        leaf: &impl Fn(&'s T) -> R,
        and: &impl Fn(Vec<R>) -> R,
        not: &impl Fn(R) -> R,
    ) -> R {
        match self {
            Self::Leaf(value) => leaf(value),
            Self::And(children) => and(children
                .iter()
                .map(|child| child.fold(leaf, and, not))
                .collect()),
            Self::Not(child) => not(child.fold(leaf, and, not)),
        }
    }

    pub fn conjuncts(self) -> Vec<Self> {
        match self {
            Self::And(children) => children,
            expression => vec![expression],
        }
    }
}
