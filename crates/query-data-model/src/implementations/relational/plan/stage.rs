use super::RelationalError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    From,
    Where,
    Grouped,
    Having,
    Computed,
    Ordered,
    LimitedBy,
    Limited,
    Selected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Stage {
    phase: Phase,
    united: bool,
}

impl Stage {
    pub(super) const FROM: Self = Self {
        phase: Phase::From,
        united: false,
    };

    pub(super) const UNITED: Self = Self {
        phase: Phase::Computed,
        united: true,
    };

    pub(super) fn complete(self) -> bool {
        self.phase == Phase::Selected || self.united
    }

    pub(super) fn joinable(self) -> bool {
        self == Self::FROM
    }

    pub(super) fn filter(self) -> Result<Self, RelationalError> {
        match self.phase {
            Phase::From | Phase::Where if !self.united => Ok(self.to(Phase::Where)),
            Phase::Grouped | Phase::Having => Ok(self.to(Phase::Having)),
            _ => Err(RelationalError::Misplaced("filter")),
        }
    }

    pub(super) fn aggregate(self) -> Result<Self, RelationalError> {
        self.advance("aggregate", Phase::Where, Phase::Grouped)
    }

    pub(super) fn compute(self) -> Result<Self, RelationalError> {
        self.advance("compute", Phase::Having, Phase::Computed)
    }

    pub(super) fn order(self) -> Result<Self, RelationalError> {
        self.advance("order", Phase::Computed, Phase::Ordered)
    }

    pub(super) fn latest(self) -> Result<Self, RelationalError> {
        self.advance("latest", Phase::Computed, Phase::LimitedBy)
    }

    pub(super) fn limit_by(self) -> Result<Self, RelationalError> {
        self.advance("limit by", Phase::Ordered, Phase::LimitedBy)
    }

    pub(super) fn limit(self) -> Result<Self, RelationalError> {
        self.advance("limit", Phase::LimitedBy, Phase::Limited)
    }

    pub(super) fn select(self) -> Result<Self, RelationalError> {
        match self.united {
            true => Err(RelationalError::Misplaced("select")),
            false => self.advance("select", Phase::Limited, Phase::Selected),
        }
    }

    fn advance(
        self,
        operation: &'static str,
        latest: Phase,
        next: Phase,
    ) -> Result<Self, RelationalError> {
        match self.phase <= latest {
            true => Ok(self.to(next)),
            false => Err(RelationalError::Misplaced(operation)),
        }
    }

    fn to(self, phase: Phase) -> Self {
        Self { phase, ..self }
    }
}
