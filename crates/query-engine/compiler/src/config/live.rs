//! The `(gql, live)` stage phases: plan a live call, read the workspaces its policies admit,
//! prove every call carries that precondition, render the call.

use query_data_model::catalog::Render;
use query_data_model::implementations::live::Live;

use super::{CompilerCtx, require};
use crate::error::Result;
use crate::passes::plan::remote::plan_live;
use crate::passes::remote_security::{check_live, secure_live};

pub(super) fn plan(ctx: &mut impl CompilerCtx<Backend = Live>) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let planned = plan_live(&statement.plan, ctx.data_model())?;
    if *ctx.trace() {
        ctx.set_planned(crate::dag::describe(&planned));
    }
    ctx.set_live(planned);
    Ok(())
}

pub(super) fn security(ctx: &mut impl CompilerCtx<Backend = Live>) -> Result<()> {
    let plan = require(ctx.take_live(), "live plan")?;
    let admitted = require(ctx.admitted().as_ref(), "admitted")?;
    let plan = secure_live(ctx.data_model(), plan, admitted)?;
    ctx.set_live(plan);
    Ok(())
}

pub(super) fn security_check(ctx: &mut impl CompilerCtx<Backend = Live>) -> Result<()> {
    check_live(require(ctx.live().as_ref(), "live plan")?)?;
    ctx.set_proved(true);
    Ok(())
}

pub(super) fn render(ctx: &mut impl CompilerCtx<Backend = Live>) -> Result<()> {
    let plan = require(ctx.live().as_ref(), "live plan")?;
    let request = ctx.data_model().render(plan)?;
    ctx.set_request(crate::dag::Request::Live(request));
    Ok(())
}
