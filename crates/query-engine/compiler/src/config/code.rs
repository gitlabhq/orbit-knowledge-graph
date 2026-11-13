//! The `(gql, orbit_code)` stage phases: plan a code search, restrict its repositories to the
//! ones its policies admit, prove every repository read carries the restriction, render the request.

use query_data_model::catalog::Render;
use query_data_model::implementations::orbit_code::OrbitCode;

use super::{CompilerCtx, require};
use crate::error::Result;
use crate::passes::plan::remote::plan_code;
use crate::passes::remote_security::{check_code, secure_code};

pub(super) fn plan(ctx: &mut impl CompilerCtx<Backend = OrbitCode>) -> Result<()> {
    let statement = require(ctx.statement().as_ref(), "statement")?;
    let placed = require(ctx.placed().as_ref(), "placed")?;
    let planned = plan_code(&statement.plan, ctx.data_model(), placed)?;
    if *ctx.trace() {
        ctx.set_planned(crate::dag::describe(&planned));
    }
    ctx.set_code(planned);
    Ok(())
}

pub(super) fn security(ctx: &mut impl CompilerCtx<Backend = OrbitCode>) -> Result<()> {
    let plan = require(ctx.take_code(), "search plan")?;
    let admitted = require(ctx.admitted().as_ref(), "admitted")?;
    let plan = secure_code(ctx.data_model(), plan, admitted)?;
    ctx.set_code(plan);
    Ok(())
}

pub(super) fn security_check(ctx: &mut impl CompilerCtx<Backend = OrbitCode>) -> Result<()> {
    check_code(require(ctx.code().as_ref(), "search plan")?)?;
    ctx.set_proved(true);
    Ok(())
}

pub(super) fn render(ctx: &mut impl CompilerCtx<Backend = OrbitCode>) -> Result<()> {
    let plan = require(ctx.code().as_ref(), "search plan")?;
    let request = ctx.data_model().render(plan)?;
    ctx.set_request(crate::dag::Request::Code(request));
    Ok(())
}
