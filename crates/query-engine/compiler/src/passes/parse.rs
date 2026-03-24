//! Parse pass: deserializes raw JSON into `Input`.

use crate::error::Result;
use crate::input::Input;
use crate::pipeline::{CompilerContext, CompilerPass, PipelineEnv};

/// Pipeline pass: deserializes the raw JSON query string into a typed `Input`.
pub struct ParsePass;

impl<E: PipelineEnv> CompilerPass<E> for ParsePass {
    const NAME: &'static str = "parse";

    fn run(&self, ctx: &mut CompilerContext<E>) -> Result<()> {
        let json = ctx.json.as_deref().expect("json must exist at Raw phase");
        let input: Input = serde_json::from_str(json)?;
        ctx.input = Some(input);
        Ok(())
    }
}
