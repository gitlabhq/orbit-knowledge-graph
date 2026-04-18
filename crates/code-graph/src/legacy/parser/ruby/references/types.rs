//! Ruby-specific reference types and structures

use crate::legacy::parser::references::{ReferenceInfo, TargetResolution};
use crate::legacy::parser::ruby::definitions::RubyDefinitionInfo;
use crate::legacy::parser::ruby::imports::RubyImportedSymbolInfo;
use crate::legacy::parser::ruby::references::expressions::RubyExpressionSymbol;
use crate::legacy::parser::ruby::types::RubyFqn;

/// Type alias for Ruby reference information to reduce type complexity
pub type RubyReferenceInfo =
    ReferenceInfo<RubyTargetResolution, RubyReferenceType, RubyExpressionMetadata, RubyFqn>;

/// Types of references in Ruby code
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RubyReferenceType {
    /// Method or function call (e.g., `user.save`)
    Call,
    /// Assignment to a variable/constant (e.g., `user = User.new`)
    Assignment,
    /// Constant reference (e.g., `User`)
    Constant,
    /// Instance variable reference (e.g., `@user`)
    InstanceVariable,
    /// Class variable reference (e.g., `@@count`)
    ClassVariable,
    /// Global variable reference (e.g., `$stdout`)
    GlobalVariable,
}

/// Ruby expression metadata containing symbol chain information for partial resolution
/// This is stored in the metadata field of ReferenceInfo to be resolved later in the indexer
#[derive(Debug, Clone)]
pub struct RubyExpressionMetadata {
    /// The variable or constant being assigned to (LHS).
    /// Present in expressions like `user = User.new` or `@profile = Profile.new`.
    pub assignment_target: Option<RubyExpressionSymbol>,
    /// An ordered sequence of symbols representing the expression's logic.
    /// For `user.profile.update`, this would be `[user, profile, update]`.
    pub symbols: Vec<RubyExpressionSymbol>,
}

/// Ruby-specific target resolution types
pub type RubyTargetResolution =
    TargetResolution<RubyDefinitionInfo, RubyImportedSymbolInfo, RubyExpressionMetadata>;
