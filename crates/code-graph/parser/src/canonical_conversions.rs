use crate::canonical::{DefKind, ToCanonical};

use crate::csharp::types::CSharpDefinitionType;
use crate::java::types::JavaDefinitionType;
use crate::kotlin::types::KotlinDefinitionType;
use crate::python::types::PythonDefinitionType;
use crate::ruby::types::RubyDefinitionType;
use crate::rust::types::RustDefinitionType;
use crate::typescript::types::TypeScriptDefinitionType;

impl ToCanonical for PythonDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class | Self::DecoratedClass => DefKind::Class,
            Self::Function
            | Self::AsyncFunction
            | Self::DecoratedFunction
            | Self::DecoratedAsyncFunction => DefKind::Function,
            Self::Method
            | Self::AsyncMethod
            | Self::DecoratedMethod
            | Self::DecoratedAsyncMethod => DefKind::Method,
            Self::Lambda => DefKind::Lambda,
        }
    }
}

impl ToCanonical for RubyDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class => DefKind::Class,
            Self::Module => DefKind::Module,
            Self::Method | Self::SingletonMethod => DefKind::Method,
            Self::Lambda | Self::Proc => DefKind::Lambda,
        }
    }
}

impl ToCanonical for JavaDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class
            | Self::Enum
            | Self::Record
            | Self::Annotation
            | Self::AnnotationDeclaration => DefKind::Class,
            Self::Interface => DefKind::Interface,
            Self::Constructor => DefKind::Constructor,
            Self::Method => DefKind::Method,
            Self::Lambda => DefKind::Lambda,
            Self::EnumConstant => DefKind::EnumEntry,
            Self::Package => DefKind::Module,
            Self::Field | Self::Parameter | Self::LocalVariable => DefKind::Property,
        }
    }
}

impl ToCanonical for KotlinDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class
            | Self::DataClass
            | Self::ValueClass
            | Self::AnnotationClass
            | Self::Enum => DefKind::Class,
            Self::Object | Self::CompanionObject => DefKind::Class,
            Self::Interface => DefKind::Interface,
            Self::Function => DefKind::Function,
            Self::Lambda => DefKind::Lambda,
            Self::EnumEntry => DefKind::EnumEntry,
            Self::Constructor => DefKind::Constructor,
            Self::Package => DefKind::Module,
            Self::Property | Self::Parameter | Self::LocalVariable => DefKind::Property,
        }
    }
}

impl ToCanonical for CSharpDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class | Self::Record | Self::Struct | Self::Enum | Self::AnonymousType => {
                DefKind::Class
            }
            Self::Interface => DefKind::Interface,
            Self::InstanceMethod | Self::StaticMethod | Self::ExtensionMethod => DefKind::Method,
            Self::Constructor => DefKind::Constructor,
            Self::Finalizer => DefKind::Method,
            Self::Lambda => DefKind::Lambda,
            Self::Operator | Self::Indexer => DefKind::Method,
            Self::Property | Self::Event | Self::Field => DefKind::Property,
            Self::Delegate => DefKind::Other,
        }
    }
}

impl ToCanonical for TypeScriptDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Class | Self::NamedClassExpression => DefKind::Class,
            Self::Interface | Self::Type => DefKind::Interface,
            Self::Function
            | Self::NamedFunctionExpression
            | Self::NamedArrowFunction
            | Self::NamedGeneratorFunctionExpression
            | Self::NamedGeneratorFunctionDeclaration
            | Self::NamedCallExpression => DefKind::Function,
            Self::Method => DefKind::Method,
            Self::Namespace => DefKind::Module,
            Self::Enum => DefKind::Class,
        }
    }
}

impl ToCanonical for RustDefinitionType {
    fn to_def_kind(&self) -> DefKind {
        match self {
            Self::Struct | Self::Enum | Self::Union => DefKind::Class,
            Self::Trait => DefKind::Interface,
            Self::Function => DefKind::Function,
            Self::Method | Self::AssociatedFunction => DefKind::Method,
            Self::Module => DefKind::Module,
            Self::Variant => DefKind::EnumEntry,
            Self::Closure => DefKind::Lambda,
            Self::Constant | Self::Static | Self::TypeAlias | Self::Field => DefKind::Property,
            Self::Impl | Self::Macro | Self::MacroCall => DefKind::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_canonical() {
        assert_eq!(PythonDefinitionType::Class.to_def_kind(), DefKind::Class);
        assert_eq!(
            PythonDefinitionType::DecoratedClass.to_def_kind(),
            DefKind::Class
        );
        assert_eq!(PythonDefinitionType::Method.to_def_kind(), DefKind::Method);
        assert_eq!(
            PythonDefinitionType::AsyncMethod.to_def_kind(),
            DefKind::Method
        );
        assert_eq!(
            PythonDefinitionType::Function.to_def_kind(),
            DefKind::Function
        );
        assert_eq!(PythonDefinitionType::Lambda.to_def_kind(), DefKind::Lambda);
    }

    #[test]
    fn test_ruby_canonical() {
        assert_eq!(RubyDefinitionType::Class.to_def_kind(), DefKind::Class);
        assert_eq!(RubyDefinitionType::Module.to_def_kind(), DefKind::Module);
        assert_eq!(RubyDefinitionType::Method.to_def_kind(), DefKind::Method);
        assert_eq!(RubyDefinitionType::Lambda.to_def_kind(), DefKind::Lambda);
    }

    #[test]
    fn test_java_canonical() {
        assert_eq!(JavaDefinitionType::Class.to_def_kind(), DefKind::Class);
        assert_eq!(
            JavaDefinitionType::Interface.to_def_kind(),
            DefKind::Interface
        );
        assert_eq!(JavaDefinitionType::Method.to_def_kind(), DefKind::Method);
        assert_eq!(
            JavaDefinitionType::Constructor.to_def_kind(),
            DefKind::Constructor
        );
        assert_eq!(
            JavaDefinitionType::EnumConstant.to_def_kind(),
            DefKind::EnumEntry
        );
        assert_eq!(JavaDefinitionType::Package.to_def_kind(), DefKind::Module);
    }

    #[test]
    fn test_all_languages_covered() {
        // Verify every variant of every language maps to something
        // (compile-time exhaustiveness check via the match statements)
        let _ = PythonDefinitionType::Lambda.to_def_kind();
        let _ = RubyDefinitionType::Proc.to_def_kind();
        let _ = JavaDefinitionType::LocalVariable.to_def_kind();
        let _ = KotlinDefinitionType::LocalVariable.to_def_kind();
        let _ = CSharpDefinitionType::Delegate.to_def_kind();
        let _ = TypeScriptDefinitionType::Enum.to_def_kind();
        let _ = RustDefinitionType::MacroCall.to_def_kind();
    }
}
