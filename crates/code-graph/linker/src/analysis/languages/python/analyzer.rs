use tracing::error;

use crate::analysis::languages::python::interfile::get_possible_symbol_locations;
use crate::analysis::types::{
    ConsolidatedRelationship, DefinitionNode, DefinitionType, FqnType, ImportIdentifier,
    ImportType, ImportedSymbolLocation, ImportedSymbolNode, OptimizedFileTree,
};
use crate::graph::{RelationshipKind, RelationshipType};
use crate::parsing::processor::{FileProcessingResult, References};
use internment::ArcIntern;
use parser_core::python::types::PythonReferenceInfo;
use parser_core::python::types::{Connector, PythonImportType, Symbol};
use parser_core::python::{
    fqn::python_fqn_to_string,
    types::{
        PartialResolution, PythonDefinitionType, PythonFqn, PythonImportedSymbolInfo,
        PythonTargetResolution,
    },
};
use parser_core::references::ReferenceTarget;
use parser_core::utils::Range;
use std::collections::{HashMap, HashSet};

/// Represents the result of resolving an imported symbol
#[derive(Debug, Clone)]
enum ResolvedTarget {
    ImportedSymbol(ImportedSymbolNode),
    Definition(DefinitionNode),
    File(String),
}

// Handles Python-specific analysis operations
pub struct PythonAnalyzer;

impl Default for PythonAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl PythonAnalyzer {
    /// Create a new Python analyzer
    pub fn new() -> Self {
        Self
    }

    /// Process definitions from a file result and update the definitions map
    pub fn process_definitions(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        definition_map: &mut HashMap<(String, String), (DefinitionNode, FqnType)>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(defs) = file_result.definitions.iter_python() {
            for definition in defs {
                let fqn = FqnType::Python(definition.fqn.clone());
                let path = ArcIntern::new(relative_file_path.to_string());
                let definition_node = DefinitionNode::new(
                    fqn.clone(),
                    DefinitionType::Python(definition.definition_type),
                    definition.range,
                    path.clone(),
                );

                if self.is_top_level_definition(&definition.fqn) {
                    let mut relationship =
                        ConsolidatedRelationship::file_to_definition(path.clone(), path.clone());
                    relationship.source_range = ArcIntern::new(Range::empty()); // File source has no specific range
                    relationship.target_range = ArcIntern::new(definition.range);
                    relationship.relationship_type = RelationshipType::FileDefines;
                    relationships.push(relationship);
                }
                let key = (fqn.to_string(), relative_file_path.to_string());

                if definition_map.contains_key(&key) {
                    log::warn!(
                        "Duplicate definition found for Python: {} in file {}",
                        definition.name,
                        relative_file_path
                    );
                    continue;
                }

                definition_map.insert(
                    key,
                    (
                        definition_node.clone(),
                        FqnType::Python(definition.fqn.clone()),
                    ),
                );
            }
        }
    }

    /// Process imported symbols from a file result and update the import map
    pub fn process_imports(
        &self,
        file_result: &FileProcessingResult,
        relative_file_path: &str,
        imported_symbol_map: &mut HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        if let Some(imported_symbols) = &file_result.imported_symbols
            && let Some(imports) = imported_symbols.iter_python()
        {
            for imported_symbol in imports {
                let location =
                    self.create_imported_symbol_location(imported_symbol, relative_file_path);
                let identifier = self.create_imported_symbol_identifier(imported_symbol);
                let scope_fqn_string = if let Some(ref scope) = imported_symbol.scope {
                    python_fqn_to_string(scope)
                } else {
                    "".to_string()
                };
                let imported_symbol_node = ImportedSymbolNode::new(
                    ImportType::Python(imported_symbol.import_type),
                    imported_symbol.import_path.clone(),
                    identifier,
                    location.clone(),
                );

                if let Some(imported_symbol_nodes) = imported_symbol_map
                    .get_mut(&(scope_fqn_string.clone(), relative_file_path.to_string()))
                {
                    imported_symbol_nodes.push(imported_symbol_node);
                } else {
                    imported_symbol_map.insert(
                        (scope_fqn_string.clone(), relative_file_path.to_string()),
                        vec![imported_symbol_node],
                    );
                }

                let mut relationship: ConsolidatedRelationship =
                    ConsolidatedRelationship::file_to_imported_symbol(
                        ArcIntern::new(relative_file_path.to_string()),
                        ArcIntern::new(location.file_path.clone()),
                    );
                relationship.source_range = ArcIntern::new(Range::empty());
                relationship.target_range = ArcIntern::new(location.range());
                relationship.relationship_type = RelationshipType::FileImports;
                relationships.push(relationship);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_references(
        &self,
        file_references: &Option<References>,
        relative_file_path: &str,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
        imported_symbol_to_imported_symbols: &HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        imported_symbol_to_definitions: &HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        imported_symbol_to_files: &HashMap<ImportedSymbolLocation, Vec<String>>,
    ) {
        let file_path = relative_file_path.to_string();
        if let Some(references) = file_references
            && let Some(references) = references.iter_python()
        {
            for reference in references {
                let source_definition = if let Some(scope) = reference.scope.as_ref() {
                    let fqn_string = python_fqn_to_string(scope);
                    definition_map
                        .get(&(fqn_string, file_path.clone()))
                        .map(|map_value| map_value.0.clone())
                } else {
                    None
                };

                match &reference.target {
                    ReferenceTarget::Resolved(resolved_target) => {
                        self.process_resolved_target(
                            resolved_target,
                            &file_path,
                            reference,
                            &source_definition,
                            definition_map,
                            imported_symbol_map,
                            relationships,
                            imported_symbol_to_imported_symbols,
                            imported_symbol_to_definitions,
                            imported_symbol_to_files,
                            false,
                        );
                    }
                    ReferenceTarget::Ambiguous(possible_targets) => {
                        for possible_target in possible_targets {
                            self.process_resolved_target(
                                possible_target,
                                &file_path,
                                reference,
                                &source_definition,
                                definition_map,
                                imported_symbol_map,
                                relationships,
                                imported_symbol_to_imported_symbols,
                                imported_symbol_to_definitions,
                                imported_symbol_to_files,
                                true,
                            );
                        }
                    }
                    ReferenceTarget::Unresolved() => {
                        continue;
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_resolved_target(
        &self,
        resolved_target: &PythonTargetResolution,
        file_path: &str,
        reference: &PythonReferenceInfo,
        source_definition: &Option<DefinitionNode>,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
        imported_symbol_to_imported_symbols: &HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        imported_symbol_to_definitions: &HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        imported_symbol_to_files: &HashMap<ImportedSymbolLocation, Vec<String>>,
        is_ambiguous: bool,
    ) {
        match resolved_target {
            PythonTargetResolution::Definition(target_def_info) => {
                let target_def_node = definition_map
                    .get(&(
                        python_fqn_to_string(&target_def_info.fqn),
                        file_path.to_owned(),
                    ))
                    .map(|map_value| map_value.0.clone());
                if let Some(target_def_node) = target_def_node {
                    self.add_definition_reference_relationship(
                        file_path,
                        reference,
                        source_definition,
                        &target_def_node,
                        relationships,
                        is_ambiguous,
                    );
                }
            }
            PythonTargetResolution::ImportedSymbol(target_import_info) => {
                let mut results = Vec::new();
                let mut visited = HashSet::new();
                let imported_symbol_location =
                    self.create_imported_symbol_location(target_import_info, file_path);

                Self::recursively_resolve_imported_symbol(
                    imported_symbol_location.clone(),
                    imported_symbol_to_imported_symbols,
                    imported_symbol_to_definitions,
                    imported_symbol_to_files,
                    &mut results,
                    &mut visited,
                );

                // Create relationships based on resolved targets
                let is_ambiguous = results.len() > 1 || is_ambiguous;
                for resolved_target in results {
                    match resolved_target {
                        ResolvedTarget::ImportedSymbol(target_imported_symbol_node) => {
                            self.add_imported_symbol_reference_relationship(
                                file_path,
                                reference,
                                source_definition,
                                &target_imported_symbol_node,
                                relationships,
                                is_ambiguous,
                            );
                        }
                        ResolvedTarget::Definition(target_definition_node) => self
                            .add_definition_reference_relationship(
                                file_path,
                                reference,
                                source_definition,
                                &target_definition_node,
                                relationships,
                                is_ambiguous,
                            ),
                        _ => {}
                    }
                }
            }
            PythonTargetResolution::PartialResolution(partial_resolution) => {
                // Only imported symbols can be targets of a partial resolution
                if let PythonTargetResolution::ImportedSymbol(target_import_info) =
                    &*partial_resolution.target
                {
                    // Get all possible starting targets
                    let mut targets = Vec::new();
                    match target_import_info.import_type {
                        PythonImportType::Import | PythonImportType::AliasedImport => {
                            let mut results = Vec::new();
                            let mut visited = HashSet::new();
                            let imported_symbol_location =
                                self.create_imported_symbol_location(target_import_info, file_path);

                            Self::recursively_resolve_imported_symbol(
                                imported_symbol_location.clone(),
                                imported_symbol_to_imported_symbols,
                                imported_symbol_to_definitions,
                                imported_symbol_to_files,
                                &mut results,
                                &mut visited,
                            );

                            targets.extend(results);
                        }
                        PythonImportType::FromImport
                        | PythonImportType::AliasedFromImport
                        | PythonImportType::RelativeImport
                        | PythonImportType::AliasedRelativeImport => {
                            let mut results = Vec::new();
                            let mut visited = HashSet::new();
                            let imported_symbol_location =
                                self.create_imported_symbol_location(target_import_info, file_path);

                            Self::recursively_resolve_imported_symbol(
                                imported_symbol_location.clone(),
                                imported_symbol_to_imported_symbols,
                                imported_symbol_to_definitions,
                                imported_symbol_to_files,
                                &mut results,
                                &mut visited,
                            );

                            for resolved_target in results {
                                match &resolved_target {
                                    ResolvedTarget::Definition(_) => {
                                        targets.push(resolved_target);
                                    }
                                    ResolvedTarget::ImportedSymbol(imported_symbol_node) => {
                                        // Non-local imported symbol - create a relationship
                                        self.add_imported_symbol_reference_relationship(
                                            file_path,
                                            reference,
                                            source_definition,
                                            imported_symbol_node,
                                            relationships,
                                            is_ambiguous,
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {
                            // We ignore wildcard and __future__ imports
                            return;
                        }
                    }

                    // Process each possible initial target by creating branches for multiple matches
                    for target in targets {
                        self.process_partial_resolution_branch(
                            target,
                            partial_resolution,
                            file_path,
                            reference,
                            source_definition,
                            definition_map,
                            imported_symbol_map,
                            relationships,
                            imported_symbol_to_imported_symbols,
                            imported_symbol_to_definitions,
                            imported_symbol_to_files,
                            is_ambiguous,
                        );
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_partial_resolution_branch(
        &self,
        initial_target: ResolvedTarget,
        partial_resolution: &PartialResolution,
        file_path: &str,
        reference: &PythonReferenceInfo,
        source_definition: &Option<DefinitionNode>,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
        imported_symbol_to_imported_symbols: &HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        imported_symbol_to_definitions: &HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        imported_symbol_to_files: &HashMap<ImportedSymbolLocation, Vec<String>>,
        is_ambiguous: bool,
    ) {
        let mut curr_target = initial_target;
        let mut right_pointer = partial_resolution.index + 1;

        // Expand the symbol chain
        let mut is_terminated = false;
        while right_pointer < partial_resolution.symbol_chain.symbols.len() {
            let prev_symbol = &partial_resolution.symbol_chain.symbols[right_pointer - 1];
            let curr_symbol = &partial_resolution.symbol_chain.symbols[right_pointer];

            match curr_symbol {
                Symbol::Identifier(identifier) => {
                    // If identifier following identifier, then we break
                    if let Symbol::Identifier(_) = prev_symbol {
                        is_terminated = true;
                        break;
                    }

                    match &curr_target {
                        ResolvedTarget::File(curr_target_file) => {
                            // Check if identifier is attribute of file
                            if let Symbol::Connector(connector) = prev_symbol
                                && *connector == Connector::Attribute
                            {
                                if let Some(matched_target) = self
                                    .get_matching_definition_or_imported_symbol(
                                        definition_map,
                                        imported_symbol_map,
                                        identifier,
                                        curr_target_file,
                                    )
                                {
                                    match matched_target {
                                        ResolvedTarget::ImportedSymbol(imported_symbol_node) => {
                                            let mut results = Vec::new();
                                            let mut visited = HashSet::new();

                                            Self::recursively_resolve_imported_symbol(
                                                imported_symbol_node.location.clone(),
                                                imported_symbol_to_imported_symbols,
                                                imported_symbol_to_definitions,
                                                imported_symbol_to_files,
                                                &mut results,
                                                &mut visited,
                                            );

                                            // Create branches for each resolved match
                                            for resolved_match in results {
                                                self.process_partial_resolution_branch(
                                                    resolved_match,
                                                    partial_resolution,
                                                    file_path,
                                                    reference,
                                                    source_definition,
                                                    definition_map,
                                                    imported_symbol_map,
                                                    relationships,
                                                    imported_symbol_to_imported_symbols,
                                                    imported_symbol_to_definitions,
                                                    imported_symbol_to_files,
                                                    is_ambiguous,
                                                );
                                            }
                                            return; // Exit this branch since we've created sub-branches
                                        }
                                        ResolvedTarget::Definition(_) => {
                                            curr_target = matched_target;
                                        }
                                        _ => {}
                                    }
                                } else {
                                    // No match found, terminate search
                                    is_terminated = true;
                                    break;
                                }
                            }
                        }
                        ResolvedTarget::Definition(definition_node) => {
                            let fqn = format!("{}.{}", definition_node.fqn, &identifier);
                            if let Some(matched_target) = self
                                .get_matching_definition_or_imported_symbol(
                                    definition_map,
                                    imported_symbol_map,
                                    &fqn,
                                    &definition_node.file_path,
                                )
                            {
                                match matched_target {
                                    ResolvedTarget::ImportedSymbol(imported_symbol_node) => {
                                        let mut results = Vec::new();
                                        let mut visited = HashSet::new();

                                        Self::recursively_resolve_imported_symbol(
                                            imported_symbol_node.location.clone(),
                                            imported_symbol_to_imported_symbols,
                                            imported_symbol_to_definitions,
                                            imported_symbol_to_files,
                                            &mut results,
                                            &mut visited,
                                        );

                                        // Create branches for each resolved match
                                        for resolved_match in results {
                                            self.process_partial_resolution_branch(
                                                resolved_match,
                                                partial_resolution,
                                                file_path,
                                                reference,
                                                source_definition,
                                                definition_map,
                                                imported_symbol_map,
                                                relationships,
                                                imported_symbol_to_imported_symbols,
                                                imported_symbol_to_definitions,
                                                imported_symbol_to_files,
                                                is_ambiguous,
                                            );
                                        }
                                        return; // Exit this branch since we've created sub-branches
                                    }
                                    ResolvedTarget::Definition(_) => {
                                        curr_target = matched_target;
                                    }
                                    _ => {}
                                }
                            } else {
                                is_terminated = true;
                                break;
                            }
                        }
                        ResolvedTarget::ImportedSymbol(_) => {
                            is_terminated = true;
                            break;
                        }
                    }
                }
                // Decides whether to terminate the search
                Symbol::Connector(connector) => {
                    match connector {
                        Connector::Index => {
                            is_terminated = true;
                            break;
                        }
                        Connector::Call => {
                            // If call following call, then we break
                            if let Symbol::Connector(prev_connector) = prev_symbol
                                && *prev_connector == Connector::Call
                            {
                                is_terminated = true;
                                break;
                            }
                        }
                        _ => {}
                    }

                    match &curr_target {
                        ResolvedTarget::Definition(definition_node) => {
                            if let DefinitionType::Python(definition_type) =
                                definition_node.definition_type
                            {
                                // Special handling for function definitions, since we don't track function outputs (for now)
                                if !(definition_type == PythonDefinitionType::Class
                                    || definition_type == PythonDefinitionType::DecoratedClass)
                                {
                                    // If not a function call, we terminate search
                                    if *connector != Connector::Call {
                                        is_terminated = true;
                                        break;
                                    }
                                }
                            } else {
                                // Non-Python definition, terminate search (shouldn't happen)
                                is_terminated = true;
                                break;
                            }
                        }
                        ResolvedTarget::File(_) => {
                            if let Symbol::Identifier(_) = prev_symbol
                                && *connector != Connector::Attribute
                            {
                                is_terminated = true;
                                break;
                            }
                        }
                        ResolvedTarget::ImportedSymbol(_) => {
                            // Terminate search for attributes of non-local imported symbols
                            if *connector != Connector::Call {
                                is_terminated = true;
                                break;
                            } else if let Symbol::Connector(prev_connector) = prev_symbol
                                && *prev_connector == Connector::Call
                            {
                                is_terminated = true;
                                break;
                            }
                        }
                    }
                }
                Symbol::Receiver() => {
                    // We ignore receivers until cross-file inheritance is implemented
                    is_terminated = true;
                    break;
                }
            }

            right_pointer += 1;
        }

        if is_terminated {
            return;
        }

        // Create a relationship
        match &curr_target {
            ResolvedTarget::Definition(definition_node) => {
                self.add_definition_reference_relationship(
                    file_path,
                    reference,
                    source_definition,
                    definition_node,
                    relationships,
                    is_ambiguous,
                );
            }
            ResolvedTarget::ImportedSymbol(imported_symbol_node) => {
                self.add_imported_symbol_reference_relationship(
                    file_path,
                    reference,
                    source_definition,
                    imported_symbol_node,
                    relationships,
                    is_ambiguous,
                );
            }
            _ => {}
        }
    }

    pub fn resolve_imported_symbols(
        &self,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        file_tree: &OptimizedFileTree,
        imported_symbol_to_imported_symbols: &mut HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        imported_symbol_to_definitions: &mut HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        imported_symbol_to_files: &mut HashMap<ImportedSymbolLocation, Vec<String>>,
    ) {
        for ((_imported_symbol_fqn_string, _imported_symbol_file_path), imported_symbol_nodes) in
            imported_symbol_map
        {
            for imported_symbol_node in imported_symbol_nodes {
                if let ImportType::Python(import_type) = imported_symbol_node.import_type {
                    let possible_files = get_possible_symbol_locations(
                        imported_symbol_node,
                        file_tree,
                        definition_map,
                    );

                    match import_type {
                        PythonImportType::FutureImport | PythonImportType::AliasedFutureImport => {}
                        PythonImportType::Import | PythonImportType::AliasedImport => {
                            // NOTE: For now, we are ignoring other possible files because it's very unlikely that there will
                            // be more than one
                            if let Some(possible_file) = possible_files.first() {
                                imported_symbol_to_files.insert(
                                    imported_symbol_node.location.clone(),
                                    vec![possible_file.clone()],
                                );
                            }
                        }
                        PythonImportType::WildcardImport
                        | PythonImportType::RelativeWildcardImport => {
                            // TODO: We should preserve all *possible* relationships instead of only the first. When we attempt to resolve
                            // unresolved or partial resolutions, we will need to explore all possible files for a symbol.
                            let first_possible_file = possible_files.first();
                            if let Some(first_possible_file) = first_possible_file {
                                imported_symbol_to_files.insert(
                                    imported_symbol_node.location.clone(),
                                    vec![first_possible_file.clone()],
                                );
                            }
                        }
                        // From imports (`from A import B`, `from A import B as C`, `from . import A`, `from . import *`)
                        _ => {
                            if let Some(name) = imported_symbol_node
                                .identifier
                                .as_ref()
                                .map(|identifier| identifier.name.clone())
                            {
                                let mut matched_definitions = vec![];
                                let mut matched_imported_symbols = vec![];
                                for possible_file in possible_files {
                                    if let Some(matched_target) = self
                                        .get_matching_definition_or_imported_symbol(
                                            definition_map,
                                            imported_symbol_map,
                                            &name,
                                            &possible_file,
                                        )
                                    {
                                        match matched_target {
                                            ResolvedTarget::Definition(def_node) => {
                                                matched_definitions.push(def_node);
                                            }
                                            ResolvedTarget::ImportedSymbol(imp_node) => {
                                                matched_imported_symbols.push(imp_node);
                                            }
                                            _ => {}
                                        }
                                    }
                                }

                                imported_symbol_to_imported_symbols.insert(
                                    imported_symbol_node.location.clone(),
                                    matched_imported_symbols,
                                );
                                imported_symbol_to_definitions.insert(
                                    imported_symbol_node.location.clone(),
                                    matched_definitions,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Create definition-to-definition and definition-to-imported-symbol relationships using definitions map
    pub fn add_definition_relationships(
        &self,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        relationships: &mut Vec<ConsolidatedRelationship>,
    ) {
        for ((child_fqn_string, child_file_path), (child_def, child_fqn)) in definition_map {
            // Handle definition-to-imported-symbol relationships
            if let Some(imported_symbol_nodes) =
                imported_symbol_map.get(&(child_fqn_string.clone(), child_file_path.to_string()))
            {
                for imported_symbol in imported_symbol_nodes {
                    let relationship = ConsolidatedRelationship {
                        source_path: Some(ArcIntern::new(child_file_path.to_string())),
                        target_path: Some(ArcIntern::new(
                            imported_symbol.location.file_path.clone(),
                        )),
                        kind: RelationshipKind::DefinitionToImportedSymbol,
                        relationship_type: RelationshipType::DefinesImportedSymbol,
                        source_range: ArcIntern::new(child_def.range),
                        target_range: ArcIntern::new(imported_symbol.location.range()),
                        ..Default::default()
                    };
                    relationships.push(relationship);
                }
            }

            // Handle definition-to-definition relationships
            if let Some(parent_fqn_string) = self.get_parent_fqn_string(child_fqn)
                && let Some((parent_def, _)) =
                    definition_map.get(&(parent_fqn_string.clone(), child_file_path.to_string()))
                && let Some(relationship_type) = self.get_definition_relationship_type(
                    &parent_def.definition_type,
                    &child_def.definition_type,
                )
            {
                let relationship = ConsolidatedRelationship {
                    source_path: Some(parent_def.file_path.clone()),
                    target_path: Some(child_def.file_path.clone()),
                    kind: RelationshipKind::DefinitionToDefinition,
                    relationship_type,
                    source_range: ArcIntern::new(parent_def.range),
                    target_range: ArcIntern::new(child_def.range),
                    ..Default::default()
                };
                relationships.push(relationship);
            }
        }
    }

    fn recursively_resolve_imported_symbol(
        current_location: ImportedSymbolLocation,
        imported_symbol_to_imported_symbols: &HashMap<
            ImportedSymbolLocation,
            Vec<ImportedSymbolNode>,
        >,
        imported_symbol_to_definitions: &HashMap<ImportedSymbolLocation, Vec<DefinitionNode>>,
        imported_symbol_to_files: &HashMap<ImportedSymbolLocation, Vec<String>>,
        results: &mut Vec<ResolvedTarget>,
        visited: &mut HashSet<ImportedSymbolLocation>,
    ) {
        let remaining_stack = stacker::remaining_stack().unwrap_or(0);
        if remaining_stack < crate::MINIMUM_STACK_REMAINING {
            error!(
                remaining_stack,
                "stack limit reached, aborting Python imported symbol resolution"
            );
            return;
        }

        // Prevent infinite recursion
        if visited.contains(&current_location) {
            return;
        }
        visited.insert(current_location.clone());

        // Check imported_symbol_to_imported_symbols hashmap
        if let Some(matched_imported_symbols) =
            imported_symbol_to_imported_symbols.get(&current_location)
        {
            for matched_imported_symbol in matched_imported_symbols {
                // Check if this is a terminal imported symbol (no further resolution)
                let is_terminal = !imported_symbol_to_imported_symbols
                    .contains_key(&matched_imported_symbol.location)
                    && !imported_symbol_to_definitions
                        .contains_key(&matched_imported_symbol.location)
                    && !imported_symbol_to_files.contains_key(&matched_imported_symbol.location);

                if is_terminal {
                    results.push(ResolvedTarget::ImportedSymbol(
                        matched_imported_symbol.clone(),
                    ));
                } else {
                    // Keep recursing
                    Self::recursively_resolve_imported_symbol(
                        matched_imported_symbol.location.clone(),
                        imported_symbol_to_imported_symbols,
                        imported_symbol_to_definitions,
                        imported_symbol_to_files,
                        results,
                        visited,
                    );
                }
            }
        }

        // Check imported_symbol_to_definitions hashmap
        if let Some(matched_definitions) = imported_symbol_to_definitions.get(&current_location) {
            for matched_definition in matched_definitions {
                results.push(ResolvedTarget::Definition(matched_definition.clone()));
            }
        }

        // Check imported_symbol_to_files hashmap
        if let Some(matched_files) = imported_symbol_to_files.get(&current_location) {
            for matched_file in matched_files {
                results.push(ResolvedTarget::File(matched_file.clone()));
            }
        }

        // TODO: Should we just return the imported symbol if no other matches are found?
    }

    fn get_matching_definition_or_imported_symbol(
        &self,
        definition_map: &HashMap<(String, String), (DefinitionNode, FqnType)>,
        imported_symbol_map: &HashMap<(String, String), Vec<ImportedSymbolNode>>,
        name: &String,
        file_path: &str,
    ) -> Option<ResolvedTarget> {
        // Get matching definition and imported symbol (if either exist)
        let matched_definition_node = definition_map
            .get(&(name.clone(), file_path.to_owned()))
            .map(|(definition_node, _)| definition_node.clone());
        let matched_imported_symbol_node = if let Some(imported_symbol_nodes) =
            imported_symbol_map.get(&("".to_string(), file_path.to_owned()))
        {
            imported_symbol_nodes
                .iter()
                .filter(|node| {
                    if let Some(identifier) = &node.identifier {
                        if let Some(alias) = &identifier.alias {
                            *alias == *name
                        } else {
                            identifier.name == *name
                        }
                    } else {
                        false
                    }
                })
                .max_by_key(|node| node.location.start_byte)
        } else {
            None
        };

        // Prefer the most recent symbol: imported symbol if it exists and is more recent, otherwise definition, otherwise imported symbol
        match (matched_definition_node, matched_imported_symbol_node) {
            (Some(def_node), Some(imp_node)) => {
                if imp_node.location.start_byte > def_node.range.byte_offset.0 as i64 {
                    Some(ResolvedTarget::ImportedSymbol(imp_node.clone()))
                } else {
                    Some(ResolvedTarget::Definition(def_node.clone()))
                }
            }
            (Some(def_node), None) => Some(ResolvedTarget::Definition(def_node.clone())),
            (None, Some(imp_node)) => Some(ResolvedTarget::ImportedSymbol(imp_node.clone())),
            (None, None) => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_definition_reference_relationship(
        &self,
        file_path: &str,
        reference: &PythonReferenceInfo,
        source_definition: &Option<DefinitionNode>,
        target_definition_node: &DefinitionNode,
        relationships: &mut Vec<ConsolidatedRelationship>,
        is_ambiguous: bool,
    ) {
        if source_definition.is_none() {
            let relationship = ConsolidatedRelationship {
                source_path: Some(ArcIntern::new(file_path.to_string())),
                target_path: Some(target_definition_node.file_path.clone()),
                kind: RelationshipKind::FileToDefinition,
                relationship_type: if is_ambiguous {
                    RelationshipType::AmbiguouslyCalls
                } else {
                    RelationshipType::Calls
                },
                source_range: ArcIntern::new(reference.range),
                target_range: ArcIntern::new(target_definition_node.range),
                ..Default::default()
            };
            // warn!("add_definition_reference_relationship::target range: {:?}", relationship.target_range);
            relationships.push(relationship);
        } else {
            let source_definition = source_definition.as_ref().unwrap();
            let relationship = ConsolidatedRelationship {
                source_path: Some(source_definition.file_path.clone()),
                target_path: Some(target_definition_node.file_path.clone()),
                kind: RelationshipKind::DefinitionToDefinition,
                relationship_type: if is_ambiguous {
                    RelationshipType::AmbiguouslyCalls
                } else {
                    RelationshipType::Calls
                },
                source_range: ArcIntern::new(source_definition.range),
                target_range: ArcIntern::new(target_definition_node.range),
                ..Default::default()
            };
            relationships.push(relationship);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn add_imported_symbol_reference_relationship(
        &self,
        file_path: &str,
        reference: &PythonReferenceInfo,
        source_definition: &Option<DefinitionNode>,
        target_imported_symbol_node: &ImportedSymbolNode,
        relationships: &mut Vec<ConsolidatedRelationship>,
        is_ambiguous: bool,
    ) {
        if source_definition.is_none() {
            let relationship = ConsolidatedRelationship {
                source_path: Some(ArcIntern::new(file_path.to_string())),
                target_path: Some(ArcIntern::new(
                    target_imported_symbol_node.location.file_path.clone(),
                )),
                kind: RelationshipKind::FileToImportedSymbol,
                relationship_type: if is_ambiguous {
                    RelationshipType::AmbiguouslyCalls
                } else {
                    RelationshipType::Calls
                },
                source_range: ArcIntern::new(reference.range),
                target_range: ArcIntern::new(target_imported_symbol_node.location.range()),
                ..Default::default()
            };
            relationships.push(relationship);
        } else {
            let source_definition = source_definition.as_ref().unwrap();
            let relationship = ConsolidatedRelationship {
                source_path: Some(source_definition.file_path.clone()),
                target_path: Some(ArcIntern::new(
                    target_imported_symbol_node.location.file_path.clone(),
                )),
                kind: RelationshipKind::DefinitionToImportedSymbol,
                relationship_type: if is_ambiguous {
                    RelationshipType::AmbiguouslyCalls
                } else {
                    RelationshipType::Calls
                },
                source_range: ArcIntern::new(source_definition.range),
                target_range: ArcIntern::new(target_imported_symbol_node.location.range()),
                ..Default::default()
            };
            relationships.push(relationship);
        }
    }

    /// Create an imported symbol location from an imported symbol info
    fn create_imported_symbol_location(
        &self,
        imported_symbol: &PythonImportedSymbolInfo,
        file_path: &str,
    ) -> ImportedSymbolLocation {
        ImportedSymbolLocation {
            file_path: file_path.to_string(),
            start_byte: imported_symbol.range.byte_offset.0 as i64,
            end_byte: imported_symbol.range.byte_offset.1 as i64,
            start_line: imported_symbol.range.start.line as i32,
            end_line: imported_symbol.range.end.line as i32,
            start_col: imported_symbol.range.start.column as i32,
            end_col: imported_symbol.range.end.column as i32,
        }
    }

    fn create_imported_symbol_identifier(
        &self,
        imported_symbol: &PythonImportedSymbolInfo,
    ) -> Option<ImportIdentifier> {
        if let Some(identifier) = &imported_symbol.identifier {
            return Some(ImportIdentifier {
                name: identifier.name.clone(),
                alias: identifier.alias.clone(),
            });
        }

        None
    }

    /// Extract parent FQN string from a given FQN
    fn get_parent_fqn_string(&self, fqn: &FqnType) -> Option<String> {
        match fqn {
            FqnType::Python(python_fqn) => {
                let parts_len = python_fqn.parts.len();
                if parts_len <= 1 {
                    return None;
                }

                // Build parent string directly without creating new Vec
                Some(
                    python_fqn.parts[..parts_len - 1]
                        .iter()
                        .map(|part| part.node_name.replace('.', "#"))
                        .collect::<Vec<_>>()
                        .join("."),
                )
            }
            _ => None,
        }
    }

    fn simplify_definition_type(&self, definition_type: &DefinitionType) -> Option<DefinitionType> {
        use PythonDefinitionType::*;

        match definition_type {
            DefinitionType::Python(Class) | DefinitionType::Python(DecoratedClass) => {
                Some(DefinitionType::Python(Class))
            }
            DefinitionType::Python(Method)
            | DefinitionType::Python(AsyncMethod)
            | DefinitionType::Python(DecoratedMethod)
            | DefinitionType::Python(DecoratedAsyncMethod) => Some(DefinitionType::Python(Method)),
            DefinitionType::Python(Function)
            | DefinitionType::Python(AsyncFunction)
            | DefinitionType::Python(DecoratedFunction)
            | DefinitionType::Python(DecoratedAsyncFunction) => {
                Some(DefinitionType::Python(Function))
            }
            DefinitionType::Python(Lambda) => Some(DefinitionType::Python(Lambda)),
            _ => None,
        }
    }

    /// Determine the relationship type between parent and child definitions using proper types
    fn get_definition_relationship_type(
        &self,
        parent_type: &DefinitionType,
        child_type: &DefinitionType,
    ) -> Option<RelationshipType> {
        use PythonDefinitionType::*;

        let parent_type = self.simplify_definition_type(parent_type)?;
        let child_type = self.simplify_definition_type(child_type)?;

        match (parent_type, child_type) {
            (DefinitionType::Python(Class), DefinitionType::Python(Class)) => {
                Some(RelationshipType::ClassToClass)
            }
            (DefinitionType::Python(Class), DefinitionType::Python(Method)) => {
                Some(RelationshipType::ClassToMethod)
            }
            (DefinitionType::Python(Class), DefinitionType::Python(Lambda)) => {
                Some(RelationshipType::ClassToLambda)
            }
            (DefinitionType::Python(Method), DefinitionType::Python(Class)) => {
                Some(RelationshipType::MethodToClass)
            }
            (DefinitionType::Python(Method), DefinitionType::Python(Function)) => {
                Some(RelationshipType::MethodToFunction)
            }
            (DefinitionType::Python(Method), DefinitionType::Python(Lambda)) => {
                Some(RelationshipType::MethodToLambda)
            }
            (DefinitionType::Python(Function), DefinitionType::Python(Function)) => {
                Some(RelationshipType::FunctionToFunction)
            }
            (DefinitionType::Python(Function), DefinitionType::Python(Class)) => {
                Some(RelationshipType::FunctionToClass)
            }
            (DefinitionType::Python(Function), DefinitionType::Python(Lambda)) => {
                Some(RelationshipType::FunctionToLambda)
            }
            (DefinitionType::Python(Lambda), DefinitionType::Python(Lambda)) => {
                Some(RelationshipType::LambdaToLambda)
            }
            (DefinitionType::Python(Lambda), DefinitionType::Python(Class)) => {
                Some(RelationshipType::LambdaToClass)
            }
            (DefinitionType::Python(Lambda), DefinitionType::Python(Function)) => {
                Some(RelationshipType::LambdaToFunction)
            }
            _ => None, // Unknown or unsupported relationship
        }
    }

    fn is_top_level_definition(&self, fqn: &PythonFqn) -> bool {
        fqn.len() == 1
    }
}
