/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt};
use typeql::query::stage::Match;
use compiler::inference::TypeInferenceError;
use concept::error::ConceptReadError;
use function::FunctionError;
use ir::PatternDefinitionError;
use ir::program::FunctionDefinitionError;

use crate::define::DefineError;

#[derive(Debug)]
pub enum QueryError {
    ParseError { typeql_query: String, source: typeql::common::Error },
    ReadError { source: ConceptReadError },
    Define { source: DefineError },
    Pattern { source: PatternDefinitionError },
    Function { source: FunctionError },
    PipelineFunctionDefinition { source: FunctionDefinitionError },
    MatchWithFunctionsTypeInferenceFailure { clause: Match, source: TypeInferenceError },
}

impl fmt::Display for QueryError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for QueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ParseError { source, .. }
            | Self::ReadError { source, .. }
            | Self::Define { source, .. }
            | Self::Pattern { source, .. }
            | Self::Function { source, .. }
            | Self::PipelineFunctionDefinition { source, .. }
            | Self::MatchWithFunctionsTypeInferenceFailure { source, .. } => Some(source),
        }
    }
}
