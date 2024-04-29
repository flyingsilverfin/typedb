/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use encoding::{
    graph::type_::vertex::TypeVertex,
    layout::prefix::Prefix,
    Prefixed,
    value::{label::Label, value_type::ValueType},
};
use encoding::graph::type_::Kind;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    ConceptAPI,
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationAbstract, AnnotationIndependent},
        owns::Owns,
        type_manager::TypeManager,
        TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::type_decoder::TypeDecoder;
use crate::type_::type_encoder;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType<'_> {
        if vertex.prefix() != Prefix::VertexAttributeType {
            panic!(
                "Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                Prefix::VertexAttributeType,
                vertex.prefix()
            )
        }
        AttributeType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    fn vertex<'this>(&'this self) -> TypeVertex<'this> {
        self.vertex.as_reference()
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }

    fn is_abstract<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let annotations = self.get_annotations(snapshot, type_manager)?;
        Ok(annotations.contains(&AttributeTypeAnnotation::Abstract(AnnotationAbstract::new())))
    }

    fn delete<Snapshot: WritableSnapshot>(
        self, snapshot: &mut Snapshot,
    ) -> Result<(), ConceptWriteError> {
        todo!()
    }
}

impl<'a> AttributeType<'a> {
    pub fn create_attribute_type<Snapshot: WritableSnapshot>(
        snapshot: &mut Snapshot,
        vertex_generator: &TypeVertexGenerator,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<AttributeType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = vertex_generator.create_attribute_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let attribute_type = AttributeType::new(type_vertex);
        type_encoder::set_label(snapshot, attribute_type.clone(), label);
        if !is_root {
            type_encoder::set_supertype(
                snapshot,
                attribute_type.clone(),
                TypeDecoder::get_attribute_type(snapshot, &Kind::Attribute.root_label()).unwrap().unwrap(),
            );
        }
        Ok(attribute_type)
    }

    pub fn is_root<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_attribute_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn set_value_type<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        value_type: ValueType,
    ) {
        type_encoder::set_value_type(snapshot, self.clone().into_owned(), value_type)
    }

    pub fn get_value_type<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        type_manager.get_attribute_type_value_type(snapshot, self.clone().into_owned())
    }

    pub fn get_label<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_label(snapshot, self.clone().into_owned())
    }

    pub fn set_label<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        if self.is_root(TypeDecoder::new(snapshot))? {
            Err(ConceptWriteError::RootModification)
        } else {
            Ok(type_encoder::set_label(snapshot, self.clone().into_owned(), label))
        }
    }

    pub fn get_supertype<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<AttributeType<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        supertype: AttributeType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_encoder::set_supertype(snapshot, self.clone().into_owned(), supertype);
        Ok(())
    }

    pub fn get_supertypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub(crate) fn is_independent<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        Ok(
            self
                .get_annotations(snapshot, type_manager)?
                .contains(&AttributeTypeAnnotation::Independent(AnnotationIndependent::new()))
        )
    }

    pub fn get_annotations<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<AttributeTypeAnnotation>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        annotation: AttributeTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_encoder::set_annotation_abstract(snapshot, self.clone().into_owned())
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_encoder::set_annotation_independent(snapshot, self.clone().into_owned())
            }
        };
        Ok(())
    }

    fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        annotation: AttributeTypeAnnotation,
    ) {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_encoder::delete_annotation_abstract(snapshot, self.clone().into_owned())
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_encoder::storage_storage_annotation_independent(snapshot, self.clone().into_owned())
            }
        }
    }

    pub fn into_owned(self) -> AttributeType<'static> {
        AttributeType { vertex: self.vertex.into_owned() }
    }
}

// --- Owned API ---
impl<'a> AttributeType<'a> {
    fn get_owns<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: Snapshot,
        _type_manager: &'m TypeManager<Snapshot>,
    ) -> MaybeOwns<'m, HashSet<Owns<'static>>> {
        todo!()
    }

    fn get_owns_owners<Snapshot: ReadableSnapshot>(&self) {
        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttributeTypeAnnotation {
    Abstract(AnnotationAbstract),
    Independent(AnnotationIndependent),
}

impl From<Annotation> for AttributeTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => AttributeTypeAnnotation::Abstract(annotation),
            Annotation::Independent(annotation) => AttributeTypeAnnotation::Independent(annotation),
            Annotation::Distinct(_) => unreachable!("Distinct annotation not available for Attribute type."),
            Annotation::Cardinality(_) => unreachable!("Cardinality annotation not available for Attribute type."),
        }
    }
}
