/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use bytes::Bytes;
use encoding::{
    graph::type_::vertex::{new_vertex_relation_type, TypeVertex},
    layout::prefix::Prefix,
    Prefixed,
    value::label::Label,
};
use encoding::graph::type_::Kind;
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use primitive::maybe_owns::MaybeOwns;
use storage::{
    key_value::StorageKeyReference,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    concept_iterator,
    ConceptAPI,
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationAbstract},
        attribute_type::AttributeType,
        object_type::ObjectType,
        ObjectTypeAPI,
        OwnerAPI,
        owns::Owns,
        PlayerAPI,
        plays::Plays,
        relates::Relates, role_type::RoleType, type_manager::TypeManager, TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::{Ordering, type_encoder};
use crate::type_::type_decoder::TypeDecoder;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct RelationType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> RelationType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> RelationType<'_> {
        if vertex.prefix() != Prefix::VertexRelationType {
            panic!(
                "Type IID prefix was expected to be Prefix::RelationType ({:?}) but was {:?}",
                Prefix::VertexRelationType,
                vertex.prefix()
            )
        }
        RelationType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for RelationType<'a> {}

impl<'a> TypeAPI<'a> for RelationType<'a> {
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
        Ok(annotations.contains(&RelationTypeAnnotation::Abstract(AnnotationAbstract::new())))
    }

    fn delete<Snapshot: WritableSnapshot>(
        self,
        snapshot: &mut Snapshot,
    ) -> Result<(), ConceptWriteError> {
        todo!()
    }
}

impl<'a> ObjectTypeAPI<'a> for RelationType<'a> {}

impl<'a> RelationType<'a> {
    pub fn create_relation_type<Snapshot: WritableSnapshot>(
        snapshot: &mut Snapshot,
        vertex_generator: &TypeVertexGenerator,
        label: &Label<'_>,
        is_root: bool,
    ) -> Result<RelationType<'static>, ConceptWriteError> {
        // TODO: validate type doesn't exist already
        let type_vertex = vertex_generator.create_relation_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let relation = RelationType::new(type_vertex);
        type_encoder::set_label(snapshot, relation.clone(), label);
        if !is_root {
            type_encoder::set_supertype(
                snapshot,
                relation.clone(),
                TypeDecoder::get_relation_type(snapshot, &Kind::Relation.root_label()).unwrap().unwrap(),
            );
        }
        Ok(relation)
    }

    pub fn is_root<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        type_manager.get_relation_type_is_root(snapshot, self.clone().into_owned())
    }

    pub fn get_label<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_relation_type_label(snapshot, self.clone().into_owned())
    }

    pub fn set_label<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        label: &Label<'_>,
    ) -> Result<(), ConceptWriteError> {
        if self.is_root(snapshot, type_manager)? {
            Err(ConceptWriteError::RootModification)
        } else {
            Ok(type_encoder::set_label(snapshot, self.clone().into_owned(), label))
        }
    }

    pub fn get_supertype<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Option<RelationType<'static>>, ConceptReadError> {
        type_manager.get_relation_type_supertype(snapshot, self.clone().into_owned())
    }

    pub fn set_supertype<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        supertype: RelationType<'static>,
    ) -> Result<(), ConceptWriteError> {
        type_encoder::set_supertype(snapshot, self.clone().into_owned(), supertype);
        Ok(())
    }

    pub fn get_supertypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_supertypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_subtypes(snapshot, self.clone().into_owned())
    }

    pub fn get_subtypes_transitive<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, Vec<RelationType<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_subtypes_transitive(snapshot, self.clone().into_owned())
    }

    pub fn get_annotations<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<RelationTypeAnnotation>>, ConceptReadError> {
        type_manager.get_relation_type_annotations(snapshot, self.clone().into_owned())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        annotation: RelationTypeAnnotation,
    ) -> Result<(), ConceptWriteError> {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_encoder::set_annotation_abstract(snapshot, self.clone().into_owned())
            }
        };
        Ok(())
    }

    fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        annotation: RelationTypeAnnotation,
    ) {
        match annotation {
            RelationTypeAnnotation::Abstract(_) => {
                type_encoder::delete_annotation_abstract(snapshot, self.clone().into_owned())
            }
        }
    }

    pub fn get_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        name: &str,
    ) -> Result<Option<RoleType<'static>>, ConceptReadError> {
        let label = Label::build_scoped(name, self.get_label(snapshot, type_manager)?.name().as_str());
        type_manager.get_role_type(snapshot, &label)
    }

    pub fn create_relates<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        vertex_generator: &TypeVertexGenerator,
        name: &str,
        ordering: Ordering,
    ) -> Result<RoleType<'static>, ConceptWriteError> {
        let label = Label::build_scoped(name, self.get_label(snapshot, type_manager).unwrap().name().as_str());

        // TODO: validate type doesn't exist already
        let type_vertex = vertex_generator.create_role_type(snapshot)
            .map_err(|err| ConceptWriteError::Encoding { source: err })?;
        let role = RoleType::new(type_vertex);
        type_encoder::set_label(snapshot, role.clone(), &label);
        type_encoder::set_relates(snapshot, self.clone(), role.clone());
        type_encoder::set_role_ordering(snapshot, role.clone(), ordering);
        if label != Kind::Role.root_label() {
            type_encoder::set_supertype(
                snapshot, role.clone(), self.get_role_type(snapshot, &Kind::Role.root_label()).unwrap().unwrap(),
            );
        }
        Ok(role)
    }

    fn delete_relates<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        role_type: RoleType<'static>,
    ) {
        type_encoder::delete_relates(snapshot, self.clone().into_owned(), role_type)
    }

    pub(crate) fn get_relates<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Relates<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_relates(snapshot, self.clone().into_owned())
    }

    pub fn get_relates_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        name: &str,
    ) -> Result<Option<Relates<'static>>, ConceptReadError> {
        Ok(self.get_role(snapshot, type_manager, name)?.map(|role_type| Relates::new(self.clone().into_owned(), role_type)))
    }

    fn has_relates_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        name: &str,
    ) -> Result<bool, ConceptReadError> {
        Ok(self.get_relates_role(snapshot, type_manager, name)?.is_some())
    }

    fn into_owned(self) -> RelationType<'static> {
        RelationType { vertex: self.vertex.into_owned() }
    }
}

impl<'a> OwnerAPI<'a> for RelationType<'a> {
    fn set_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        attribute_type: AttributeType<'static>,
        ordering: Ordering,
    ) -> Owns<'static> {
        type_encoder::set_owns(snapshot, self.clone().into_owned(), attribute_type.clone(), ordering);
        Owns::new(ObjectType::Relation(self.clone().into_owned()), attribute_type)
    }

    fn delete_owns<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        attribute_type: AttributeType<'static>,
    ) {
        // TODO: error if not owned?
        type_encoder::delete_owns(snapshot, self.clone().into_owned(), attribute_type)
    }

    fn get_owns<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Owns<'static>>>, ConceptReadError> {
        type_manager.get_relation_type_owns(snapshot, self.clone().into_owned())
    }

    fn get_owns_attribute<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<Owns<'static>>, ConceptReadError> {
        let expected_owns = Owns::new(ObjectType::Relation(self.clone().into_owned()), attribute_type);
        Ok(self.get_owns(snapshot, type_manager)?.contains(&expected_owns).then_some(expected_owns))
    }
}

impl<'a> PlayerAPI<'a> for RelationType<'a> {
    fn set_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        role_type: RoleType<'static>,
    ) -> Plays<'static> {
        // TODO: decide behaviour (ok or error) if already playing
        type_encoder::set_plays(snapshot, self.clone().into_owned(), role_type.clone());
        Plays::new(ObjectType::Relation(self.clone().into_owned()), role_type)
    }

    fn delete_plays<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        role_type: RoleType<'static>,
    ) {
        // TODO: error if not playing?
        type_encoder::delete_plays(snapshot, self.clone().into_owned(), role_type)
    }

    fn get_plays<'m, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        _type_manager: &'m TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<Plays<'static>>>, ConceptReadError> {
        todo!()
        // type_manager.get_relation_type_plays(self.clone().into_owned())
    }

    fn get_plays_role<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
        role_type: RoleType<'static>,
    ) -> Result<Option<Plays<'static>>, ConceptReadError> {
        let expected_plays = Plays::new(ObjectType::Relation(self.clone().into_owned()), role_type);
        Ok(self.get_plays(snapshot, type_manager)?.contains(&expected_plays).then_some(expected_plays))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RelationTypeAnnotation {
    Abstract(AnnotationAbstract),
}

impl From<Annotation> for RelationTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => RelationTypeAnnotation::Abstract(annotation),
            Annotation::Distinct(_) => unreachable!("Distinct annotation not available for Relation type."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Relation type."),
            Annotation::Cardinality(_) => unreachable!("Cardinality annotation not available for Relation type."),
        }
    }
}

// TODO: can we inline this into the macro invocation?
fn storage_key_to_relation_type(storage_key_ref: StorageKeyReference<'_>) -> RelationType<'_> {
    RelationType::new(new_vertex_relation_type(Bytes::Reference(storage_key_ref.byte_ref())))
}

concept_iterator!(RelationTypeIterator, RelationType, storage_key_to_relation_type);
