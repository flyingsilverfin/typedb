/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashSet, sync::Arc};
use std::hash::Hash;
use std::marker::PhantomData;

use bytes::Bytes;
use encoding::{
    graph::type_::{
        Kind,
        vertex::{
            new_vertex_attribute_type, new_vertex_entity_type, new_vertex_relation_type, new_vertex_role_type,
        },
        vertex_generator::TypeVertexGenerator,
    }
    , value::{
        label::Label,
        value_type::ValueType,
    },
};
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::AnnotationCardinality,
        attribute_type::{AttributeType, AttributeTypeAnnotation},
        entity_type::{EntityType, EntityTypeAnnotation},
        owns::Owns,
        plays::Plays,
        relates::Relates,
        relation_type::{RelationType, RelationTypeAnnotation},
        role_type::{RoleType, RoleTypeAnnotation},
        type_cache::TypeCache, TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::annotation::Annotation;
use crate::type_::Ordering;
use crate::type_::owns::OwnsAnnotation;
use crate::type_::type_decoder::TypeDecoder;

// TODO: this should be parametrised into the database options? Would be great to have it be changable at runtime!
pub(crate) const RELATION_INDEX_THRESHOLD: u64 = 8;

pub struct TypeManager<Snapshot> {
    vertex_generator: Arc<TypeVertexGenerator>,
    type_cache: Option<Arc<TypeCache>>,
    snapshot: PhantomData<Snapshot>,
}

impl<Snapshot> TypeManager<Snapshot> {

    pub fn finalise(self) -> Result<(), Vec<ConceptWriteError>> {
        todo!("Do we need to finalise anything here?");
        Ok(())
    }
}

macro_rules! get_type_methods {
    ($(
        fn $method_name:ident() -> $output_type:ident = $cache_method:ident | $new_vertex_method:ident;
    )*) => {
        $(
            pub fn $method_name(
                &self, snapshot: &Snapshot, label: &Label<'_>
            ) -> Result<Option<$output_type<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(label))
                } else {
                    TypeDecoder::get_labelled_type::<$output_type<'static>>(snapshot, label)
                }
            }
        )*
    }
}

macro_rules! get_supertype_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<Option<$type_<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    TypeDecoder::get_supertype(snapshot, type_)
                }
            }
        )*
    }
}

macro_rules! get_supertypes_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let supertypes = TypeDecoder::get_supertypes_transitive(snapshot, type_)?;
                    Ok(MaybeOwns::owned(supertypes))
                }
            }
        )*
    }
}

macro_rules! get_subtypes_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let subtypes = TypeDecoder::get_subtypes(snapshot, type_)?;
                    Ok(MaybeOwns::owned(subtypes))
                }
            }
        )*
    }
}

macro_rules! get_subtypes_transitive_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            // WARN: supertypes currently do NOT include themselves
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Vec<$type_<'static>>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let subtypes = TypeDecoder::get_subtypes_transitive(snapshot, type_)?;
                    Ok(MaybeOwns::owned(subtypes))
                }
            }
        )*
    }
}

macro_rules! get_type_is_root_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $base_variant:expr;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<bool, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(cache.$cache_method(type_))
                } else {
                    let type_label = TypeDecoder::get_label(snapshot, type_)?.unwrap();
                    Ok(Self::check_type_is_root(&type_label, $base_variant))
                }
            }
        )*
    }
}

macro_rules! get_type_label_methods {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, Label<'static>>, ConceptReadError> {
                if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    Ok(MaybeOwns::owned(TypeDecoder::get_label(snapshot, type_)?.unwrap()))
                }
            }
        )*
    }
}

macro_rules! get_type_annotations {
    ($(
        fn $method_name:ident() -> $type_:ident = $cache_method:ident | $annotation_type:ident;
    )*) => {
        $(
            pub(crate) fn $method_name(
                &self, snapshot: &Snapshot, type_: $type_<'static>
            ) -> Result<MaybeOwns<'_, HashSet<$annotation_type>>, ConceptReadError> {
                 if let Some(cache) = &self.type_cache {
                    Ok(MaybeOwns::borrowed(cache.$cache_method(type_)))
                } else {
                    let mut annotations: HashSet<$annotation_type> = HashSet::new();
                    let annotations = TypeDecoder::get_type_annotations(snapshot, type_)?
                        .into_iter()
                        .map(|annotation| $annotation_type::from(annotation))
                        .collect();
                    Ok(MaybeOwns::owned(annotations))
                }
            }
        )*
    }
}

// TODO: The '_s is only here for the enforcement of pass-by-value of types. If we drop that, we can move it to the function signatures
impl<'_s, Snapshot: ReadableSnapshot> TypeManager<Snapshot>
    where '_s: 'static {
    pub fn new(
        vertex_generator: Arc<TypeVertexGenerator>,
        schema_cache: Option<Arc<TypeCache>>,
    ) -> Self {
        TypeManager { vertex_generator, type_cache: schema_cache, snapshot: PhantomData::default() }
    }

    pub(crate) fn check_type_is_root(type_label: &Label<'_>, kind: Kind) -> bool {
        type_label == &kind.root_label()
    }

    get_type_methods! {
        fn get_entity_type() -> EntityType = get_entity_type | new_vertex_entity_type;
        fn get_relation_type() -> RelationType = get_relation_type | new_vertex_relation_type;
        fn get_role_type() -> RoleType = get_role_type | new_vertex_role_type;
        fn get_attribute_type() -> AttributeType = get_attribute_type | new_vertex_attribute_type;
    }

    get_supertype_methods! {
        fn get_entity_type_supertype() -> EntityType = get_entity_type_supertype;
        fn get_relation_type_supertype() -> RelationType = get_relation_type_supertype;
        fn get_role_type_supertype() -> RoleType = get_role_type_supertype;
        fn get_attribute_type_supertype() -> AttributeType = get_attribute_type_supertype;
    }

    get_supertypes_methods! {
        fn get_entity_type_supertypes() -> EntityType = get_entity_type_supertypes;
        fn get_relation_type_supertypes() -> RelationType = get_relation_type_supertypes;
        fn get_role_type_supertypes() -> RoleType = get_role_type_supertypes;
        fn get_attribute_type_supertypes() -> AttributeType = get_attribute_type_supertypes;
    }

    get_subtypes_methods! {
        fn get_entity_type_subtypes() -> EntityType = get_entity_type_subtypes;
        fn get_relation_type_subtypes() -> RelationType = get_relation_type_subtypes;
        fn get_role_type_subtypes() -> RoleType = get_role_type_subtypes;
        fn get_attribute_type_subtypes() -> AttributeType = get_attribute_type_subtypes;
    }

    get_subtypes_transitive_methods! {
        fn get_entity_type_subtypes_transitive() -> EntityType = get_entity_type_subtypes_transitive;
        fn get_relation_type_subtypes_transitive() -> RelationType = get_relation_type_subtypes_transitive;
        fn get_role_type_subtypes_transitive() -> RoleType = get_role_type_subtypes_transitive;
        fn get_attribute_type_subtypes_transitive() -> AttributeType = get_attribute_type_subtypes_transitive;
    }

    get_type_is_root_methods! {
        fn get_entity_type_is_root() -> EntityType = get_entity_type_is_root | Kind::Entity;
        fn get_relation_type_is_root() -> RelationType = get_relation_type_is_root | Kind::Relation;
        fn get_role_type_is_root() -> RoleType = get_role_type_is_root | Kind::Role;
        fn get_attribute_type_is_root() -> AttributeType = get_attribute_type_is_root | Kind::Attribute;
    }

    get_type_label_methods! {
        fn get_entity_type_label() -> EntityType = get_entity_type_label;
        fn get_relation_type_label() -> RelationType = get_relation_type_label;
        fn get_role_type_label() -> RoleType = get_role_type_label;
        fn get_attribute_type_label() -> AttributeType = get_attribute_type_label;
    }

    pub(crate) fn get_entity_type_owns(
        &self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_owns(entity_type)))
        } else {
            let owns = TypeDecoder::get_owns(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_owns(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Owns<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_owns(relation_type)))
        } else {
            let owns = TypeDecoder::get_owns(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::owned(owns))
        }
    }

    pub(crate) fn get_relation_type_relates(
        &self,
        snapshot: &Snapshot,
        relation_type: RelationType<'static>,
    ) -> Result<MaybeOwns<'_, HashSet<Relates<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_relation_type_relates(relation_type)))
        } else {
            let relates = TypeDecoder::get_relates(snapshot, relation_type.clone())?;
            Ok(MaybeOwns::owned(relates))
        }
    }

    pub(crate) fn relation_index_available(&self, snapshot: &Snapshot, relation_type: RelationType<'_>) -> Result<bool, ConceptReadError> {
        // TODO: it would be good if this doesn't require recomputation
        let mut max_card = 0;
        let relates = relation_type.get_relates(snapshot, self)?;
        for relates in relates.iter() {
            let card = relates.role().get_cardinality(snapshot, self)?;
            match card.end() {
                None => return Ok(false),
                Some(end) => max_card += end,
            }
        };
        Ok(max_card <= RELATION_INDEX_THRESHOLD)
    }

    pub(crate) fn get_entity_type_plays<'this>(
        &'this self,
        snapshot: &Snapshot,
        entity_type: EntityType<'static>,
    ) -> Result<MaybeOwns<'this, HashSet<Plays<'static>>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_entity_type_plays(entity_type)))
        } else {
            let plays = TypeDecoder::get_plays(snapshot, entity_type.clone())?;
            Ok(MaybeOwns::owned(plays))
        }
    }

    pub(crate) fn get_attribute_type_value_type(
        &self,
        snapshot: &Snapshot,
        attribute_type: AttributeType<'static>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_attribute_type_value_type(attribute_type))
        } else {
            TypeDecoder::get_value_type(snapshot, attribute_type)
        }
    }

    get_type_annotations! {
        fn get_entity_type_annotations() -> EntityType = get_entity_type_annotations | EntityTypeAnnotation;
        fn get_relation_type_annotations() -> RelationType = get_relation_type_annotations | RelationTypeAnnotation;
        fn get_role_type_annotations() -> RoleType = get_role_type_annotations | RoleTypeAnnotation;
        fn get_attribute_type_annotations() -> AttributeType = get_attribute_type_annotations | AttributeTypeAnnotation;
    }

    pub(crate) fn get_owns_annotations<'this>(
        &'this self,
        snapshot: &Snapshot,
        owns: Owns<'this>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(MaybeOwns::borrowed(cache.get_owns_annotations(owns)))
        } else {
            let annotations: HashSet<OwnsAnnotation> = TypeDecoder::get_type_edge_annotations(snapshot, owns)?
                .into_iter()
                .map(|annotation| OwnsAnnotation::from(annotation))
                .collect();
            Ok(MaybeOwns::owned(annotations))
        }
    }

    pub(crate) fn get_owns_ordering(&self, snapshot: &Snapshot, owns: Owns<'_s>) -> Result<Ordering, ConceptReadError> {
        if let Some(cache) = &self.type_cache {
            Ok(cache.get_owns_ordering(owns))
        } else {
            TypeDecoder::get_type_edge_ordering(snapshot, owns)
        }
    }

    pub(crate) const fn role_default_cardinality(&self) -> AnnotationCardinality {
        // TODO: read from database properties the default role cardinality the db was created with
        AnnotationCardinality::new(1, Some(1))
    }
}

pub trait ReadableType<'a, 'b>: TypeAPI<'a> {
    // Consider replacing 'b with 'static
    type SelfRead: ReadableType<'b, 'b>;
    type AnnotationType: Hash + Eq + From<Annotation>;
    const ROOT_KIND: Kind;
    fn read_from(b: Bytes<'b, BUFFER_KEY_INLINE>) -> Self::SelfRead;
}

impl<'a, 'b> ReadableType<'a, 'b> for AttributeType<'a> {
    type SelfRead = AttributeType<'b>;
    type AnnotationType = AttributeTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Attribute;


    fn read_from(b: Bytes<'b, BUFFER_KEY_INLINE>) -> Self::SelfRead {
        AttributeType::new(new_vertex_attribute_type(b))
    }
}

impl<'a, 'b> ReadableType<'a, 'b> for EntityType<'a> {
    type SelfRead = EntityType<'b>;
    type AnnotationType = EntityTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Entity;
    fn read_from(b: Bytes<'b, BUFFER_KEY_INLINE>) -> Self::SelfRead {
        EntityType::new(new_vertex_entity_type(b))
    }
}

impl<'a, 'b> ReadableType<'a, 'b> for RelationType<'a> {
    type SelfRead = RelationType<'b>;
    type AnnotationType = RelationTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Relation;
    fn read_from(b: Bytes<'b, BUFFER_KEY_INLINE>) -> RelationType<'b> {
        RelationType::new(new_vertex_relation_type(b))
    }
}

impl<'a, 'b> ReadableType<'a, 'b> for RoleType<'a> {
    type SelfRead = RoleType<'b>;
    type AnnotationType = RoleTypeAnnotation;
    const ROOT_KIND: Kind = Kind::Role;
    fn read_from(b: Bytes<'b, BUFFER_KEY_INLINE>) -> RoleType<'b> {
        RoleType::new(new_vertex_role_type(b))
    }
}
