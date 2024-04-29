/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */


use std::sync::Arc;
use bytes::byte_array::ByteArray;
use durability::DurabilityService;
use encoding::{AsBytes, Keyable};
use encoding::graph::type_::edge::{build_edge_owns, build_edge_owns_reverse, build_edge_plays, build_edge_plays_reverse, build_edge_relates, build_edge_relates_reverse, build_edge_sub, build_edge_sub_reverse, TypeEdge};
use encoding::graph::type_::index::LabelToTypeVertexIndex;
use encoding::graph::type_::Kind;
use encoding::graph::type_::property::{build_property_type_annotation_abstract, build_property_type_annotation_cardinality, build_property_type_annotation_distinct, build_property_type_annotation_independent, build_property_type_edge_annotation_cardinality, build_property_type_edge_annotation_distinct, build_property_type_edge_ordering, build_property_type_label, build_property_type_ordering, build_property_type_value_type};
use encoding::graph::type_::vertex_generator::TypeVertexGenerator;
use encoding::layout::prefix::Prefix;
use encoding::value::label::Label;
use encoding::value::value_type::ValueType;
use storage::MVCCStorage;
use storage::snapshot::{CommittableSnapshot, WritableSnapshot, WriteSnapshot};
use crate::error::ConceptWriteError;
use crate::type_::role_type::{RoleType, RoleTypeAnnotation};
use crate::type_::{IntoCanonicalTypeEdge, ObjectTypeAPI, Ordering, serialise_annotation_cardinality, serialise_ordering, TypeAPI};
use crate::type_::annotation::{AnnotationAbstract, AnnotationCardinality};
use crate::type_::attribute_type::{AttributeType, AttributeTypeAnnotation};
use crate::type_::entity_type::{EntityType, EntityTypeAnnotation};
use crate::type_::relation_type::{RelationType, RelationTypeAnnotation};
use crate::type_::type_decoder::TypeDecoder;
use crate::type_::type_manager::TypeManager;

pub fn initialise_types<D: DurabilityService>(
    storage: Arc<MVCCStorage<D>>,
    vertex_generator: Arc<TypeVertexGenerator>,
) -> Result<(), ConceptWriteError> {
    let mut snapshot = storage.clone().open_snapshot_write();
    {
        let type_manager = TypeManager::<WriteSnapshot<D>>::new(vertex_generator.clone(), None);
        let root_entity = type_manager.create_entity_type(&mut snapshot, &Kind::Entity.root_label(), true)?;
        root_entity.set_annotation(&mut snapshot, EntityTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
        let root_relation = type_manager.create_relation_type(&mut snapshot, &Kind::Relation.root_label(), true)?;
        root_relation.set_annotation(&mut snapshot, RelationTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
        let root_role = type_manager.create_role_type(
            &mut snapshot, &Kind::Role.root_label(), root_relation.clone(), true, Ordering::Unordered
        )?;
        root_role.set_annotation(&mut snapshot,RoleTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
        let root_attribute = type_manager.create_attribute_type(&mut snapshot, &Kind::Attribute.root_label(), true)?;
        root_attribute.set_annotation(&mut snapshot, AttributeTypeAnnotation::Abstract(AnnotationAbstract::new()))?;
    }
    // TODO: pass error up
    snapshot.commit().unwrap();
    Ok(())
}

pub(crate) fn delete_entity_type<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, entity_type: EntityType<'_>) {
    let key = entity_type.into_vertex().into_storage_key().into_owned_array();
    todo!("Do we need to lock?");
    snapshot.delete(key)
}

pub(crate) fn delete_relation_type<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, relation_type: RelationType<'_>) {
    let key = relation_type.into_vertex().into_storage_key().into_owned_array();
    todo!("Do we need to lock?");
    snapshot.delete(key)
}

pub(crate) fn delete_attribute_type<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, attribute_type: AttributeType<'_>) {
    let key = attribute_type.into_vertex().into_storage_key().into_owned_array();
    todo!("Do we need to lock?");
    snapshot.delete(key);
}

pub(crate) fn delete_role_type<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, role_type: RoleType<'_>) {
    let key = role_type.into_vertex().into_storage_key().into_owned_array();
    todo!("Do we need to lock?");
    snapshot.delete(key);
}

pub(crate) fn set_label<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owner: impl TypeAPI<'static>, label: &Label<'_>) {
    may_delete_label(snapshot, owner.clone());

    let vertex_to_label_key = build_property_type_label(owner.clone().into_vertex());
    let label_value = ByteArray::from(label.scoped_name().bytes());
    snapshot.put_val(vertex_to_label_key.into_storage_key().into_owned_array(), label_value);

    let label_to_vertex_key = LabelToTypeVertexIndex::build(label);
    let vertex_value = ByteArray::from(owner.into_vertex().bytes());
    snapshot.put_val(label_to_vertex_key.into_storage_key().into_owned_array(), vertex_value);
}

fn may_delete_label<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owner: impl TypeAPI<'static>) {
    let existing_label = TypeDecoder::get_label(snapshot, owner.clone()).unwrap();
    if let Some(label) = existing_label {
        let vertex_to_label_key = build_property_type_label(owner.into_vertex());
        snapshot.delete(vertex_to_label_key.into_storage_key().into_owned_array());
        let label_to_vertex_key = LabelToTypeVertexIndex::build(&label);
        snapshot.delete(label_to_vertex_key.into_storage_key().into_owned_array());
    }
}

pub(crate) fn set_role_ordering<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, role: RoleType<'_>, ordering: Ordering) {
    snapshot.put_val(
        build_property_type_ordering(role.into_vertex()).into_storage_key().into_owned_array(),
        ByteArray::boxed(serialise_ordering(ordering)),
    )
}

pub(crate) fn set_supertype<Snapshot: WritableSnapshot, K: TypeAPI<'static>>(
    snapshot: &mut Snapshot, subtype: K, supertype: K,
) {
    may_delete_supertype(snapshot, subtype.clone());
    let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone().into_vertex());
    snapshot.put(sub.into_storage_key().into_owned_array());
    let sub_reverse = build_edge_sub_reverse(supertype.into_vertex(), subtype.into_vertex());
    snapshot.put(sub_reverse.into_storage_key().into_owned_array());
}

fn may_delete_supertype<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, subtype: impl TypeAPI<'static>) {
    let supertype_vertex = TypeDecoder::get_supertype_vertex(snapshot, subtype.clone().into_vertex()).unwrap();
    if let Some(supertype) = supertype_vertex {
        let sub = build_edge_sub(subtype.clone().into_vertex(), supertype.clone());
        snapshot.delete(sub.into_storage_key().into_owned_array());
        let sub_reverse = build_edge_sub_reverse(supertype, subtype.into_vertex());
        snapshot.delete(sub_reverse.into_storage_key().into_owned_array());
    }
}

pub(crate) fn set_owns<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owner: impl ObjectTypeAPI<'static>, attribute: AttributeType<'static>, ordering: Ordering) {
    let owns = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
    snapshot.put(owns.clone().into_storage_key().into_owned_array());
    let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
    snapshot.put(owns_reverse.into_storage_key().into_owned_array());
    set_owns_ordering(snapshot, owns, ordering);
}

pub(crate) fn set_owns_ordering<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owns_edge: TypeEdge<'_>, ordering: Ordering) {
    debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
    snapshot.put_val(
        build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array(),
        ByteArray::boxed(serialise_ordering(ordering)),
    )
}

pub(crate) fn delete_owns<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owner: impl ObjectTypeAPI<'static>, attribute: AttributeType<'static>) {
    let owns_edge = build_edge_owns(owner.clone().into_vertex(), attribute.clone().into_vertex());
    snapshot.delete(owns_edge.as_storage_key().into_owned_array());
    let owns_reverse = build_edge_owns_reverse(attribute.into_vertex(), owner.into_vertex());
    snapshot.delete(owns_reverse.into_storage_key().into_owned_array());
    delete_owns_ordering(snapshot, owns_edge);
}

pub(crate) fn delete_owns_ordering<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, owns_edge: TypeEdge<'_>) {
    debug_assert_eq!(owns_edge.prefix(), Prefix::EdgeOwns);
    snapshot.delete(
        build_property_type_edge_ordering(owns_edge).into_storage_key().into_owned_array(),
    )
}

pub(crate) fn set_plays<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, player: impl ObjectTypeAPI<'static>, role: RoleType<'static>) {
    let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
    snapshot.put(plays.into_storage_key().into_owned_array());
    let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
    snapshot.put(plays_reverse.into_storage_key().into_owned_array());
}

pub(crate) fn delete_plays<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, player: impl ObjectTypeAPI<'static>, role: RoleType<'static>) {
    let plays = build_edge_plays(player.clone().into_vertex(), role.clone().into_vertex());
    snapshot.delete(plays.into_storage_key().into_owned_array());
    let plays_reverse = build_edge_plays_reverse(role.into_vertex(), player.into_vertex());
    snapshot.delete(plays_reverse.into_storage_key().into_owned_array());
}

pub(crate) fn set_relates<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, relation: RelationType<'static>, role: RoleType<'static>) {
    let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
    snapshot.put(relates.into_storage_key().into_owned_array());
    let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
    snapshot.put(relates_reverse.into_storage_key().into_owned_array());
}

pub(crate) fn delete_relates<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, relation: RelationType<'static>, role: RoleType<'static>) {
    let relates = build_edge_relates(relation.clone().into_vertex(), role.clone().into_vertex());
    snapshot.delete(relates.into_storage_key().into_owned_array());
    let relates_reverse = build_edge_relates_reverse(role.into_vertex(), relation.into_vertex());
    snapshot.delete(relates_reverse.into_storage_key().into_owned_array());
}

pub(crate) fn set_value_type<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, attribute: AttributeType<'static>, value_type: ValueType) {
    let property_key =
        build_property_type_value_type(attribute.into_vertex()).into_storage_key().into_owned_array();
    let property_value = ByteArray::copy(&value_type.value_type_id().bytes());
    snapshot.put_val(property_key, property_value);
}

pub(crate) fn set_annotation_abstract<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
    snapshot.put(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn delete_annotation_abstract<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_abstract(type_.into_vertex());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn set_annotation_distinct<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
    snapshot.put(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn delete_annotation_distinct<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_distinct(type_.into_vertex());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn set_edge_annotation_distinct<'b, Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, edge: impl IntoCanonicalTypeEdge<'b>) {
    let annotation_property = build_property_type_edge_annotation_distinct(edge.into_type_edge());
    snapshot.put(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn delete_edge_annotation_distinct<'b, Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, edge: impl IntoCanonicalTypeEdge<'b>) {
    let annotation_property = build_property_type_edge_annotation_distinct(edge.into_type_edge());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn set_annotation_independent<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
    snapshot.put(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn storage_storage_annotation_independent<Snapshot: WritableSnapshot>(snapshot: &mut Snapshot, type_: impl TypeAPI<'static>) {
    let annotation_property = build_property_type_annotation_independent(type_.into_vertex());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn set_annotation_cardinality<Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    type_: impl TypeAPI<'static>,
    annotation: AnnotationCardinality,
) {
    snapshot
        .put_val(
            build_property_type_annotation_cardinality(type_.into_vertex()).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_annotation_cardinality(annotation)),
        );
}

pub(crate) fn delete_annotation_cardinality<Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    type_: impl TypeAPI<'static>,
) {
    let annotation_property = build_property_type_annotation_cardinality(type_.into_vertex());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}

pub(crate) fn set_edge_annotation_cardinality<'b, Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    edge: impl IntoCanonicalTypeEdge<'b>,
    annotation: AnnotationCardinality,
) {
    snapshot
        .put_val(
            build_property_type_edge_annotation_cardinality(edge.into_type_edge()).into_storage_key().into_owned_array(),
            ByteArray::boxed(serialise_annotation_cardinality(annotation)),
        );
}

pub(crate) fn delete_edge_annotation_cardinality<'b, Snapshot: WritableSnapshot>(
    snapshot: &mut Snapshot,
    edge: impl IntoCanonicalTypeEdge<'b>,
) {
    let annotation_property = build_property_type_edge_annotation_cardinality(edge.into_type_edge());
    snapshot.delete(annotation_property.into_storage_key().into_owned_array());
}