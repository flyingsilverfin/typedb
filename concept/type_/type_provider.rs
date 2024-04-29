/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */



use std::collections::HashSet;

use encoding::value::label::Label;
use encoding::value::value_type::ValueType;

use crate::type_::attribute_type::{AttributeType, AttributeTypeAnnotation};
use crate::type_::entity_type::{EntityType, EntityTypeAnnotation};
use crate::type_::Ordering;
use crate::type_::owns::{Owns, OwnsAnnotation};
use crate::type_::plays::Plays;
use crate::type_::relates::Relates;
use crate::type_::relation_type::{RelationType, RelationTypeAnnotation};
use crate::type_::role_type::{RoleType, RoleTypeAnnotation};

pub(crate) trait TypeProvider {
    fn get_entity_type(&self, label: &Label<'_>) -> Option<EntityType<'static>>;
    fn get_relation_type(&self, label: &Label<'_>) -> Option<RelationType<'static>>;
    fn get_role_type(&self, label: &Label<'_>) -> Option<RoleType<'static>>;
    fn get_attribute_type(&self, label: &Label<'_>) -> Option<AttributeType<'static>>;
    fn get_entity_type_supertype(&self, entity_type: EntityType<'static>) -> Option<EntityType<'static>>;
    fn get_relation_type_supertype(
        &self,
        relation_type: RelationType<'static>,
    ) -> Option<RelationType<'static>>;
    fn get_role_type_supertype(&self, role_type: RoleType<'static>) -> Option<RoleType<'static>>;
    fn get_attribute_type_supertype(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> Option<AttributeType<'static>>;
    fn get_entity_type_supertypes(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>>;
    fn get_relation_type_supertypes(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>>;
    fn get_role_type_supertypes(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>>;
    fn get_attribute_type_supertypes(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>>;
    fn get_entity_type_subtypes(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>>;
    fn get_relation_type_subtypes(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>>;
    fn get_role_type_subtypes(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>>;
    fn get_attribute_type_subtypes(
        &self,
        attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>>;
    fn get_entity_type_subtypes_transitive(&self, entity_type: EntityType<'_>) -> &Vec<EntityType<'static>>;
    fn get_relation_type_subtypes_transitive(
        &self,
        relation_type: RelationType<'static>,
    ) -> &Vec<RelationType<'static>>;
    fn get_role_type_subtypes_transitive(&self, role_type: RoleType<'static>) -> &Vec<RoleType<'static>>;
    fn get_attribute_type_subtypes_transitive(&self,
                                              attribute_type: AttributeType<'static>,
    ) -> &Vec<AttributeType<'static>>;
    fn get_entity_type_label(&self, entity_type: EntityType<'static>) -> &Label<'static>;
    fn get_relation_type_label(&self, relation_type: RelationType<'static>) -> &Label<'static>;
    fn get_role_type_label(&self, role_type: RoleType<'static>) -> &Label<'static>;
    fn get_attribute_type_label(&self, attribute_type: AttributeType<'static>) -> &Label<'static>;
    fn get_entity_type_is_root(&self, entity_type: EntityType<'static>) -> bool;
    fn get_relation_type_is_root(&self, relation_type: RelationType<'static>) -> bool;
    fn get_role_type_is_root(&self, role_type: RoleType<'static>) -> bool;
    fn get_attribute_type_is_root(&self, attribute_type: AttributeType<'static>) -> bool;
    fn get_role_type_ordering(&self, role_type: RoleType<'static>) -> Ordering;
    fn get_entity_type_owns(&self, entity_type: EntityType<'static>) -> &HashSet<Owns<'static>>;
    fn get_relation_type_owns(&self, relation_type: RelationType<'static>) -> &HashSet<Owns<'static>>;
    fn get_relation_type_relates(&self, relation_type: RelationType<'static>) -> &HashSet<Relates<'static>>;
    fn get_entity_type_plays(&self, entity_type: EntityType<'static>) -> &HashSet<Plays<'static>>;
    fn get_relation_type_plays(&self, relation_type: RelationType<'static>) -> &HashSet<Plays<'static>>;
    fn get_attribute_type_value_type(&self, attribute_type: AttributeType<'static>) -> Option<ValueType>;
    fn get_entity_type_annotations(&self, entity_type: EntityType<'static>) -> &HashSet<EntityTypeAnnotation>;
    fn get_relation_type_annotations(&self, relation_type: RelationType<'static>) -> &HashSet<RelationTypeAnnotation>;
    fn get_role_type_annotations(&self, role_type: RoleType<'static>) -> &HashSet<RoleTypeAnnotation>;
    fn get_attribute_type_annotations(&self, attribute_type: AttributeType<'static>) -> &HashSet<AttributeTypeAnnotation>;
    fn get_owns_annotations<'c>(&'c self, owns: Owns<'c>) -> &'c HashSet<OwnsAnnotation>;
    fn get_owns_ordering<'c>(&'c self, owns: Owns<'c>) -> Ordering;
}


