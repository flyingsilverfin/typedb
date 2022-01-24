/*
 * Copyright (C) 2021 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.logic.tool;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.parameters.Label;
import com.vaticle.typedb.core.graph.GraphManager;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.iid.VertexIID;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;
import com.vaticle.typedb.core.logic.LogicCache;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.Disjunction;
import com.vaticle.typedb.core.pattern.constraint.thing.HasConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IIDConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.IsaConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.RelationConstraint;
import com.vaticle.typedb.core.pattern.constraint.thing.ValueConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.OwnsConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.PlaysConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RegexConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.RelatesConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.SubConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.TypeConstraint;
import com.vaticle.typedb.core.pattern.constraint.type.ValueTypeConstraint;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.TraversalEngine;
import com.vaticle.typedb.core.traversal.common.Identifier;
import com.vaticle.typedb.core.traversal.common.Identifier.Variable.Retrievable;
import com.vaticle.typedb.core.traversal.graph.TraversalVertex;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.vaticle.typedb.common.collection.Collections.set;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN_VARIABLE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_PATTERN_VARIABLE_VALUE;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Pattern.UNSATISFIABLE_SUB_PATTERN;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.ROLE_TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.TypeRead.TYPE_NOT_FOUND;
import static com.vaticle.typedb.core.common.iterator.Iterators.empty;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Type.ATTRIBUTE;

public class TypeInference {

    private final LogicCache logicCache;
    private final GraphManager graphMgr;
    private final TraversalEngine traversalEng;

    public TypeInference(LogicCache logicCache, TraversalEngine traversalEng, GraphManager graphMgr) {
        this.logicCache = logicCache;
        this.graphMgr = graphMgr;
        this.traversalEng = traversalEng;
    }

    public void infer(Disjunction disjunction) {
        infer(disjunction, new HashMap<>());
    }

    private void infer(Disjunction disjunction, Map<Identifier.Variable.Name, Set<Label>> namedInferences) {
        disjunction.conjunctions().forEach(conjunction -> infer(conjunction, namedInferences, false));
    }

    public void infer(Conjunction conjunction) {
        infer(conjunction, false);
    }

    public void infer(Conjunction conjunction, boolean insertable) {
        infer(conjunction, new HashMap<>(), insertable);
    }

    private void infer(Conjunction conjunction, Map<Identifier.Variable.Name, Set<Label>> scopingInferences, boolean insertable) {
        propagateLabels(conjunction, graphMgr);
        if (isSchemaQuery(conjunction)) return;
        ConjunctionInference.run(conjunction, scopingInferences, insertable, logicCache, graphMgr, traversalEng);
        if (!conjunction.negations().isEmpty()) {
            Map<Retrievable.Name, Set<Label>> namedInferences = namedInferences(conjunction);
            namedInferences.putAll(scopingInferences);
            conjunction.negations().forEach(negation -> infer(negation.disjunction(), namedInferences));
        }
    }

    private static void propagateLabels(Conjunction conj, GraphManager graphMgr) {
        iterate(conj.variables()).filter(v -> v.isType() && v.asType().label().isPresent()).forEachRemaining(typeVar -> {
            Label label = typeVar.asType().label().get().properLabel();
            if (label.scope().isPresent()) {
                String scope = label.scope().get();
                Set<Label> labels = graphMgr.schema().resolveRoleTypeLabels(label);
                if (labels.isEmpty()) throw TypeDBException.of(ROLE_TYPE_NOT_FOUND, label.name(), scope);
                typeVar.addInferredTypes(labels);
            } else {
                TypeVertex type = graphMgr.schema().getType(label);
                if (type == null) throw TypeDBException.of(TYPE_NOT_FOUND, label);
                typeVar.addInferredTypes(label);
            }
        });
    }

    private Map<Retrievable.Name, Set<Label>> namedInferences(Conjunction conjunction) {
        Map<Retrievable.Name, Set<Label>> namedInferences = new HashMap<>();
        iterate(conjunction.variables()).filter(var -> var.id().isName()).forEachRemaining(var -> {
            namedInferences.put(var.id().asName(), var.inferredTypes());
        });
        return namedInferences;
    }

    private boolean isSchemaQuery(Conjunction conjunction) {
        return iterate(conjunction.variables()).noneMatch(Variable::isThing);
    }

    private static class ConjunctionInference {

        private static final Identifier.Variable ROOT_ATTRIBUTE_ID = Identifier.Variable.label(ATTRIBUTE.toString());

        private final Conjunction conjunction;
        private final Map<Identifier.Variable.Name, Set<Label>> scopingInferences;
        private final LogicCache logicCache;
        private final GraphManager graphMgr;
        private final TraversalEngine traversalEng;
        private final boolean insertable;

        private final GraphTraversal.Type traversal;
        private final Map<Identifier.Variable, TypeVariable> originalToInference;
        private final Map<Identifier.Variable, Variable> inferenceToOriginal;
        private int nextGeneratedID;

        private ConjunctionInference(Conjunction conjunction, Map<Identifier.Variable.Name, Set<Label>> scopingInferences,
                                     boolean insertable, LogicCache logicCache, GraphManager graphMgr, TraversalEngine traversalEng) {
            this.conjunction = conjunction;
            this.scopingInferences = scopingInferences;
            this.logicCache = logicCache;
            this.graphMgr = graphMgr;
            this.traversalEng = traversalEng;
            this.insertable = insertable;
            this.traversal = new GraphTraversal.Type();
            this.originalToInference = new HashMap<>();
            this.inferenceToOriginal = new HashMap<>();
            this.nextGeneratedID = largestAnonymousVar(conjunction) + 1;
            conjunction.variables().forEach(this::toInferenceVar);
            traversal.filter(iterate(inferenceToOriginal.keySet()).filter(Identifier::isRetrievable).map(Identifier.Variable::asRetrievable).toSet());
        }

        static void run(Conjunction conjunction, Map<Identifier.Variable.Name, Set<Label>> scopingInferences,
                        boolean insertable, LogicCache logicCache, GraphManager graphMgr, TraversalEngine traversalEng) {
            ConjunctionInference conjunctionInference = new ConjunctionInference(
                    conjunction, scopingInferences, insertable, logicCache, graphMgr, traversalEng
            );
            conjunctionInference.runInference();
        }

        private static int largestAnonymousVar(Conjunction conjunction) {
            return iterate(conjunction.variables()).filter(var -> var.id().isAnonymous())
                    .map(var -> var.id().asAnonymous().anonymousId()).stream().max(Comparator.naturalOrder()).orElse(0);
        }

        private void runInference() {
            Optional<Map<Retrievable, Set<Label>>> inferredTypes = logicCache.inference().get(
                    traversal,
                    traversal -> traversalEng.combination(traversal, thingInferenceVars()).map(this::toLabels)
            );
            if (inferredTypes.isPresent()) applyInferredTypes(inferredTypes.get());
            else conjunction.setCoherent(false);
        }

        private Set<Identifier.Variable.Retrievable> thingInferenceVars() {
            return iterate(conjunction.variables()).filter(Variable::isThing).map(var ->
                    originalToInference.get(var.id()).id().asRetrievable()
            ).toSet();
        }

        private void applyInferredTypes(Map<Retrievable, Set<Label>> inferredTypes) {
            conjunction.variables().forEach(var -> {
                Identifier.Variable inferenceID = originalToInference.get(var.id()).id();
                if (inferenceID != null && inferenceID.isRetrievable()) {
                    var.setInferredTypes(inferredTypes.get(inferenceID.asRetrievable()));
                }
            });
        }

        private Map<Retrievable, Set<Label>> toLabels(Map<Retrievable, Set<TypeVertex>> types) {
            HashMap<Retrievable, Set<Label>> labels = new HashMap<>();
            types.forEach((id, ts) -> labels.put(id, iterate(ts).map(TypeVertex::properLabel).toSet()));
            return labels;
        }

        private void toInferenceVar(Variable variable) {
            if (variable.isType()) toInferenceVar(variable.asType());
            else if (variable.isThing()) toInferenceVar(variable.asThing());
            else throw TypeDBException.of(ILLEGAL_STATE);
        }

        private TypeVariable toInferenceVar(TypeVariable var) {
            if (originalToInference.containsKey(var.id())) return originalToInference.get(var.id());

            TypeVariable inferenceVar = new TypeVariable(generateVariableID());
            originalToInference.put(var.id(), inferenceVar);
            inferenceToOriginal.putIfAbsent(inferenceVar.id(), var);
            if (!var.inferredTypes().isEmpty()) restrictTypes(inferenceVar.id(), iterate(var.inferredTypes()));
            if (var.id().isName() && scopingInferences.containsKey(var.id().asName())) {
                restrictTypes(inferenceVar.id(), iterate(scopingInferences.get(var.id().asName())));
            }

            for (TypeConstraint constraint : var.constraints()) {
                if (constraint.isAbstract()) registerAbstract(inferenceVar);
                else if (constraint.isIs()) registerIsType(inferenceVar, constraint.asIs());
                else if (constraint.isOwns()) registerOwns(inferenceVar, constraint.asOwns());
                else if (constraint.isPlays()) registerPlays(inferenceVar, constraint.asPlays());
                else if (constraint.isRegex()) registerRegex(inferenceVar, constraint.asRegex());
                else if (constraint.isRelates()) registerRelates(inferenceVar, constraint.asRelates());
                else if (constraint.isSub()) registerSub(inferenceVar, constraint.asSub());
                else if (constraint.isValueType()) registerValueType(inferenceVar, constraint.asValueType());
                else if (!constraint.isLabel()) throw TypeDBException.of(ILLEGAL_STATE);
            }
            return inferenceVar;
        }

        private void registerAbstract(TypeVariable resolver) {
            traversal.isAbstract(resolver.id());
        }

        private void registerIsType(TypeVariable resolver, com.vaticle.typedb.core.pattern.constraint.type.IsConstraint isConstraint) {
            traversal.equalTypes(resolver.id(), toInferenceVar(isConstraint.variable()).id());
        }

        private void registerOwns(TypeVariable inferenceVar, OwnsConstraint ownsConstraint) {
            TypeVariable attrVar = toInferenceVar(ownsConstraint.attribute());
            traversal.owns(inferenceVar.id(), attrVar.id(), ownsConstraint.isKey());
        }

        private void registerPlays(TypeVariable inferenceVar, PlaysConstraint playsConstraint) {
            TypeVariable roleVar = toInferenceVar(playsConstraint.role());
            traversal.plays(inferenceVar.id(), roleVar.id());
        }

        private void registerRegex(TypeVariable inferenceVar, RegexConstraint regexConstraint) {
            traversal.regex(inferenceVar.id(), regexConstraint.regex().pattern());
            restrictTypes(inferenceVar.id(), graphMgr.schema().attributeTypes(Encoding.ValueType.STRING)
                    .map(TypeVertex::properLabel));
        }

        private void registerRelates(TypeVariable inferenceVar, RelatesConstraint relatesConstraint) {
            TypeVariable roleVar = toInferenceVar(relatesConstraint.role());
            traversal.relates(inferenceVar.id(), roleVar.id());
        }

        private void registerSub(TypeVariable inferenceVar, SubConstraint subConstraint) {
            TypeVariable superVar = toInferenceVar(subConstraint.type());
            traversal.sub(inferenceVar.id(), superVar.id(), !subConstraint.isExplicit());
        }

        private void registerValueType(TypeVariable resolver, ValueTypeConstraint valueTypeConstraint) {
            traversal.valueType(resolver.id(), valueTypeConstraint.valueType());
            restrictTypesByValueType(resolver, set(Encoding.ValueType.of(valueTypeConstraint.valueType())));
        }


        private TypeVariable toInferenceVar(ThingVariable var) {
            if (originalToInference.containsKey(var.id())) return originalToInference.get(var.id());

            TypeVariable inferenceVar = new TypeVariable(var.id());
            originalToInference.put(var.id(), inferenceVar);
            inferenceToOriginal.putIfAbsent(inferenceVar.id(), var);
            if (var.id().isName() && scopingInferences.containsKey(var.id().asName())) {
                restrictTypes(inferenceVar.id(), iterate(scopingInferences.get(var.id().asName())));
            }

            var.value().forEach(constraint -> registerValue(inferenceVar, constraint));
            var.isa().ifPresent(constraint -> registerIsa(inferenceVar, constraint));
            var.is().forEach(constraint -> registerIs(inferenceVar, constraint));
            var.has().forEach(constraint -> registerHas(inferenceVar, constraint));
            if (insertable)
                var.relation().ifPresent(constraint -> registerInsertableRelation(inferenceVar, constraint));
            else var.relation().ifPresent(constraint -> registerRelation(inferenceVar, constraint));
            var.iid().ifPresent(constraint -> registerIID(inferenceVar, constraint));
            return inferenceVar;
        }

        private void registerValue(TypeVariable inferenceVar, ValueConstraint<?> constraint) {
            Set<Encoding.ValueType> comparableValueTypes;
            if (constraint.isVariable()) {
                TypeVariable comparableVar = toInferenceVar(constraint.asVariable().value());
                registerSubAttribute(comparableVar);
                comparableValueTypes = traversal.structure().typeVertex(comparableVar.id()).props().valueTypes();
            } else {
                comparableValueTypes = set(Encoding.ValueType.of(constraint.value().getClass()));
            }

            Set<Encoding.ValueType> valueTypes = iterate(comparableValueTypes)
                    .flatMap(valueType -> iterate(valueType.comparables())).toSet();
            if (!valueTypes.isEmpty()) {
                restrictValueTypes(inferenceVar.id(), iterate(valueTypes));
                restrictTypesByValueType(inferenceVar, valueTypes);
            }
            registerSubAttribute(inferenceVar);
        }

        private void registerIsa(TypeVariable inferenceVar, IsaConstraint isaConstraint) {
            if (!isaConstraint.isExplicit() && !insertable) {
                traversal.sub(inferenceVar.id(), toInferenceVar(isaConstraint.type()).id(), true);
            } else if (isaConstraint.type().id().isName() || isaConstraint.type().id().isLabel()) {
                traversal.equalTypes(inferenceVar.id(), toInferenceVar(isaConstraint.type()).id());
            } else {
                throw TypeDBException.of(ILLEGAL_STATE);
            }
        }

        private void registerIs(TypeVariable inferenceVar, IsConstraint isConstraint) {
            traversal.equalTypes(inferenceVar.id(), toInferenceVar(isConstraint.variable()).id());
        }

        /**
         * We only extract the type of the IID, and include this information in the type inference
         * We only look at the type prefix because the static type checker (this class) only read schema, not data
         */
        private void registerIID(TypeVariable resolver, IIDConstraint constraint) {
            TypeVertex type = graphMgr.schema().convert(VertexIID.Thing.of(constraint.iid()).type());
            if (type == null) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_SUB_PATTERN, conjunction, constraint);
            }
            restrictTypes(resolver.id(), iterate(type).map(TypeVertex::properLabel));
        }

        private void registerHas(TypeVariable inferenceVar, HasConstraint hasConstraint) {
            TypeVariable attrVar = toInferenceVar(hasConstraint.attribute());
            traversal.owns(inferenceVar.id(), attrVar.id(), false);
            registerSubAttribute(attrVar);
        }

        private void registerRelation(TypeVariable inferenceVar, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerVar = toInferenceVar(rolePlayer.player());
                TypeVariable roleInstanceVar = new TypeVariable(generateVariableID());
                if (rolePlayer.roleType().isPresent()) {
                    TypeVariable roleTypeVar = toInferenceVar(rolePlayer.roleType().get());
                    traversal.sub(roleInstanceVar.id(), roleTypeVar.id(), true);
                }
                traversal.relates(inferenceVar.id(), roleInstanceVar.id());
                traversal.plays(playerVar.id(), roleInstanceVar.id());
            }
        }

        private void registerInsertableRelation(TypeVariable inferenceVar, RelationConstraint constraint) {
            for (RelationConstraint.RolePlayer rolePlayer : constraint.players()) {
                TypeVariable playerVar = toInferenceVar(rolePlayer.player());
                TypeVariable roleTypeVar;
                if (rolePlayer.roleType().isPresent()) {
                    roleTypeVar = toInferenceVar(rolePlayer.roleType().get());
                } else {
                    roleTypeVar = new TypeVariable(generateVariableID());
                }
                traversal.relates(inferenceVar.id(), roleTypeVar.id());
                traversal.plays(playerVar.id(), roleTypeVar.id());
            }
        }

        private void registerSubAttribute(Variable inferenceVar) {
            traversal.labels(ROOT_ATTRIBUTE_ID, Label.of(ATTRIBUTE.toString()));
            traversal.sub(inferenceVar.id(), ROOT_ATTRIBUTE_ID, true);
        }

        private void restrictTypes(Identifier.Variable id, FunctionalIterator<Label> labels) {
            TraversalVertex.Properties.Type props = traversal.structure().typeVertex(id).props();
            if (props.labels().isEmpty()) labels.forEachRemaining(props.labels()::add);
            else {
                Set<Label> intersection = labels.filter(props.labels()::contains).toSet();
                props.clearLabels();
                props.labels(intersection);
            }
            if (props.labels().isEmpty()) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_PATTERN_VARIABLE, conjunction, inferenceToOriginal.get(id));
            }
        }

        private void restrictValueTypes(Identifier.Variable id, FunctionalIterator<Encoding.ValueType> valueTypes) {
            TraversalVertex.Properties.Type props = traversal.structure().typeVertex(id).props();
            if (props.valueTypes().isEmpty()) valueTypes.forEachRemaining(props.valueTypes()::add);
            else {
                Set<Encoding.ValueType> intersection = valueTypes.filter(props.valueTypes()::contains).toSet();
                props.clearValueTypes();
                props.valueTypes(intersection);
            }
            if (props.valueTypes().isEmpty()) {
                conjunction.setCoherent(false);
                throw TypeDBException.of(UNSATISFIABLE_PATTERN_VARIABLE_VALUE, conjunction, inferenceToOriginal.get(id));
            }
        }

        private void restrictTypesByValueType(TypeVariable resolver, Set<Encoding.ValueType> valueTypes) {
            FunctionalIterator<TypeVertex> attrTypes = empty();
            for (Encoding.ValueType valueType : valueTypes) {
                switch (valueType) {
                    case STRING:
                        attrTypes = attrTypes.link(graphMgr.schema().attributeTypes(Encoding.ValueType.STRING));
                        break;
                    case LONG:
                        attrTypes = attrTypes.link(graphMgr.schema().attributeTypes(Encoding.ValueType.LONG));
                        break;
                    case DOUBLE:
                        attrTypes = attrTypes.link(graphMgr.schema().attributeTypes(Encoding.ValueType.DOUBLE));
                        break;
                    case BOOLEAN:
                        attrTypes = attrTypes.link(graphMgr.schema().attributeTypes(Encoding.ValueType.BOOLEAN));
                        break;
                    case DATETIME:
                        attrTypes = attrTypes.link(graphMgr.schema().attributeTypes(Encoding.ValueType.DATETIME));
                        break;
                    default:
                        throw TypeDBException.of(ILLEGAL_STATE);
                }
            }
            restrictTypes(resolver.id(), attrTypes.map(TypeVertex::properLabel));
        }

        private Identifier.Variable generateVariableID() {
            return Identifier.Variable.anon(nextGeneratedID++);
        }
    }
}
