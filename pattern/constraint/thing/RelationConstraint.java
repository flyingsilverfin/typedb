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

package com.vaticle.typedb.core.pattern.constraint.thing;

import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.pattern.Conjunction;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalence;
import com.vaticle.typedb.core.pattern.equivalence.AlphaEquivalent;
import com.vaticle.typedb.core.pattern.variable.ThingVariable;
import com.vaticle.typedb.core.pattern.variable.TypeVariable;
import com.vaticle.typedb.core.pattern.variable.Variable;
import com.vaticle.typedb.core.pattern.variable.VariableCloner;
import com.vaticle.typedb.core.pattern.variable.VariableRegistry;
import com.vaticle.typedb.core.traversal.GraphTraversal;
import com.vaticle.typedb.core.traversal.common.Identifier;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COLON;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.COMMA;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.PARAN_CLOSE;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.PARAN_OPEN;
import static com.vaticle.typeql.lang.common.TypeQLToken.Char.SPACE;

public class RelationConstraint extends ThingConstraint implements AlphaEquivalent<RelationConstraint> {

    private final LinkedHashSet<RolePlayer> rolePlayers;
    private final int hash;

    public RelationConstraint(ThingVariable owner, LinkedHashSet<RolePlayer> rolePlayers) {
        super(owner, rolePlayerVariables(rolePlayers));
        assert rolePlayers != null && !rolePlayers.isEmpty();
        this.rolePlayers = new LinkedHashSet<>(rolePlayers);
        this.hash = Objects.hash(RelationConstraint.class, this.owner, this.rolePlayers);
        for (RelationConstraint.RolePlayer rp : rolePlayers) {
            rp.player().constraining(this);
            rp.roleType().constraining(this);
        }
    }

    static RelationConstraint of(ThingVariable owner, com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Relation constraint,
                                 VariableRegistry register) {
        return new RelationConstraint(
                owner, iterate(constraint.players()).map(rp -> RolePlayer.of(rp, register)).toLinkedSet()
        );
    }

    static RelationConstraint of(ThingVariable owner, RelationConstraint clone, VariableCloner cloner) {
        return new RelationConstraint(
                owner, iterate(clone.players()).map(rp -> RolePlayer.of(rp, cloner)).toLinkedSet()
        );
    }

    @Override
    public RelationConstraint clone(Conjunction.Cloner cloner) {
        return cloner.cloneVariable(owner()).relation(Iterators.iterate(rolePlayers).map(
                rolePlayer -> rolePlayer.clone(cloner)).toLinkedSet());
    }

    public LinkedHashSet<RolePlayer> players() {
        return rolePlayers;
    }

    @Override
    public void addTo(GraphTraversal.Thing traversal) {
        for (RolePlayer rolePlayer : rolePlayers) {
            ThingVariable player = rolePlayer.player();
            int rep = rolePlayer.repetition();
            TypeVariable roleType = rolePlayer.roleType();
            if (roleType.reference().isName()) {
                Identifier.Scoped role = Identifier.Scoped.of(owner.id(), roleType.id(), player.id(), rep);
                traversal.relating(owner.id(), role);
                traversal.playing(player.id(), role);
                traversal.isa(role, roleType.id());
            } else {
                assert roleType.reference().isLabel() && !roleType.inferredTypes().isEmpty();
                traversal.rolePlayer(owner.id(), player.id(), roleType.inferredTypes(), rep);
            }
        }
    }

    @Override
    public boolean isRelation() {
        return true;
    }

    @Override
    public RelationConstraint asRelation() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationConstraint that = (RelationConstraint) o;
        return (this.owner.equals(that.owner) && this.rolePlayers.equals(that.rolePlayers));
    }

    @Override
    public int hashCode() {
        return hash;
    }

    private static Set<Variable> rolePlayerVariables(Set<RolePlayer> rolePlayers) {
        Set<com.vaticle.typedb.core.pattern.variable.Variable> variables = new HashSet<>();
        rolePlayers.forEach(player -> {
            variables.add(player.player());
            variables.add(player.roleType());
        });
        return variables;
    }

    @Override
    public AlphaEquivalence alphaEquals(RelationConstraint that) {
        return AlphaEquivalence.valid()
                .validIf(players().size() == that.players().size())
                .addOrInvalidate(() -> Iterators.permutation(players()).stream().map(playersPermutation -> {
                    Iterator<RolePlayer> thisRolePlayersIt = playersPermutation.iterator();
                    Iterator<RolePlayer> thatRolePlayersIt = that.players().iterator();
                    AlphaEquivalence permutationMap = AlphaEquivalence.valid();
                    while (thisRolePlayersIt.hasNext() && thatRolePlayersIt.hasNext()) {
                        permutationMap = permutationMap.validIfAlphaEqual(thisRolePlayersIt.next(), thatRolePlayersIt.next());
                        if (!permutationMap.isValid()) return permutationMap;
                    }
                    return permutationMap;
                }).filter(AlphaEquivalence::isValid).findFirst().orElse(AlphaEquivalence.invalid()));
    }

    public static class RolePlayer implements AlphaEquivalent<RolePlayer> {

        private final TypeVariable roleType;
        private final ThingVariable player;
        private final int repetition;
        private final int hash;

        public RolePlayer(TypeVariable roleType, ThingVariable player, int repetition) {
            assert roleType.id().isName() || roleType.id().isAnonymous() ||
                    (roleType.label().isPresent() && roleType.label().get().scope().isPresent());
            if (player == null) throw new NullPointerException("Null player");
            this.roleType = roleType;
            this.player = player;
            this.repetition = repetition;
            this.hash = Objects.hash(this.roleType, this.player, this.repetition);
        }

        public static RolePlayer of(com.vaticle.typeql.lang.pattern.constraint.ThingConstraint.Relation.RolePlayer constraint,
                                    VariableRegistry registry) {
            return new RolePlayer(
                    registry.register(constraint.roleType()),
                    registry.register(constraint.player()),
                    constraint.repetition()
            );
        }

        public static RolePlayer of(RolePlayer clone, VariableCloner cloner) {
            return new RolePlayer(
                    cloner.clone(clone.roleType()),
                    cloner.clone(clone.player()),
                    clone.repetition()
            );
        }

        public int repetition() {
            return repetition;
        }

        public TypeVariable roleType() {
            return roleType;
        }

        public ThingVariable player() {
            return player;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RolePlayer that = (RolePlayer) o;
            return this.roleType.equals(that.roleType) &&
                    this.player.equals(that.player) &&
                    this.repetition == that.repetition;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return roleType.toString() + COLON + player.toString();
        }

        @Override
        public AlphaEquivalence alphaEquals(RolePlayer that) {
            return AlphaEquivalence.valid()
                    .validIfAlphaEqual(roleType, that.roleType)
                    .validIfAlphaEqual(player, that.player);
        }

        public RolePlayer clone(Conjunction.Cloner cloner) {
            TypeVariable roleTypeClone = cloner.cloneVariable(roleType);
            ThingVariable playerClone = cloner.cloneVariable(player);
            return new RelationConstraint.RolePlayer(roleTypeClone, playerClone, repetition);
        }
    }

    @Override
    public String toString() {
        return owner.toString() + SPACE + PARAN_OPEN
                + rolePlayers.stream().map(RolePlayer::toString).collect(Collectors.joining("" + COMMA + SPACE))
                + PARAN_CLOSE;
    }
}
