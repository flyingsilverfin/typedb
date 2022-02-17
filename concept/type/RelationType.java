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

package com.vaticle.typedb.core.concept.type;

import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.concept.thing.Relation;

import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order.Asc;

public interface RelationType extends ThingType {

    @Override
    Seekable<? extends RelationType, Asc> getSubtypes();

    @Override
    Seekable<? extends RelationType, Asc> getSubtypesExplicit();

    @Override
    Seekable<? extends Relation, Asc> getInstances();

    @Override
    Seekable<? extends Relation, Asc> getInstancesExplicit();

    void setSupertype(RelationType superType);

    void setRelates(String roleLabel);

    void setRelates(String roleLabel, String overriddenLabel);

    void unsetRelates(String roleLabel);

    FunctionalIterator<? extends RoleType> getRelates();

    FunctionalIterator<? extends RoleType> getRelatesExplicit();

    RoleType getRelates(String roleLabel);

    RoleType getRelatesExplicit(String roleLabel);

    RoleType getRelatesOverridden(String roleLabel);

    Relation create();

    Relation create(boolean isInferred);
}
