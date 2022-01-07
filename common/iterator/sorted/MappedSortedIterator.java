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

package com.vaticle.typedb.core.common.iterator.sorted;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

public class MappedSortedIterator<
        T extends Comparable<? super T>, U extends Comparable<? super U>,
        ORDER extends Order, ITER extends FunctionalIterator<T>
        > extends AbstractSortedIterator<U, ORDER> {

    final ITER source;
    final Function<T, U> mappingFn;
    State state;
    U next;
    U last;

    enum State {EMPTY, FETCHED, COMPLETED}

    public MappedSortedIterator(ORDER order, ITER source, Function<T, U> mappingFn) {
        super(order);
        this.source = source;
        this.mappingFn = mappingFn;
        this.state = State.EMPTY;
        last = null;
    }

    @Override
    public U peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case EMPTY:
                return fetchAndCheck();
            default:
                throw TypeDBException.of(ILLEGAL_STATE);
        }
    }

    private boolean fetchAndCheck() {
        if (source.hasNext()) {
            this.next = mappedNext();
            state = State.FETCHED;
        } else {
            state = State.COMPLETED;
        }
        return state == State.FETCHED;
    }

    U mappedNext() {
        T value = source.next();
        return mappingFn.apply(value);
    }

    @Override
    public U next() {
        if (!hasNext()) throw new NoSuchElementException();
        assert last == null || order.isValidNext(last, next) : "Sorted mapped iterator produces out of order values";
        last = next;
        state = State.EMPTY;
        return next;
    }

    @Override
    public void recycle() {
        source.recycle();
    }

    public static class Forwardable<T extends Comparable<? super T>, U extends Comparable<? super U>, ORDER extends Order>
            extends MappedSortedIterator<T, U, ORDER, SortedIterator.Forwardable<T, ?>>
            implements SortedIterator.Forwardable<U, ORDER> {

        private final Function<U, T> reverseMappingFn;

        /**
         * @param source           - iterator to create mapped iterators from
         * @param mappingFn        - The forward mapping function must return a new iterator that is sorted with respect to U's comparator.
         * @param reverseMappingFn - The reverse mapping function must be the able to invert the forward mapping function
         */
        public Forwardable(ORDER order, SortedIterator.Forwardable<T, ?> source,
                           Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            super(order, source, mappingFn);
            this.reverseMappingFn = reverseMappingFn;
        }

        @Override
        U mappedNext() {
            T value = source.next();
            U next = mappingFn.apply(value);
//                assert reverseMappingFn.apply(next).equals(value);
            return next;
        }

        @Override
        public void forward(U target) {
            if (last != null && !order.isValidNext(last, target)) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            T reverseMapped = reverseMappingFn.apply(target);
            source.forward(reverseMapped);
            state = State.EMPTY;
        }

        @Override
        public final SortedIterator.Forwardable<U, ORDER> merge(SortedIterator.Forwardable<U, ORDER> iterator) {
            return Iterators.Sorted.merge(this, iterator);
        }

        @Override
        public <V extends Comparable<? super V>, ORD extends Order> SortedIterator.Forwardable<V, ORD> mapSorted(
                ORD order, Function<U, V> mappingFn, Function<V, U> reverseMappingFn) {
            return Iterators.Sorted.mapSorted(order, this, mappingFn, reverseMappingFn);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> distinct() {
            return Iterators.Sorted.distinct(this);
        }

        @Override
        public SortedIterator.Forwardable<U, ORDER> filter(Predicate<U> predicate) {
            return Iterators.Sorted.filter(this, predicate);
        }
    }
}
