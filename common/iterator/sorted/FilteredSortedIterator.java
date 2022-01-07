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
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;

public class FilteredSortedIterator<T extends Comparable<? super T>, ORDER extends Order, ITER extends SortedIterator<T, ORDER>>
        extends AbstractSortedIterator<T, ORDER> {

    private final Predicate<T> predicate;
    final ITER iterator;
    T next;
    T last;

    public FilteredSortedIterator(ITER iterator, Predicate<T> predicate) {
        super(iterator.order());
        this.iterator = iterator;
        this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
        return (next != null) || fetchAndCheck();
    }

    private boolean fetchAndCheck() {
        while (iterator.hasNext() && !predicate.test(next = iterator.next())) next = null;
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        last = next;
        next = null;
        return last;
    }

    @Override
    public T peek() {
        if (!hasNext()) throw new NoSuchElementException();
        return next;
    }

    @Override
    public void recycle() {
        iterator.recycle();
    }

    public static class Forwardable<T extends Comparable<? super T>, ORDER extends Order>
            extends FilteredSortedIterator<T, ORDER, SortedIterator.Forwardable<T, ORDER>>
            implements SortedIterator.Forwardable<T, ORDER> {

        public Forwardable(SortedIterator.Forwardable<T, ORDER> source, Predicate<T> predicate) {
            super(source, predicate);
        }

        @Override
        public void forward(T target) {
            if (last != null && target.compareTo(last) < 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
            iterator.forward(target);
            next = null;
        }

        @Override
        public final SortedIterator.Forwardable<T, ORDER> merge(SortedIterator.Forwardable<T, ORDER> iterator) {
            return Iterators.Sorted.merge(this, iterator);
        }

        @Override
        public <U extends Comparable<? super U>, ORD extends Order> SortedIterator.Forwardable<U, ORD> mapSorted(
                ORD order, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            return Iterators.Sorted.mapSorted(order, this, mappingFn,  reverseMappingFn);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> distinct() {
            return Iterators.Sorted.distinct(this);
        }

        @Override
        public SortedIterator.Forwardable<T, ORDER> filter(Predicate<T> predicate) {
            return Iterators.Sorted.filter(this, predicate);
        }
    }
}
