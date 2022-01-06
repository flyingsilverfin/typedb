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

package com.vaticle.typedb.core.rocks;

import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.Iterators;
import com.vaticle.typedb.core.common.iterator.sorted.AbstractSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.graph.common.Storage.Key;

import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.RESOURCE_CLOSED;

public final class RocksIterator<T extends Key> extends AbstractSortedIterator<KeyValue<T, ByteArray>, Order.Asc>
        implements SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc>, AutoCloseable {

    private final Key.Prefix<T> prefix;
    private final RocksStorage storage;
    private State state;
    private KeyValue<T, ByteArray> next;
    private boolean isClosed;
    org.rocksdb.RocksIterator internalRocksIterator;

    private enum State {INIT, UNFETCHED, FORWARDED, FETCHED, COMPLETED;}

    RocksIterator(RocksStorage storage, Key.Prefix<T> prefix) {
        super(ASC);
        this.storage = storage;
        this.prefix = prefix;
        state = State.INIT;
        isClosed = false;
    }

    @Override
    public synchronized final KeyValue<T, ByteArray> peek() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    public synchronized final KeyValue<T, ByteArray> next() {
        if (!hasNext()) {
            if (isClosed) throw TypeDBException.of(RESOURCE_CLOSED);
            else throw new NoSuchElementException();
        }
        state = State.UNFETCHED;
        return next;
    }

    @Override
    public final boolean hasNext() {
        switch (state) {
            case COMPLETED:
                return false;
            case FETCHED:
                return true;
            case UNFETCHED:
                return fetchAndCheck();
            case FORWARDED:
                return hasValidNext();
            case INIT:
                return initialiseAndCheck();
            default: // This should never be reached
                return false;
        }
    }

    @Override
    public synchronized void forward(KeyValue<T, ByteArray> target) {
        if (state == State.COMPLETED) return;
        else if (state == State.INIT) initialise(target.key());
        else internalRocksIterator.seek(target.key().bytes().getBytes());
        state = State.FORWARDED;
    }

    private synchronized boolean initialiseAndCheck() {
        if (state != State.COMPLETED) {
            initialise(prefix);
            return hasValidNext();
        } else {
            return false;
        }
    }

    private synchronized void initialise(Key key) {
        assert state == State.INIT;
        this.internalRocksIterator = storage.getInternalRocksIterator(key.partition(), usePrefixBloom());
        this.internalRocksIterator.seek(key.bytes().getBytes());
        state = State.FORWARDED;
    }

    private synchronized boolean fetchAndCheck() {
        if (state != State.COMPLETED) {
            internalRocksIterator.next();
            return hasValidNext();
        } else {
            return false;
        }
    }

    private synchronized boolean hasValidNext() {
        ByteArray key;
        if (!internalRocksIterator.isValid() || !((key = ByteArray.of(internalRocksIterator.key())).hasPrefix(prefix.bytes()))) {
            recycle();
            return false;
        }
        next = KeyValue.of(prefix.builder().build(key), ByteArray.of(internalRocksIterator.value()));
        state = State.FETCHED;
        return true;
    }

    @Override
    public void recycle() {
        close();
    }

    @Override
    public synchronized void close() {
        if (state != State.COMPLETED) {
            if (state != State.INIT) storage.recycle(this);
            state = State.COMPLETED;
            isClosed = true;
            storage.remove(this);
        }
    }

    Key.Partition partition() {
        return prefix.partition();
    }

    boolean usePrefixBloom() {
        return prefix.isFixedStartInPartition();
    }

    @Override
    public final SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> merge(
            SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> iterator) {
        return Iterators.Sorted.merge(ASC, this, iterator);
    }

    @Override
    public <V extends Comparable<? super V>, ORDER extends Order> SortedIterator.Forwardable<V, ORDER> mapSorted(
            ORDER order, Function<KeyValue<T, ByteArray>, V> mappingFn, Function<V, KeyValue<T, ByteArray>> reverseMappingFn) {
        return Iterators.Sorted.mapSorted(order, this, mappingFn, reverseMappingFn);
    }

    @Override
    public SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> distinct() {
        return Iterators.Sorted.distinct(this);
    }

    @Override
    public SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> filter(Predicate<KeyValue<T, ByteArray>> predicate) {
        return Iterators.Sorted.filter(this, predicate);
    }

    @Override
    public SortedIterator.Forwardable<KeyValue<T, ByteArray>, Order.Asc> onFinalise(Runnable finalise) {
        return Iterators.Sorted.onFinalise(this, finalise);
    }
}
