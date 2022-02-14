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

import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class LockableList<E> implements List<E> {

    protected class LockableListIterator implements Iterator<E> {
        protected Iterator<E> iterator;

        public LockableListIterator(Iterator<E> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public E next() {
            return iterator.next();
        }

        public void remove() {
            checkLock();
            iterator.remove();
        }
    }

    protected class LockableListListIterator implements ListIterator<E> {
        protected ListIterator<E> listIterator;

        public LockableListListIterator(ListIterator<E> listIterator) {
            this.listIterator = listIterator;
        }

        public boolean hasNext() {
            return listIterator.hasNext();
        }

        public E next() {
            return listIterator.next();
        }

        public boolean hasPrevious() {
            return listIterator.hasPrevious();
        }

        public E previous() {
            return listIterator.previous();
        }

        public int nextIndex() {
            return listIterator.nextIndex();
        }

        public int previousIndex() {
            return listIterator.previousIndex();
        }

        public void remove() {
            checkLock();
            listIterator.remove();
        }

        public void set(E e) {
            checkLock();
            listIterator.set(e);
        }

        public void add(E e) {
            checkLock();
            listIterator.add(e);
        }

    }

    private StackTraceElement[] lockingStackTrace;
    protected List<E> list;
    protected boolean locked;

    public LockableList(List<E> list) {
        this.list = list;
        locked = false;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked, StackTraceElement[] stackTrace) {
        this.locked = locked;
        this.lockingStackTrace = stackTrace != null ? stackTrace : lockingStackTrace;
    }

    protected void checkLock() {
        if (locked) {
            System.out.println("Locked due to: " + Arrays.toString(lockingStackTrace));
            throw new ConcurrentModificationException("Locked");
        }
    }

    public int size() {
        return list.size();
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public boolean contains(Object o) {
        return list.contains(o);
    }

    public Iterator<E> iterator() {
        return new LockableListIterator(list.iterator());
    }

    public Object[] toArray() {
        return list.toArray();
    }

    public <T> T[] toArray(T[] a) {
        return list.toArray(a);
    }

    public boolean add(E e) {
        checkLock();
        return list.add(e);
    }

    public boolean remove(Object o) {
        checkLock();
        return list.remove(o);
    }

    public boolean containsAll(Collection<?> c) {
        return list.containsAll(c);
    }

    public boolean addAll(Collection<? extends E> c) {
        checkLock();
        return list.addAll(c);
    }

    public boolean addAll(int index, Collection<? extends E> c) {
        checkLock();
        return list.addAll(index, c);
    }

    public boolean removeAll(Collection<?> c) {
        checkLock();
        return list.removeAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        checkLock();
        return list.retainAll(c);
    }

    public void clear() {
        checkLock();
        list.clear();
    }

    @Override
    public boolean equals(Object o) {
        return list.equals(o);
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    public E get(int index) {
        return list.get(index);
    }

    public E set(int index, E element) {
        checkLock();
        return list.set(index, element);
    }

    public void add(int index, E element) {
        checkLock();
        list.add(index, element);
    }

    public E remove(int index) {
        checkLock();
        return list.remove(index);
    }

    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    public ListIterator<E> listIterator() {
        return new LockableListListIterator(list.listIterator());
    }

    public ListIterator<E> listIterator(int index) {
        return new LockableListListIterator(list.listIterator(index));
    }

    public List<E> subList(int fromIndex, int toIndex) {
        throw new RuntimeException();
    }
}
