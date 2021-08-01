package com.adtsw.jcommons.ds;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

public class SortedHashMap<K, V> {

    private final SortedArrayList<K> index;
    private final HashMap<K, V> data;
    private final int maxSize;
    @JsonIgnore
    private final ReentrantReadWriteLock.WriteLock writeLock;
    @JsonIgnore
    private final ReentrantReadWriteLock.ReadLock readLock;

    public SortedHashMap() {
        this(-1);
    }

    public SortedHashMap(int maxSize) {
        this.index = new SortedArrayList<>();
        this.data = new HashMap<>();
        this.maxSize = maxSize;
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
    }

    public void put(K key, V value) {
        writeLock.lock();
        boolean newKey = data.get(key) == null;
        data.put(key, value);
        if(newKey) {
            index.insertSorted(key);
            checkMaxSize();
        }
        writeLock.unlock();
    }

    private void checkMaxSize() {
        if(maxSize != -1 && index.size() > maxSize) {
            removeFirst();
        }
    }

    public V getByIndex(int i) {
        if(i >= index.size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        readLock.lock();
        K requiredKey = index.get(i);
        V requiredValue = requiredKey == null ? null : data.get(requiredKey);
        readLock.unlock();
        return requiredValue;
    }
    
    public V get(K key) {
        readLock.lock();
        V value = data.get(key);
        readLock.unlock();
        return value;
    }
    
    public List<V> values() {
        readLock.lock();
        List<V> values = new ArrayList<>();
        for (K key : index) {
            values.add(data.get(key));
        }
        readLock.unlock();
        return values;
    }
    
    public int size() {
        return index.size();
    }

    public void forEach(BiConsumer<? super K, ? super V> action, boolean reverseOrder) {
        if(!reverseOrder) {
            forEach(action);    
        } else {
            readLock.lock();
            int indexSize = index.size();
            for (int i = indexSize - 1; i >= 0; i--) {
                executeActionOnIndex(action, i);
            }
            readLock.unlock();
        }
    }

    public void forEach(BiConsumer<? super K, ? super V> action) {
        readLock.lock();
        int indexSize = index.size();
        for (int i = 0; i < indexSize; i++) {
            executeActionOnIndex(action, i);
        }
        readLock.unlock();
    }

    private void executeActionOnIndex(BiConsumer<? super K, ? super V> action, int i) {
        K key = index.get(i);
        V value = data.get(key);
        if (value != null) {
            action.accept(key, value);
        } else {
            System.out.println("Calling consumer on null key");
        }
    }

    public void compute(MapIteratorComputer<K, V> iteratorComputer, boolean reverseOrder) {
        SortedHashMapIterator iterator = new SortedHashMapIterator(reverseOrder);
        while (iterator.hasNext()) {
            Pair<K, V> next = iterator.next();
            iteratorComputer.compute(next.getValue0(), next.getValue1());
        }
    }

    public void compute(MapIteratorComputer<K, V> iteratorComputer) {
        compute(iteratorComputer, false);
    }

    public Pair<K, V> firstEntry() {
        K firstKey = index.size() > 0 ? index.get(0) : null;
        return firstKey == null ? null : new Pair<>(firstKey, data.get(firstKey));
    }

    public Pair<K, V> lastEntry() {
        K lastKey = index.size() > 0 ? index.get(index.size() - 1) : null;
        return lastKey == null ? null : new Pair<>(lastKey, data.get(lastKey));
    }

    public V lastValue() {
        K lastKey = index.size() > 0 ? index.get(index.size() - 1) : null;
        return lastKey == null ? null : data.get(lastKey);
    }

    public List<V> getLastKValues(int k) {
        readLock.lock();
        if(k > index.size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        List<K> kIndexes = index.subList(index.size() - k, index.size());
        List<V> lastKValues = new ArrayList<>();
        for (K key : kIndexes) {
            lastKValues.add(data.get(key));
        }
        readLock.unlock();
        return lastKValues;
    }

    public SortedHashMap<K, V> subSet(int beginIndex, int endIndex) {
        readLock.lock();
        if(endIndex > index.size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        SortedHashMap<K, V> subMap = new SortedHashMap<>();
        List<K> kIndexes = index.subList(beginIndex, endIndex);
        for (K key : kIndexes) {
            subMap.put(key, data.get(key));
        }
        readLock.unlock();
        return subMap;
    }
    
    public List<V> subSetValues(int beginIndex, int endIndex) {
        readLock.lock();
        if(endIndex > index.size()) {
            throw new ArrayIndexOutOfBoundsException();
        }
        List<V> subSetValues = new ArrayList<>();
        List<K> kIndexes = index.subList(beginIndex, endIndex);
        for (K key : kIndexes) {
            subSetValues.add(data.get(key));
        }
        readLock.unlock();
        return subSetValues;
    }

    private void removeFirst() {
        K firstKey = index.size() > 0 ? index.get(0) : null;
        if(firstKey != null) {
            data.remove(firstKey);
            index.remove(0);
        }
    }

    private void removeLast() {
        K lastKey = index.size() > 0 ? index.get(index.size() - 1) : null;
        if(lastKey != null) {
            data.remove(lastKey);
            index.remove(index.size() - 1);
        }
    }
    
    public void clear() {
        writeLock.lock();
        data.clear();
        index.clear();
        writeLock.unlock();
    }
    
    class SortedHashMapIterator implements Iterator<Pair<K, V>> {
    
        private int currentIndex;
        private final boolean reverseOrder;

        public SortedHashMapIterator(boolean reverseOrder) {
            this.reverseOrder = reverseOrder;
            this.currentIndex = reverseOrder ? index.size() - 1 : 0;
        }

        @Override
        public boolean hasNext() {
            return reverseOrder ? currentIndex != 0 : currentIndex != index.size() - 1;
        }

        @Override
        public Pair<K, V> next() {
            currentIndex = reverseOrder ? currentIndex - 1 : currentIndex + 1;
            if(currentIndex >= index.size() || currentIndex < 0) {
                throw new ArrayIndexOutOfBoundsException();
            }
            K key = index.get(currentIndex);
            return new Pair<>(key, data.get(key));
        }
    }
}
