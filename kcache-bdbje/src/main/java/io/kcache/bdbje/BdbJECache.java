/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kcache.bdbje;

import com.google.common.primitives.SignedBytes;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import io.kcache.KeyValue;
import io.kcache.KeyValueIterator;
import io.kcache.exceptions.CacheInitializationException;
import io.kcache.utils.KeyBytesComparator;
import io.kcache.utils.KeyComparator;
import io.kcache.utils.PersistentCache;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A persistent key-value store based on LMDB.
 */
public class BdbJECache<K, V> extends PersistentCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(BdbJECache.class);

    private static final Comparator<byte[]> BYTES_COMPARATOR = SignedBytes.lexicographicalComparator();

    private static final String DB_FILE_DIR = "lmdb";

    private final String name;
    private final String parentDir;
    private final String rootDir;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Set<KeyValueIterator<K, V>> openIterators = Collections.synchronizedSet(new HashSet<>());

    private File dbDir;
    private Environment env;
    private Database db;

    public BdbJECache(final String name,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, DB_FILE_DIR, rootDir, keySerde, valueSerde);
    }

    public BdbJECache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde) {
        this(name, parentDir, rootDir, keySerde, valueSerde, null);
    }

    public BdbJECache(final String name,
                      final String parentDir,
                      final String rootDir,
                      Serde<K> keySerde,
                      Serde<V> valueSerde,
                      Comparator<K> comparator) {
        super(comparator != null
            ? comparator
            : new KeyComparator<>(new SerdeWrapper<>(keySerde), BYTES_COMPARATOR));
        this.name = name;
        this.parentDir = parentDir;
        this.rootDir = rootDir;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    protected void openDB() {
        dbDir = new File(new File(rootDir, parentDir), name);

        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
        } catch (final IOException fatal) {
            throw new CacheInitializationException("Could not create directories", fatal);
        }

        openBdbJE();
    }

    private void openBdbJE() {
        try {
            // Environment and database opens
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            env = new Environment(dbDir, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setBtreeComparator(new KeyBytesComparator<>(new SerdeWrapper<K>(keySerde), comparator()));
            dbConfig.setKeyPrefixing(true);
            db = env.openDatabase(null, name, dbConfig);
        } catch (final Exception e) {
            throw new CacheInitializationException("Error opening store " + name + " at location " + dbDir.toString(), e);
        }
    }

    @Override
    public int size() {
        validateStoreOpen();
        return (int) db.count();
    }

    @Override
    public V put(final K key, final V value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final V originalValue = get(key);
        byte[] keyBytes = keySerde.serializer().serialize(null, key);
        DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
        byte[] valueBytes = valueSerde.serializer().serialize(null, value);
        DatabaseEntry dbValue = new DatabaseEntry(valueBytes);
        db.put(null, dbKey, dbValue);
        return originalValue;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) {
        validateStoreOpen();
        for (Map.Entry<? extends K, ? extends V> entry : entries.entrySet()) {
            byte[] keyBytes = keySerde.serializer().serialize(null, entry.getKey());
            DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
            byte[] valueBytes = valueSerde.serializer().serialize(null, entry.getValue());
            DatabaseEntry dbValue = new DatabaseEntry(valueBytes);
            db.put(null, dbKey, dbValue);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(final Object key) {
        validateStoreOpen();
        byte[] keyBytes = keySerde.serializer().serialize(null, (K) key);
        DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
        DatabaseEntry dbValue = new DatabaseEntry();
        if (db.get(null, dbKey, dbValue, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
            byte[] valueBytes = dbValue.getData();
            return valueSerde.deserializer().deserialize(null, valueBytes);
        } else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        Objects.requireNonNull(key, "key cannot be null");
        final V originalValue = get(key);
        byte[] keyBytes = keySerde.serializer().serialize(null, (K) key);
        DatabaseEntry dbKey = new DatabaseEntry(keyBytes);
        db.delete(null, dbKey);
        return originalValue;
    }

    @Override
    protected KeyValueIterator<K, V> range(K from, boolean fromInclusive, K to, boolean toInclusive, boolean isDescending) {
        validateStoreOpen();
        byte[] fromBytes = keySerde.serializer().serialize(null, from);
        DatabaseEntry dbKey = new DatabaseEntry(fromBytes);
        DatabaseEntry dbValue = new DatabaseEntry();
        Cursor cursor = db.openCursor(null, CursorConfig.READ_UNCOMMITTED); // avoid read locks

        Comparator<? super K> comparator = isDescending
            ? Collections.reverseOrder(comparator())
            : comparator();
        Predicate<K> fromTest = from != null ? k -> {
            int cmp = comparator.compare(from, k);
            return cmp < 0 || (cmp == 0 && fromInclusive);
        } : kv -> true;
        Predicate<K> toTest = to != null ? k -> {
            int cmp = comparator.compare(k, to);
            return cmp < 0 || (cmp == 0 && toInclusive);
        } : kv -> true;

        KeyValueIterator<K, V> iter = new KeyValueIterator<K, V>() {
            private OperationStatus status;
            private KeyValue<K, V> current;

            @Override
            public boolean hasNext() {
                if (current == null) {
                    current = getNextEntry();
                    while (current != null && !fromTest.test(current.key)) {
                        current = getNextEntry();
                    }
                }
                return current != null;
            }

            @Override
            public KeyValue<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                KeyValue<K, V> next = current;
                current = null;
                return next;
            }

            private KeyValue<K, V> getNextEntry() {
                try {
                    if (status != null && status != OperationStatus.SUCCESS) {
                        return null;
                    }
                    while (true) {
                        if (status == null && from != null) {
                            status = cursor.getSearchKeyRange(dbKey, dbValue, LockMode.DEFAULT);
                        } else {
                            if (isDescending) {
                                status = cursor.getPrev(dbKey, dbValue, LockMode.DEFAULT);
                            } else {
                                status = cursor.getNext(dbKey, dbValue, LockMode.DEFAULT);
                            }
                        }
                        if (status != OperationStatus.SUCCESS) {
                            break;
                        }
                        K key = keySerde.deserializer().deserialize(null, Arrays.copyOfRange(
                            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));

                        if (!toTest.test(key)) {
                            status = OperationStatus.NOTFOUND;
                            break;
                        }

                        V value = valueSerde.deserializer().deserialize(null, Arrays.copyOfRange(
                            dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));

                        return new KeyValue<>(key, value);
                    }
                    return null;
                } catch (SerializationException e) {
                    log.error("Failed to serialize", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                openIterators.remove(this);
                cursor.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        openIterators.add(iter);
        return iter;
    }

    @Override
    protected KeyValueIterator<K, V> all(boolean isDescending) {
        validateStoreOpen();
        DatabaseEntry dbKey = new DatabaseEntry();
        DatabaseEntry dbValue = new DatabaseEntry();
        Cursor cursor = db.openCursor(null, CursorConfig.READ_UNCOMMITTED); // avoid read locks

        KeyValueIterator<K, V> iter = new KeyValueIterator<K, V>() {
            private OperationStatus status;
            private KeyValue<K, V> current;

            @Override
            public boolean hasNext() {
                if (current == null) {
                    current = getNextEntry();
                }
                return current != null;
            }

            @Override
            public KeyValue<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                KeyValue<K, V> next = current;
                current = null;
                return next;
            }

            private KeyValue<K, V> getNextEntry() {
                try {
                    if (status != null && status != OperationStatus.SUCCESS) {
                        return null;
                    }
                    while (true) {
                        status = isDescending
                            ? cursor.getPrev(dbKey, dbValue, LockMode.DEFAULT)
                            : cursor.getNext(dbKey, dbValue, LockMode.DEFAULT);
                        if (status != OperationStatus.SUCCESS) {
                            break;
                        }
                        K key = keySerde.deserializer().deserialize(null, Arrays.copyOfRange(
                            dbKey.getData(), dbKey.getOffset(), dbKey.getOffset() + dbKey.getSize()));

                        V value = valueSerde.deserializer().deserialize(null, Arrays.copyOfRange(
                            dbValue.getData(), dbValue.getOffset(), dbValue.getOffset() + dbValue.getSize()));

                        return new KeyValue<>(key, value);
                    }
                    return null;
                } catch (SerializationException e) {
                    log.error("Failed to serialize", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                openIterators.remove(this);
                cursor.close();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        openIterators.add(iter);
        return iter;
    }

    @Override
    public void flush() {
        if (db == null) {
            return;
        }
        env.sync();
    }

    @Override
    protected void closeDB() {
        try {
            closeOpenIterators();
            if (db != null) {
                db.close();
            }
            if (env != null) {
                env.close();
            }
            db = null;
            env = null;
        } catch (Exception e) {
            log.warn("Error during close", e);
        }
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator<K, V>> iterators = new HashSet<>(openIterators);
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            for (final KeyValueIterator<K, V> iterator : iterators) {
                iterator.close();
            }
        }
    }

    @Override
    public synchronized void destroy() throws IOException {
        Utils.delete(new File(rootDir + File.separator + parentDir + File.separator + name));
    }
}
