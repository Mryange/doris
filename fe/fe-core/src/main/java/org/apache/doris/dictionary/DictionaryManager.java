// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.dictionary;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.persist.CreateDictionaryPersistInfo;
import org.apache.doris.persist.DropDictionaryPersistInfo;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for dictionary operations, including creation, deletion, and data loading.
 */
public class DictionaryManager extends MasterDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(DictionaryManager.class);

    // Lock for protecting dictionaries map
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    // Map of database name -> dictionary name -> dictionary
    @SerializedName(value = "d")
    private Map<String, Map<String, Dictionary>> dictionaries = Maps.newConcurrentMap();

    @SerializedName(value = "i")
    private long uniqueId = 0;

    public DictionaryManager() {
        super("Dictionary Manager", 10 * 60 * 1000); // run every 10 minutes
    }

    @Override
    protected void runAfterCatalogReady() {
        // Check and update dictionary data in each cycle
        try {
            checkAndUpdateDictionaries();
        } catch (Exception e) {
            LOG.warn("Failed to check and update dictionaries", e);
        }
    }

    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    /**
     * Create a new dictionary based on the provided info.
     *
     * @throws DdlException if the dictionary already exists and ifNotExists is false
     */
    public Dictionary createDictionary(CreateDictionaryInfo info) throws DdlException {
        // 1. Check if dictionary already exists
        if (hasDictionary(info.getDbName(), info.getDictName())) {
            if (info.isIfNotExists()) {
                return getDictionary(info.getDbName(), info.getDictName());
            } else {
                throw new DdlException(
                        "Dictionary " + info.getDictName() + " already exists in database " + info.getDbName());
            }
        }

        Dictionary dictionary;
        lockWrite();
        try {
            // Create dictionary object
            dictionary = new Dictionary(info, ++uniqueId);
            // Add to dictionaries map. no throw here. so schedule below is safe.
            Map<String, Dictionary> dbDictionaries = dictionaries.computeIfAbsent(info.getDbName(),
                    k -> Maps.newConcurrentMap());
            dbDictionaries.put(info.getDictName(), dictionary);

            // Log the creation operation
            Env.getCurrentEnv().getEditLog().logCreateDictionary(dictionary);
        } finally {
            unlockWrite();
        }

        scheduleDataLoad(dictionary);

        return dictionary;
    }

    /**
     * Delete a dictionary.
     *
     * @throws DdlException if the dictionary does not exist
     */
    public void dropDictionary(String dbName, String dictName, boolean ifExists) throws DdlException {
        lockWrite();
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
            if (dbDictionaries == null || !dbDictionaries.containsKey(dictName)) {
                if (!ifExists) {
                    throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
                }
                return;
            }
            dbDictionaries.remove(dictName);
            if (dbDictionaries.isEmpty()) {
                dictionaries.remove(dbName);
            }

            // Log the drop operation
            Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, dictName);
        } finally {
            unlockWrite();
        }
    }

    /**
     * Drop all dictionaries in a database. Used when dropping a database.
     */
    public void dropDbDictionaries(String dbName) {
        lockWrite();
        try {
            // pop and save item from dictionaries
            Map<String, Dictionary> dbDictionaries = dictionaries.remove(dbName);
            // Log the drop operation
            if (dbDictionaries != null) {
                for (Map.Entry<String, Dictionary> entry : dbDictionaries.entrySet()) {
                    Env.getCurrentEnv().getEditLog().logDropDictionary(dbName, entry.getKey());
                }
            }
        } finally {
            unlockWrite();
        }
    }

    /**
     * Check if a dictionary exists.
     */
    public boolean hasDictionary(String dbName, String dictName) {
        lockRead();
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
            return dbDictionaries != null && dbDictionaries.containsKey(dictName);
        } finally {
            unlockRead();
        }
    }

    public Map<String, Dictionary> getDictionaries(String dbName) {
        lockRead();
        try {
            return dictionaries.computeIfAbsent(dbName, k -> Maps.newConcurrentMap());
        } finally {
            unlockRead();
        }
    }

    /**
     * Get a dictionary.
     *
     * @throws DdlException if the dictionary does not exist
     */
    public Dictionary getDictionary(String dbName, String dictName) throws DdlException {
        lockRead();
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(dbName);
            if (dbDictionaries == null || !dbDictionaries.containsKey(dictName)) {
                throw new DdlException("Dictionary " + dictName + " does not exist in database " + dbName);
            }
            return dbDictionaries.get(dictName);
        } finally {
            unlockRead();
        }
    }

    private void checkAndUpdateDictionaries() {
        // TODO: Implement dictionary data check and update logic
        // This should:
        // 1. Check source tables for changes
        // 2. scheduleDataLoad if necessary
        // 3. Handle any errors or inconsistencies
    }

    public void scheduleDataLoad(Dictionary dictionary) {
        // TODO: Implement data load scheduling logic
        // This should:
        // 1. Create a load task
        // 2. Submit the task to a task executor
        // 3. Monitor the task progress
    }

    public void replayCreateDictionary(CreateDictionaryPersistInfo info) {
        Dictionary dictionary = info.getDictionary();
        lockWrite();
        try {
            // Add to dictionaries map
            Map<String, Dictionary> dbDictionaries = dictionaries.computeIfAbsent(dictionary.getDbName(),
                    k -> Maps.newConcurrentMap());
            if (dbDictionaries.containsKey(dictionary.getName())) {
                LOG.warn("Dictionary {} already exists when replaying create dictionary", dictionary.getName());
                return;
            }
            dbDictionaries.put(dictionary.getName(), dictionary);
            uniqueId = Math.max(uniqueId, dictionary.getId());
        } finally {
            unlockWrite();
        }
    }

    public void replayDropDictionary(DropDictionaryPersistInfo info) {
        lockWrite();
        try {
            Map<String, Dictionary> dbDictionaries = dictionaries.get(info.getDbName());
            if (dbDictionaries != null) {
                dbDictionaries.remove(info.getDictionaryName());
                if (dbDictionaries.isEmpty()) {
                    dictionaries.remove(info.getDbName());
                }
            } else {
                LOG.warn("Database {} does not exist when replaying drop dictionary", info.getDbName());
            }
        } finally {
            unlockWrite();
        }
    }

    // Metadata serialization
    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DictionaryManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DictionaryManager.class);
    }
}
