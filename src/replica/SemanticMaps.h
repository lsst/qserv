/*
 * LSST Data Management System
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#ifndef LSST_QSERV_REPLICA_SEMANTICMAPS_H
#define LSST_QSERV_REPLICA_SEMANTICMAPS_H

/**
 * This header declares tools for constructing the header-only views for nested
 * Standard Library's maps. Also a few ready to use algorithms are provided
 * for some most commonly used map.
 */

// System headers
#include <iosfwd>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * The namespace providing core implementations for a family
 * of the nested map-based structures.
 */
namespace detail {

/**
  * Class template SemanticMap is a base class for type-specific collections.
  * This class template has two parameters:
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename K, typename V>
class SemanticMap {

public:

    /// Internal collection (is public because this is the only state here)
    std::map<K, V> coll;

    virtual ~SemanticMap() = default;

    /// @return number of elements in the collection
    size_t size() const { return coll.size(); }

    /// @return 'true' if the collection is empty
    bool empty() const { return coll.empty(); }

    /// Clear the collection
    void clear() { coll.clear(); }

    /// @return iterator to the beginning of the container
    decltype(coll.begin()) begin() {
        return coll.begin();
    }

    /// @return constant iterator to the beginning of the container
    decltype(coll.cbegin()) begin() const {
        return coll.cbegin();
    }

    /// @return iterator to the end of the container
    decltype(coll.end()) end() {
        return coll.end();
    }

    /// @return constant iterator to the end of the container
    decltype(coll.cend()) end() const {
        return coll.cend();
    }

    /**
     * Merge the content of another collection of the same type
     *
     * @param coll
     *   collection whose content is to be merged
     * 
     * @param ignoreDuplicateKeys
     *   ignore duplicate keys if 'true'
     *
     * @throws std::invalid_argument
     *   on attempts to merge a collection with itself
     * 
     * @throws std::range_error
     *   on duplicate keys if ignoreDuplicateKeys is 'false'
     */
    void merge(SemanticMap<K,V> const& coll, bool ignoreDuplicateKeys = false) {

        if (this == &coll) {
            throw std::invalid_argument("attempted to merge the collection with itself");
        }
        for (auto&& entry: coll.coll) {
            K const& k = entry.first;
            V const& v = entry.second;
            if ((not ignoreDuplicateKeys) and exists(k)) {
                throw std::range_error("key already exists: " + std::to_string(k));
            }
            insert(k, v);
        }
    }

protected:

    // The constructors and the copy operator assume copy by value

    SemanticMap() = default;
    SemanticMap(SemanticMap const&) = default;
    SemanticMap& operator=(SemanticMap const&) = default;

    /// @return 'true' if such exists in the collection
    bool exists(K const& k) const { return coll.count(k); }

    /**
     * Insert a copy of an existing object
     *
     * @param k
     *   its key
     *
     * @param 
     *    object to be insert
     *
     * @return
     *   reference to the object within the collection
     */
    V& insert(K const& k, V const& v) {
        coll[k] = v;
        return coll[k];
    }

    /**
     * Insert a copy of an existing object
     *
     * @param k
     *   its key
     *
     * @param v
     *   object to be insert
     * 
     * @return
     *   reference to the object within the collection
     */
    V& insertIfNotExists(K const& k, V const& v) {
        if (not exists(k)) coll[k] = v;
        return coll[k];
    }

    /**
     * @param k
     *   object's key
     *
     * @return
     *   read-only reference to an object for a key
     */
    V const& get(K const& k) const { return coll.at(k); }

    /// @return collection object keys
    std::vector<K> keys() const {
        std::vector<K> result;
        result.reserve(coll.size());
        for (auto&& entry: coll) {
            result.push_back(entry.first);
        }
        return result;
    }
};

/**
  * Class template WorkerMap template has two parameters:
  *
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename V>
class WorkerMap : public SemanticMap<std::string,V> {

public:

    /// The key type
    using KeyType = std::string;

    /// The base map type
    using MapType = SemanticMap<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    WorkerMap() = default;
    WorkerMap(WorkerMap const&) = default;
    WorkerMap& operator=(WorkerMap const&) = default;

    ~WorkerMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into the collection.
     *
     * @param k
     *   key for an object
     *
     * @return
     *   writeable references to an object (existing or newly created one)
     *   at the key
     */
    V& atWorker(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing worker object
     *
     * @param k
     *   key for the object
     *
     * @param v
     *   reference to an object to be copied into the collection
     *
     * @return
     *   writeable references to the object within the collection
     */
    V& insertWorker(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k
     *   object's key
     *
     * @return
     *   'true' if  with the specified key already exists
     */
    bool workerExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k
     *   object's key
     *
     * @return
     *   read-only reference to a worker object for a key
     *
     * @throws
     *   std::out_of_range if no such key is found in the collection
     */
    V const& worker(KeyType const& k) const { return MapType::get(k); }

    /// @return collection of worker names
    std::vector<KeyType> workerNames() const { return MapType::keys(); }
};

/**
  * Class template DatabaseMap has two parameters:
  *
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename V>
class DatabaseMap : public SemanticMap<std::string,V> {

public:

    /// The key type
    using KeyType = std::string;

    /// The base map type
    using MapType = SemanticMap<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    DatabaseMap() = default;
    DatabaseMap(DatabaseMap const&) = default;
    DatabaseMap& operator=(DatabaseMap const&) = default;

    ~DatabaseMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into to the collection.
     *
     * @param k
     *   key for an object
     *
     * @return
     *   writeable references to an object (existing or newly created one)
     *   at the key
     */
    V& atDatabase(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing database object
     *
     * @param k
     *   key for the object
     *
     * @param v 
     *   reference to an object to be copied into the collection
     *
     * @return
     *   writeable references to the object within the collection
     */
    V& insertDatabase(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k
     *   object's key
     *
     * @return
     *   'true' if  with the specified key already exists
     */
    bool databaseExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k
     *   object's key
     *
     * @return
     *   read-only reference to a database object for a key
     *
     * @throws
     *   std::out_of_range if no such key is found in the collection
     */
    V const& database(KeyType const& k) const { return MapType::get(k); }

    /// @return collection of database names
    std::vector<KeyType> databaseNames() const { return MapType::keys(); }
};

/**
  * Class template ChunkMap has two parameters:
  *
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename V>
class ChunkMap : public SemanticMap<unsigned int,V> {

public:

    /// The key type
    using KeyType = unsigned int;

    /// The base map type
    using MapType = SemanticMap<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    ChunkMap() = default;
    ChunkMap(ChunkMap const&) = default;
    ChunkMap& operator=(ChunkMap const&) = default;

    ~ChunkMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into the collection.
     *
     * @param k
     *   key for an object
     *
     * @return
     *   writeable references to an object (existing or newly created one)
     *   at the key
     */
    V& atChunk(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing chunk object
     *
     * @param k
     *   key for the object
     *
     * @param v
     *   reference to an object to be copied into the collection
     *
     * @return
     *   writeable references to the object within the collection
     */
    V& insertChunk(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k
     *   object's key
     * 
     * @return
     *   'true' if  with the specified key already exists
     */
    bool chunkExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k
     *   object's key
     * 
     * @return
     *   read-only reference to a chunk object for a key
     */
    V const& chunk(KeyType const& k) const { return MapType::get(k); }

    /// @return collection of chunk numbers
    std::vector<KeyType> chunkNumbers() const { return MapType::keys(); }
};

}  // namespace detail

/**
 * 3-layered map template for any value type
 *
 *   .chunk(number).database(name).worker(name) -> T
 */
template<typename T>
using ChunkDatabaseWorkerMap =
        detail::ChunkMap<
            detail::DatabaseMap<
                detail::WorkerMap<T>>>;
 
 /**
  * Dictionary of: worker-chunk-database
  */
template<typename T>
using WorkerChunkDatabaseMap =
        detail::WorkerMap<
            detail::ChunkMap<
                detail::DatabaseMap<T>>>;

/**
 * Dictionary of: worker-database-chunk
 */
template<typename T>
using WorkerDatabaseChunkMap =
        detail::WorkerMap<
            detail::DatabaseMap<
                detail::ChunkMap<T>>>;


// Algorithms are put into a nested namespace below in order
// to avoid confusing them with simple singe-worded user defined
// functions.

namespace SemanticMaps {

/**
 * Merge algorithm for dictionaries of: chunk-database-worker
 *
 * @param dst
 *   destination collection to be extended
 *
 * @param src
 *   input collection whose content is to be merged
 *   into the destination
 *
 * @param ignoreDuplicateKeys
 *   ignore duplicate keys if 'true'. Note that the 'key' in this context
 *   is a composite key of all levels of a map.
 *
 * @throws std::invalid_argument
 *   on attempts to merge a collection with itself
 * 
 * @throws std::range_error
 *   on duplicate keys if ignoreDuplicateKeys is 'false'
 */
 template<typename T>
 void merge(ChunkDatabaseWorkerMap<T>& dst,
            ChunkDatabaseWorkerMap<T> const& src,
            bool ignoreDuplicateKeys = false) {

     for (auto const chunk: src.chunkNumbers()) {
         auto const& srcChunkMap = src.chunk(chunk);
         for (auto&& database: srcChunkMap.databaseNames()) {
             dst.atChunk(chunk).atDatabase(database).merge(
                 srcChunkMap.database(database),
                 ignoreDuplicateKeys);
         }
     }
 }

/**
 * One-directional comparison of dictionaries of: worker-database-chunk
 *
 * The method will also report keys which aren't found in second dictionary.
 *
 * @note
 *   the output dictionary will be modified even if the method
 *   will not find any differences.
 *
 * @param one
 *   input dictionary to be compared with the second one
 *
 * @param two
 *   input dictionary to be compared with the first one
 *
 * @param inFirstOnly
 *   output dictionary with elements of the first map which are not found
 *   in the second map
 *
 * @return 'true' if different
 */
 template<typename T>
 bool diff(WorkerDatabaseChunkMap<T> const& one,
           WorkerDatabaseChunkMap<T> const& two,
           WorkerDatabaseChunkMap<T>& inFirstOnly) {

    inFirstOnly.clear();
    for (auto&& worker: one.workerNames()) {
        if (not         two.workerExists(worker)) {
            inFirstOnly.insertWorker(worker,
                                     one.worker(worker));
            continue;
        }
        for (auto&& database: one.worker(worker).databaseNames()) {
            if (not           two.worker(worker).databaseExists(database)) {
                inFirstOnly.atWorker(worker)
                           .insertDatabase(database,
                                         one.worker(worker).database(database));
                continue;
            }
            for (auto&& chunk: one.worker(worker).database(database).chunkNumbers()) {
                if (not        two.worker(worker).database(database).chunkExists(chunk)) {
                    inFirstOnly.atWorker(worker)
                               .atDatabase(database)
                               .insertChunk(chunk,
                                          one.worker(worker).database(database).chunk(chunk));
                }
            }
        }
    }    
    return not inFirstOnly.empty();
}

/**
 * Bi-directional comparison of dictionaries of: worker-database-chunk
 *
 * The method will also report keys which aren't found in opposite
 * dictionaries.
 *
 * @note
 *   The output dictionaries will be modified even if the method
 *   will not find any differences.
 *
 * @param one
 *   input dictionary to be compared with the second one
 *
 * @param two
 *   input dictionary to be compared with the first one
 *
 * @param inFirstOnly
 *   output dictionary with elements of the first map which are not found
 *   in the second map
 *
 * @param inSecondOnly
 *   output dictionary with elements of the second map which are not found
 *   in the first map
 *
 * @return 'true' if different
 */
 template<typename T>
 bool diff2(WorkerDatabaseChunkMap<T> const& one,
            WorkerDatabaseChunkMap<T> const& two,
            WorkerDatabaseChunkMap<T>& inFirstOnly,
            WorkerDatabaseChunkMap<T>& inSecondOnly) {

    bool const notEqual1 = diff<T>(one, two, inFirstOnly);
    bool const notEqual2 = diff<T>(two, one, inSecondOnly);

    return notEqual1 or notEqual2;
}

/**
 * Find an intersection of two dictionaries of: worker-database-chunk
 *
 * The method will report keys which are found in both dictionaries.
 *
 * @note
 *   The output dictionary will be modified even if the method
 *   will not find any differences.
 *
 * @param one
 *   input dictionary to be compared with the second one
 * 
 * @param two
 *   input dictionary to be compared with the first one
 * 
 * @param inBoth
 *   output dictionary with elements of the first map which
 *   are not found in the second map
 */
 template<typename T>
 void intersect(WorkerDatabaseChunkMap<T> const& one,
                WorkerDatabaseChunkMap<T> const& two,
                WorkerDatabaseChunkMap<T>& inBoth) {

    inBoth.clear();
    for (auto&& worker: one.workerNames()) {
        if (two.workerExists(worker)) {
            for (auto&& database: one.worker(worker).databaseNames()) {
                if (two.worker(worker).databaseExists(database)) {
                    for (auto&& chunk: one.worker(worker).database(database).chunkNumbers()) {
                        if (two.worker(worker).database(database).chunkExists(chunk)) {
                            inBoth.atWorker(worker)
                                  .atDatabase(database)
                                  .insertChunk(chunk,
                                               one.worker(worker).database(database).chunk(chunk));
                        }
                    }
                }
            }
        }
    }
}

/**
 * Count the keys in all leaf nodes
 *
 * @param d
 *   input dictionary to be tested
 *
 * @return
 *   the total number of keys across all leaf nodes
 */
template<typename T>
size_t count(WorkerDatabaseChunkMap<T> const& d) {
    size_t num = 0;
    for (auto&& worker: d.workerNames()) {
        for (auto&& database: d.worker(worker).databaseNames()) {
            num += d.worker(worker).database(database).size();
        }
    }
    return num;
}

}  // namespace SemanticMaps

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SEMANTICMAPS_H
