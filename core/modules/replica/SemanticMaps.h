/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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

/// SemanticMaps.h declares:
///
/// (see individual class documentation for more information)

// System headers
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
  * Class template Map is a base class for type-specif collections.
  * This class template has two parameters:
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename K, typename V>
class Map {

public:

    virtual ~Map() = default;

    /// @return number of elements in the collection
    size_t size() const { return _coll.size(); }

    /// @return 'true' if the collection is empty
    bool empty() const { return _coll.empty(); }

    /// @return 'true' if such exists in the collection
    bool exists(K const& k) const { return _coll.count(k); }

    /**
     * Insert a copy of an existing object
     *
     * @param k - its key
     * @param v - object to be insert
     *
     * @return reference to the object within the collection
     */
    V& insert(K const& k, V const& v) {
        _coll[k] = v;
        return _coll[k];
    }

    /**
     * Insert a copy of an existing object
     *
     * @param k - its key
     * @param v - object to be insert
     *
     * @return reference to the object within the collection
     */
    V& insertIfNotExists(K const& k, V const& v) {
        if (not exists(k)) _coll[k] = v;
        return _coll[k];
    }

    /**
     * Merge the content of another collection of the same type
     *
     * @param coll - collection whose content is to be merged
     * @param ignoreDuplicateKeys - ignore duplicate keys if 'true'
     *
     * @throws std::invalid_argument - on atempts to merge a collection with itself
     * @throws std::range_error - on duplicate keys if ignoreDuplicateKeys is 'false'
     */
    void merge(Map<K,V> const& coll, bool ignoreDuplicateKeys = false) {

        if (this == &coll) {
            throw std::invalid_argument("attempted to merge the collection with itself");
        }
        for (auto&& entry: coll._coll) {
            K const& k = entry.first;
            V const& v = entry.second;
            if ((not ignoreDuplicateKeys) and exists(k)) {
                throw std::range_error("key already exists: " + std::to_string(k));
            }
            insert(k, v);
        }
    }

    /**
     * @param k - object's key
     * @return read-only reference to an object for a key
     */
    V const& get(K const& k) const { return _coll.at(k); }

    /**
     * @param k - object's key
     * @return writeable reference to an object for a key
     */
    V& get(K const& k) { return _coll[k]; }

    /// @return collection object keys
    std::vector<K> keys() const {
        std::vector<K> result;
        result.reserve(_coll.size());
        for (auto&& entry: _coll) {
            result.push_back(entry.first);
        }
        return result;
    }

protected:

    // The constructors and the copy operator assume copy by value

    Map() = default;
    Map(Map const&) = default;
    Map& operator=(Map const&) = default;

private:
    std::map<K, V> _coll;
};

/**
  * Class template WorkerMap template has two parameters:
  *
  *   K - the key type (name or a numeric identifier)
  *   V - the value type
  */
template <typename V>
class WorkerMap
    :   public Map<std::string,V> {

public:

    /// The key type
    using KeyType = std::string;

    /// The base map type
    using MapType = Map<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    WorkerMap() = default;
    WorkerMap(WorkerMap const&) = default;
    WorkerMap& operator=(WorkerMap const&) = default;

    ~WorkerMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into the collection.
     *
     * @param k - key for an object
     *
     * @return writeable references to an object (existing or newely created one)
     * at the key
     */
    V& atWorker(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing worker object
     *
     * @param k - key for the object
     * @param v - reference to an object to be copied into the collection
     *
     * @return writeable references to the object within the collection
     */
    V& insertWorker(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k - object's key
     * @return 'true' if  with the specifid key already exists
     */
    bool workerExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k - object's key
     * @return read-only reference to a worker object for a key
     */
    V const& worker(KeyType const& k) const { return MapType::get(k); }

    /**
     * @param k - object's key
     * @return writeable reference to a worker object for a key
     */
    V& worker(KeyType const& k) { return MapType::get(k); }

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
class DatabaseMap
    :   public Map<std::string,V> {

public:

    /// The key type
    using KeyType = std::string;

    /// The base map type
    using MapType = Map<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    DatabaseMap() = default;
    DatabaseMap(DatabaseMap const&) = default;
    DatabaseMap& operator=(DatabaseMap const&) = default;

    ~DatabaseMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into to the collection.
     *
     * @param k - key for an object
     *
     * @return writeable references to an object (existing or newely created one)
     * at the key
     */
    V& atDatabase(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing database object
     *
     * @param k - key for the object
     * @param v - reference to an object to be copied into the collection
     *
     * @return writeable references to the object within the collection
     */
    V& insertDatabase(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k - object's key
     * @return 'true' if  with the specifid key already exists
     */
    bool databaseExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k - object's key
     * @return read-only refeence to a database object for a key
     */
    V const& database(KeyType const& k) const { return MapType::get(k); }

    /**
     * @param k - object's key
     * @return writeable refeence to a database object for a key
     */
    V& database(KeyType const& k) { return MapType::get(k); }

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
class ChunkMap
    :   public Map<unsigned int,V> {

public:

    /// The key type
    using KeyType = unsigned int;

    /// The base map type
    using MapType = Map<KeyType,V>;

    // The constructors and the copy operator assume copy by value

    ChunkMap() = default;
    ChunkMap(ChunkMap const&) = default;
    ChunkMap& operator=(ChunkMap const&) = default;

    ~ChunkMap() override = default;

    /**
     * If no object exists for the specified key then create the one using
     * the default constructor of the value type and insert it into the collection.
     *
     * @param k - key for an object
     *
     * @return writeable references to an object (existing or newely created one)
     * at the key
     */
    V& atChunk(KeyType const& k) { return MapType::insertIfNotExists(k, V()); }

    /**
     * Insert a copy of an existing chunk object
     *
     * @param k - key for the object
     * @param v - reference to an object to be copied into the collection
     *
     * @return writeable references to the object within the collection
     */
    V& insertChunk(KeyType const& k, V const& v) { return MapType::insert(k, v); }

    /**
     * @param k - object's key
     * @return 'true' if  with the specifid key already exists
     */
    bool chunkExists(KeyType const& k) const { return MapType::exists(k); }

    /**
     * @param k - object's key
     * @return read-only reference to a chunk object for a key
     */
    V const& chunk(KeyType const& k) const { return MapType::get(k); }

    /**
     * @param k - object's key
     * @return writeable reference to a chunk object for a key
     */
    V& chunk(KeyType const& k) { return MapType::get(k); }

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
 * 3-layered map template for any value type
 *
 *   .worker(name).chunk(number).database(name) -> T
 */
template<typename T>
using WorkerChunkDatabaseMap =
        detail::WorkerMap<
            detail::ChunkMap<
                detail::DatabaseMap<T>>>;

/**
 * Merge algorithm for the 3-layered map template for any
 * value type:
 *
 *   .chunk(number).database(name).worker(name) -> T
 *
 * @param dst - destination collection to be extended
 * @param src - input collection whose content is to be merged
 *              into the destinaton
 * @param ignoreDuplicateKeys - ignore duplicate keys if 'true'.
 *              NOTE: the 'key' in this context is a composite
 *              key of all levels of a map.
 *
 * @throws std::invalid_argument - on attempts to merge a collection with itself
 * @throws std::range_error - on duplicate keys if ignoreDuplicateKeys is 'false'
 */
 template<typename T>
 void mergeMap(ChunkDatabaseWorkerMap<T>& dst,
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

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_SEMANTICMAPS_H
