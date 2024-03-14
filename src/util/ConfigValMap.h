// -*- LSST-C++ -*-
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

#ifndef LSST_QSERV_UTIL_CONFIGVALMAP_H
#define LSST_QSERV_UTIL_CONFIGVALMAP_H

// System headers
//&&&#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <sstream>

// Third party headers
#include <nlohmann/json.hpp>

// Qserv headers
//&&&#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/Issue.h"

namespace lsst::qserv::util {

/// Class for handling configuration exceptions.
class ConfigException : public util::Issue {
public:
    ConfigException(Context const& ctx, std::string const& msg) : util::Issue(ctx, msg) {}
};

class ConfigValMap;

class ConfigVal {
public:
    using Ptr = std::shared_ptr<ConfigVal>;

    ConfigVal() = delete;
    ConfigVal(std::string const& section, std::string const& name, bool required)
            : _section(section), _name(name), _required(required) {}
    virtual ~ConfigVal() = default;

    ConfigVal(ConfigVal const&) = delete;
    ConfigVal& operator=(ConfigVal const&) = delete;

    std::string getSection() const { return _section; }
    std::string getName() const { return _name; }
    std::string getSectionName() const { return _section + "." + _name; }

    /// Return true if the value is required to be from a file.
    bool isRequired() const { return _required; }

    /// Calling this indicates this value should not be shown to users or put in logs.
    void setHidden() { _hidden = true; }

    /// Return true if this value should not be seen by users or logs.
    bool isHidden() const { return _hidden; }

    bool isValSetFromFile() const { return _valSetFromFile; }

    /// All child classes should be able to return a valid string version of their value,
    /// but this function will hide values of `_hidden` `ConfigVal`.
    /// If the string value of something that is hidden is needed, call getValStrDanger().
    virtual std::string getValStr() const final {
        if (!isHidden()) {
            return getValStrDanger();
        }
        return "*****";
    }

    /// All child classes should be able to return a valid string version of their value,
    /// this function will show `_hidden` values.
    virtual std::string getValStrDanger() const = 0;

    /// &&& doc
    virtual void setValFromConfigStore(util::ConfigStore const& configStore) final;

    /// &&& doc
    virtual void setValFromConfigStoreChild(util::ConfigStore const& configStore) = 0;

    /// @throws ConfigException if the value is invalid or already in the map.
    static void addToMapBase(ConfigValMap& configValMap, Ptr const&);

private:
    std::string const _section;
    std::string const _name;
    bool const _required;          ///< &&& doc
    bool _hidden = false;          ///< &&& doc
    std::string _strFromCfg;       ///< Original string read from the configuration file.
    bool _valSetFromFile = false;  ///< set to true if value was set from configuration file.

protected:
    void setValSetFromFile(bool setFromFile) { _valSetFromFile = setFromFile; }
    /// &&& doc
    void logValSet(std::string const& msg = std::string());
};

/// &&& doc
template <typename T>
class ConfigValT : public ConfigVal {
public:
    static std::shared_ptr<ConfigValT<T>> create(std::string const& section, std::string const& name,
                                                 bool required, T defVal, ConfigValMap& configValMap) {
        auto newPtr = std::shared_ptr<ConfigValT<T>>(new ConfigValT<T>(section, name, required, defVal));
        addToMapBase(configValMap, newPtr);
        return newPtr;
    }

    virtual ~ConfigValT() = default;

    /// Return the string value of non-hidden config values. (setH
    std::string getValStrDanger() const override {
        std::stringstream os;
        os << _val;
        return os.str();
    }
    T const& getVal() const { return _val; }
    T const& getDefVal() const { return _defVal; }
    void setVal(T val) {
        _val = val;
        logValSet();
    }

protected:
    ConfigValT(std::string const& section, std::string const& name, bool required, T defVal)
            : ConfigVal(section, name, required), _val(defVal), _defVal(defVal) {}

private:
    T _val;     ///< &&& doc
    T _defVal;  ///< &&& doc
};

/// Bool is special case for json as the value should be "true" or "false" but
/// ConfigStore has it as '0' or '1'.
class ConfigValTBool : public ConfigValT<bool> {
public:
    using BoolPtr = std::shared_ptr<ConfigValTBool>;

    ConfigValTBool() = delete;
    virtual ~ConfigValTBool() = default;

    static BoolPtr create(std::string const& section, std::string const& name, bool required, bool defVal,
                          ConfigValMap& configValMap) {
        auto newPtr = BoolPtr(new ConfigValTBool(section, name, required, defVal));
        addToMapBase(configValMap, newPtr);
        return newPtr;
    }

    /// &&& doc
    static std::string toString(bool bVal);

    /// Return the string value of non-hidden config values. (setH
    std::string getValStrDanger() const override { return toString(getVal()); }

    void setValFromConfigStoreChild(util::ConfigStore const& configStore) override {
        setVal(configStore.getIntRequired(getSectionName()));
    }

private:
    ConfigValTBool(std::string const& section, std::string const& name, bool required, bool defVal)
            : ConfigValT<bool>(section, name, required, defVal) {}
};

/// &&& doc
class ConfigValTStr : public ConfigValT<std::string> {
public:
    using StrPtr = std::shared_ptr<ConfigValTStr>;

    ConfigValTStr() = delete;
    virtual ~ConfigValTStr() = default;

    static StrPtr create(std::string const& section, std::string const& name, bool required,
                         std::string const& defVal, ConfigValMap& configValMap) {
        auto newPtr = StrPtr(new ConfigValTStr(section, name, required, defVal));
        addToMapBase(configValMap, newPtr);
        return newPtr;
    }

    void setValFromConfigStoreChild(util::ConfigStore const& configStore) override {
        setVal(configStore.getRequired(getSectionName()));
    }

private:
    ConfigValTStr(std::string const& section, std::string const& name, bool required,
                  std::string const& defVal)
            : ConfigValT<std::string>(section, name, required, defVal) {}
};

// &&& doc
class ConfigValTInt : public ConfigValT<int64_t> {
public:
    using IntPtr = std::shared_ptr<ConfigValTInt>;

    ConfigValTInt() = delete;
    virtual ~ConfigValTInt() = default;

    static IntPtr create(std::string const& section, std::string const& name, bool required, int64_t defVal,
                         ConfigValMap& configValMap) {
        auto newPtr = IntPtr(new ConfigValTInt(section, name, required, defVal));
        addToMapBase(configValMap, newPtr);
        return newPtr;
    }

    void setValFromConfigStoreChild(util::ConfigStore const& configStore) override {
        setVal(configStore.getIntRequired(getSectionName()));
    }

private:
    ConfigValTInt(std::string const& section, std::string const& name, bool required, int64_t defVal)
            : ConfigValT<int64_t>(section, name, required, defVal) {}
};

// &&& doc
class ConfigValTUInt : public ConfigValT<uint64_t> {
public:
    using UIntPtr = std::shared_ptr<ConfigValTUInt>;

    ConfigValTUInt() = delete;
    virtual ~ConfigValTUInt() = default;

    static UIntPtr create(std::string const& section, std::string const& name, bool required, uint64_t defVal,
                          ConfigValMap& configValMap) {
        auto newPtr = UIntPtr(new ConfigValTUInt(section, name, required, defVal));
        addToMapBase(configValMap, newPtr);
        return newPtr;
    }

    void setValFromConfigStoreChild(util::ConfigStore const& configStore) override {
        /// &&& check for negative val
        setVal(configStore.getIntRequired(getSectionName()));
    }

private:
    ConfigValTUInt(std::string const& section, std::string const& name, bool required, uint64_t defVal)
            : ConfigValT<uint64_t>(section, name, required, defVal) {}
};

class ConfigValMap {
public:
    using NameMap = std::map<std::string, ConfigVal::Ptr>;  ///< key is ConfigVal::_name
    using SectionMap = std::map<std::string, NameMap>;      ///< key is ConfigVal::_section

    /// &&& doc
    void addEntry(ConfigVal::Ptr const& newVal);

    /// &&& doc
    ConfigVal::Ptr getEntry(std::string const& section, std::string const& name);

    /// &&& doc
    /// @throws ConfigException if there are problems.
    void readConfigStore(util::ConfigStore const& configStore);

    /// &&& doc
    std::tuple<bool, std::string> checkRequired() const;

    /// &&& doc
    void populateJson(nlohmann::json& js, std::string const& coll);  // &&& add const

private:
    SectionMap _sectionMap;
};
}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_CONFIGVALMAP_H
