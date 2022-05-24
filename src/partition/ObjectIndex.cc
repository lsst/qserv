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

// Class header
#include "partition/ObjectIndex.h"

// System headers
#include <stdexcept>
#include <vector>

// LSST headers
#include "partition/Chunker.h"
#include "partition/Constants.h"

using namespace std;

namespace lsst { namespace partition {

ObjectIndex::~ObjectIndex() { close(); }

void ObjectIndex::create(string const& fileName, csv::Editor const& editor, string const& idFieldName,
                         string const& chunkIdFieldName, string const& subChunkIdFieldName) {
    string const context = "ObjectIndex::" + string(__func__) + ": ";
    lock_guard<mutex> lock(_mtx);
    if (_isOpen) return;
    if (fileName.empty()) {
        throw invalid_argument(context + "file name is empty");
    }
    if (idFieldName.empty() || chunkIdFieldName.empty() || subChunkIdFieldName.empty()) {
        throw invalid_argument(context +
                               "at least one of the required field names isn't provided,"
                               " idFieldName='" +
                               idFieldName + "', chunkIdFieldName='" + chunkIdFieldName +
                               "',"
                               " subChunkIdFieldName='" +
                               subChunkIdFieldName + "'");
    }
    _outFileName = fileName;
    _outFile.open(_outFileName, ios_base::out | ios_base::app);
    if (not _outFile.good()) {
        throw runtime_error(context + "failed to open/create index file: '" + _outFileName + "'");
    }

    // Initialize the editor which will be used for formatting text written into
    // the output file. Initialize indexes of the fields to optimize further operations
    // with the editor.
    vector<string> const fields = {idFieldName, chunkIdFieldName, subChunkIdFieldName};
    _outEditorPtr.reset(
            new csv::Editor(editor.getOutputDialect(), editor.getOutputDialect(), fields, fields));
    _outIdField = _outEditorPtr->getFieldIndex(idFieldName);
    _outChunkIdField = _outEditorPtr->getFieldIndex(chunkIdFieldName);
    _outSubChunkIdField = _outEditorPtr->getFieldIndex(subChunkIdFieldName);

    // Allocate buffer to be large enough to accommodate the largest possible result
    // returned by csv::Editor::writeRecord.
    _outBuf.reset(new char[MAX_LINE_SIZE + 1]);

    _mode = Mode::WRITE;
    _isOpen = true;
}

void ObjectIndex::open(string const& url, csv::Dialect const& dialect) {
    string const context = "ObjectIndex::" + string(__func__) + ": ";
    lock_guard<mutex> lock(_mtx);
    if (_isOpen) return;
    // Make sure the file-based specification has a valid syntax, type and has enough room
    // to accommodate the name of the index file.
    string const scheme = "file:///";
    if (url.empty() || url.length() <= scheme.length() + 1 || url.substr(0, scheme.length()) != scheme) {
        throw invalid_argument(context + "invalid index specification: '" + url + "'");
    }
    _inUrl = url;
    // Note that the path should be always absolute in the URL. It's impossible to
    // pass a relative location of a file in this scheme. See details:
    // https://en.wikipedia.org/wiki/File_URI_scheme
    string const fileName = url.substr(scheme.length() - 1);
    ifstream inFile(fileName, ios_base::in);
    if (not inFile.good()) {
        throw runtime_error(context + "failed to open index file: '" + fileName + "'");
    }
    // Field specifications in the index file are random as they don't have any actual
    // names in the input file. What really matters is that there should be at lest 3 fields, and
    // the first 3 of those should represent represent the corresponding roles named in
    // the 'fields' vector below.
    vector<string> const fields = {"id", "chunkId", "subChunkId"};
    csv::Editor editor(dialect, dialect, fields, fields);
    int const idField = editor.getFieldIndex("id");
    int const idChunkIdField = editor.getFieldIndex("chunkId");
    int const idSubChunkIdField = editor.getFieldIndex("subChunkId");
    // Read, parse and the the content of the input file into the index map.
    _inIndexMap.clear();
    for (string line; getline(inFile, line);) {
        editor.setNull(idField);
        editor.setNull(idChunkIdField);
        editor.setNull(idSubChunkIdField);
        editor.readRecord(line.data(), line.data() + line.size());
        string const id = editor.get(idField, true);
        int32_t const chunkId = editor.get<int32_t>(idChunkIdField);
        int32_t const subChunkId = editor.get<int32_t>(idSubChunkIdField);
        _inIndexMap.insert(make_pair(id, make_pair(chunkId, subChunkId)));
    }
    inFile.close();
    _mode = Mode::READ;
    _isOpen = true;
}

void ObjectIndex::close() {
    lock_guard<mutex> lock(_mtx);
    if (not _isOpen) return;
    switch (_mode) {
        case Mode::READ:
            break;
        case Mode::WRITE:
            if (_outFile.is_open()) {
                _outFile.close();
            }
            break;
    }
    _isOpen = false;
}

void ObjectIndex::write(string const& id, ChunkLocation const& location) {
    string const context = "ObjectIndex::" + string(__func__) + ": ";
    lock_guard<mutex> lock(_mtx);
    if (not _isOpen) throw logic_error(context + "index is not open");
    if (Mode::WRITE != _mode) throw logic_error(context + "index is not open in Mode::WRITE");
    if (id.empty()) throw invalid_argument(context + "empty identifier passed as a parameter");
    if (location.chunkId < 0 || location.subChunkId < 0)
        throw invalid_argument(context + "invalid object location passed as a parameter");
    _outEditorPtr->set(_outIdField, id);
    _outEditorPtr->set(_outChunkIdField, location.chunkId);
    _outEditorPtr->set(_outSubChunkIdField, location.subChunkId);
    char const* end = _outEditorPtr->writeRecord(_outBuf.get());
    _outFile.write(_outBuf.get(), end - _outBuf.get());
}

pair<int32_t, int32_t> ObjectIndex::read(string const& id) {
    string const context = "ObjectIndex::" + string(__func__) + ": ";
    lock_guard<mutex> lock(_mtx);
    if (not _isOpen) throw logic_error(context + "index is not open");
    if (Mode::READ != _mode) throw logic_error(context + "index is not open in Mode::READ");
    if (id.empty()) throw invalid_argument(context + "empty identifier passed as a parameter");
    auto const itr = _inIndexMap.find(id);
    if (itr == _inIndexMap.cend()) {
        throw out_of_range(context + "index doesn't have such identifier: '" + id + "'");
    }
    return itr->second;
}

}}  // namespace lsst::partition
