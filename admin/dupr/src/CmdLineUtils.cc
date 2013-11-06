/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#include "CmdLineUtils.h"

#include <cstdlib>
#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <vector>

#include "Constants.h"
#include "FileUtils.h"

using std::bad_alloc;
using std::cerr;
using std::cout;
using std::endl;
using std::exit;
using std::find;
using std::free;
using std::logic_error;
using std::malloc;
using std::map;
using std::pair;
using std::runtime_error;
using std::set;
using std::string;
using std::vector;

using boost::shared_ptr;

namespace fs = boost::filesystem;
namespace po = boost::program_options;


namespace {

    // A configuration file parser that understands a forgiving format
    // based on JSON. The parser recognizes JSON, but allows short-cuts
    // so that configuration files are easier to write.
    //
    // This class exists because the INI file parser that ships with boost
    // has no escaping mechanism and automatically strips whitespace from
    // option values. As a consequence, setting an option value to e.g.
    // the TAB character in a config file is impossible with stock boost.
    //
    // The format consists of groups, strings, and key-value pairs, where the
    // configuration file contents belong to an implicit top-level group.
    // Keys are strings, and values are either strings or groups. A string
    // does not have to be quoted unless it contains whitespace, escape
    // sequences, control characters, a leading quote, or one of ",:=#[]{}()".
    // Both " and ' are recognized as quote characters, and escape sequences
    // are defined as in JSON.
    //
    // Groups contain values and/or key-value pairs (where ':' or '=' separate
    // keys from values). They are opened with '{', '[' or '(', and closed
    // with ')', ']' or '}'. Values and key-value pairs may be separated by
    // whitespace or commas; trailing commas are permitted.
    //
    // These structures are mapped to command line options by flattening.
    // To illustrate:
    //
    //     {
    //         a: {
    //             b:c,
    //             d,
    //         }
    //     },
    //     e,
    //
    // and
    //
    //     a: {b:c d}, e
    //
    // and
    //
    //     a=(b=c d) e
    //
    // are all equivalent to specifying --a.b=c --a=d --e on the command line.
    // Nested key names are joined by a key-separator character to construct
    // option names - in this case, '.' is being used.
    //
    // The '#' character begins a comment which extends to the end of the line
    // it occurs on. CR, LF and CRLF are all recognized as line terminators.
    class Parser {
    public:
        explicit Parser(fs::path const & path, char keySeparator);
        ~Parser();

        po::parsed_options const parse(po::options_description const & desc,
                                       bool verbose);

    private:
        Parser(Parser const &);
        Parser & operator=(Parser const &);

        fs::path _path;
        char * _data;
        char const * _cur;
        char const * _end;
        char _sep;

        string const _join(vector<string> const & keys);
        string const _parseValue();
        string const _parseQuotedValue(char const quote);
        string const _parseUnicodeEscape();

        void _skipWhitespace() {
            for (; _cur < _end; ++_cur) {
                char c = *_cur;
                if (c != '\t' && c != '\n' && c != '\r' && c != ' ') {
                    break;
                }
            }
        }
        void _skipLine() {
            for (; _cur < _end; ++_cur) {
                char c = *_cur;
                if (c == '\r' || c == '\n') {
                    break;
                }
            }
        }
    };

    Parser::Parser(fs::path const & path, char keySeparator) :
        _path(path), _data(0), _cur(0), _end(0), _sep(keySeparator)
    {
        lsst::qserv::admin::dupr::InputFile f(path);
        // FIXME(smm): check that cast doesn't truncate 
        size_t sz = static_cast<size_t>(f.size());
        _data = static_cast<char *>(malloc(sz));
        if (!_data) {
            throw bad_alloc();
        }
        _cur = _data;
        _end = _data + sz;
        f.read(_data, 0, sz);
    }

    Parser::~Parser() {
        free(_data);
        _data = 0;
        _cur = 0;
        _end = 0;
    }

    string const Parser::_join(vector<string> const & keys) {
        typedef vector<string>::const_iterator Iter;
        string key;
        for (Iter k = keys.begin(), ke = keys.end(); k != ke; ++k) {
            if (k->empty()) {
                continue;
            }
            size_t i = k->find_first_not_of(_sep);
            if (i == string::npos) {
                continue;
            }
            size_t j = k->find_last_not_of(_sep);
            if (!key.empty()) {
                key.push_back(_sep);
            }
            key.append(*k, i, j - i + 1);
        }
        return key;
    }

    string const Parser::_parseValue() {
        string val;
        while (_cur < _end) {
            char const c = *_cur;
            switch (c) {
                case '\t': case '\n': case '\r': case ' ':
                case '#': case ',': case ':': case '=':
                case '(': case ')': case '[': case ']': case '{': case '}':
                    return val;
                default:
                    if (c < 0x20) {
                        throw runtime_error("Unquoted values must not "
                                            "contain control characters.");
                    }
                    break;
            }
            val.push_back(c);
            ++_cur;
        }
        return val;
    }

    string const Parser::_parseUnicodeEscape() {
        string val;
        unsigned int cp = 0;
        int i = 0;
        // Extract 1-4 hexadecimal digits to build a Unicode
        // code-point in the Basic Multilingual Plane.
        for (; i < 4 && _cur < _end; ++i, ++_cur) {
            char c = *_cur;
            if (c >= '0' && c <= '9') {
                cp = (cp << 4) + static_cast<unsigned int>(c - '0');
            } else if (c >= 'a' && c <= 'f') {
                cp = (cp << 4) + static_cast<unsigned int>(c - 'a') + 10u;
            } else if (c >= 'A' && c <= 'F') {
                cp = (cp << 4) + static_cast<unsigned int>(c - 'A') + 10u;
            } else {
                break;
            }
        }
        if (i == 0) {
            throw runtime_error("Invalid unicode escape in quoted value");
        }
        // UTF-8 encode the code-point.
        if (cp <= 0x7f) {
            // 0xxxxxxx
            val.push_back(static_cast<char>(cp));
        } else if (cp <= 0x7ff) {
            // 110xxxxx 10xxxxxx
            val.push_back(static_cast<char>(0xc0 | (cp >> 6)));
            val.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        } else {
            if (cp <= 0xffff) {
                throw logic_error("Unicode escape sequence produced code-point "
                                  "outside the Basic Multilingual Plane");
            }
            // 1110xxxx 10xxxxxx 10xxxxxx
            val.push_back(static_cast<char>(0xe0 | (cp >> 12)));
            val.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3f)));
            val.push_back(static_cast<char>(0x80 | (cp & 0x3f)));
        }
        return val;
    }

    string const Parser::_parseQuotedValue(char const quote) {
        string val;
        while (true) {
            if (_cur >= _end) {
                throw runtime_error("Unmatched quote character.");
            }
            char c = *_cur;
            if (c == quote) {
                ++_cur;
                break;
            } else if (c == '\\') {
                // Handle escape sequence.
                ++_cur;
                if (_cur== _end) {
                    throw runtime_error("Unmatched quote character.");
                }
                c = *_cur;
                switch (c) {
                    case 'b': c = '\b'; break;
                    case 'f': c = '\f'; break;
                    case 'n': c = '\n'; break;
                    case 'r': c = '\r'; break;
                    case 't': c = '\t'; break;
                    case 'u':
                        ++_cur;
                        val.append(_parseUnicodeEscape());
                        continue;
                    default:
                        break;
                }
            }
            val.push_back(c);
            ++_cur;
        }
        return val;
    }

    po::parsed_options const Parser::parse(
        po::options_description const & desc, bool verbose)
    {
        set<string> registered;
        for (vector<shared_ptr<po::option_description> >::const_iterator i =
             desc.options().begin(), e = desc.options().end(); i != e; ++i) {
            if ((*i)->long_name().empty()) {
                throw logic_error(string("Abbreviated option names are not "
                                         "allowed in configuration files."));
            }
            registered.insert((*i)->long_name());
        }
        po::parsed_options parsed(&desc);
        po::option opt;
        vector<string> keys;
        vector<pair<int, char> > groups;
        pair<int, char> p(0, '\0');
        groups.push_back(p);
        int lvl = 0;
        for (_skipWhitespace(); _cur < _end; _skipWhitespace()) {
            char c = *_cur;
            string s;
            switch (c) {
                case '#':
                    ++_cur;
                    _skipLine();
                    continue;
                case ',':
                    ++_cur;
                    continue;
                case '(': case '[': case '{':
                    ++_cur;
                    groups.push_back(pair<int, char>(lvl, c));
                    continue;
                case ')': case ']': case '}':
                    ++_cur;
                    p = groups.back();
                    groups.pop_back();
                    if (p.second == '(' && c != ')') {
                        throw runtime_error("Unmatched (.");
                    } else if (p.second == '[' && c != ']') {
                        throw runtime_error("Unmatched [.");
                    } else if (p.second == '{' && c != '}') {
                        throw runtime_error("Unmatched {.");
                    } else if (p.second == '\0') {
                        throw runtime_error("Unmatched ), ], or }.");
                    }
                    for (; lvl > groups.back().first; --lvl) {
                        keys.pop_back();
                    }
                    continue;
                case '"': case '\'':
                    ++_cur;
                    s = _parseQuotedValue(c);
                    break;
                default:
                    s = _parseValue();
                    break;
            }
            _skipWhitespace();
            c = (_cur < _end) ? *_cur : ',';
            if (c == ':' || c == '=') {
                ++_cur;
                keys.push_back(s);
                ++lvl;
                continue;
            }
            opt.value.clear();
            opt.original_tokens.clear();
            if (keys.empty()) {
                opt.string_key = s;
            } else {
                opt.string_key = _join(keys);
                opt.value.push_back(s);
                opt.original_tokens.push_back(opt.string_key);
            }
            opt.unregistered =
                (registered.find(opt.string_key) == registered.end());
            opt.original_tokens.push_back(s);
            for (; lvl > groups.back().first; --lvl) {
                keys.pop_back();
            }
            if (opt.unregistered && verbose) {
                cerr << "Skipping unrecognized option --" << opt.string_key
                     <<  " in config file " << _path.string() << endl;
            }
            parsed.options.push_back(opt);
        }
        if (!keys.empty() || lvl != 0 || groups.size() != 1u) {
            throw runtime_error("Missing value for key, "
                                "or unmatched (, [ or {.");
        }
        return parsed;
    }

} // unnamed namespace


namespace lsst {
namespace qserv {
namespace admin {
namespace dupr {

FieldNameResolver::~FieldNameResolver() {
    _editor = 0;
}

int FieldNameResolver::resolve(string const & option,
                               string const & value,
                               string const & fieldName,
                               bool unique)
{
    int i = _editor->getFieldIndex(fieldName);
    if (i < 0) {
        throw runtime_error("--" + option + "=\"" + value +
                            "\" specifies an unrecognized field.");
    }
    if (!_fields.insert(i).second && unique) {
        throw runtime_error("--" + option + "=\"" + value +
                            "\" specifies a duplicate field.");
    }
    return i;
}


void parseCommandLine(po::variables_map & vm,
                      po::options_description const & options,
                      int argc,
                      char const * const * argv,
                      char const * help)
{
    // Define common options.
    po::options_description common("\\_____________________ Common", 80);
    common.add_options()
        ("help,h",
         "Demystify program usage.")
        ("verbose,v",
         "Chatty output.")
        ("config-file,c", po::value<vector<string> >(),
         "The name of a configuration file containing program option values "
         "in a JSON-like format. May be specified any number of times. If an "
         "option is specified more than once, the first specification "
         "usually takes precedence. Command line options have the highest "
         "precedence, followed by configuration files, which are parsed in "
         "the order specified on the command-line and should therefore be "
         "listed in most to least specific order. Note that the config-file "
         "option itself is not recognized inside of a configuration file.");
    po::options_description all;
    all.add(common).add(options);
    // Parse command line. Older boost versions (1.41) require the const_cast.
    po::store(po::parse_command_line(argc, const_cast<char **>(argv), all), vm);
    po::notify(vm);
    if (vm.count("help") != 0) {
        cout << argv[0]  << " [options]\n\n" << help << "\n" << all << endl;
        exit(EXIT_SUCCESS);
    }
    bool verbose = vm.count("verbose") != 0;
    // Parse configuration files, if any.
    if (vm.count("config-file") != 0) {
        typedef vector<string>::const_iterator Iter;
        vector<string> files = vm["config-file"].as<vector<string> >();
        for (Iter f = files.begin(), e = files.end(); f != e; ++f) {
            Parser p(fs::path(*f), '.');
            po::store(p.parse(options, verbose), vm);
            po::notify(vm);
        }
    }
}

namespace {
    string const trim(string const & s) {
        static char const * const WS = "\t\n\r ";
        size_t i = s.find_first_not_of(WS);
        if (i == string::npos) {
            return string();
        }
        return s.substr(i, s.find_last_not_of(WS) - i + 1);
    }
}

pair<string, string> const parseFieldNamePair(string const & opt,
                                              string const & val)
{
    pair<string, string> p;
    size_t i = val.find_first_of(',');
    if (i == string::npos) {
        throw runtime_error("--" + opt + "=" + val +
                            " is not a comma separated field name pair.");
    }
    if (val.find_first_of(',', i + 1) != string::npos) {
        throw runtime_error("--" + opt + "=" + val +
                            " is not a comma separated field name pair.");
    }
    p.first = trim(val.substr(0, i));
    p.second = trim(val.substr(i + 1));
    if (p.first.empty() || p.second.empty()) {
        throw runtime_error("--" + opt + "=" + val +
                            " is not a comma separated field name pair.");
    }
    return p;
}


void defineInputOptions(po::options_description & opts) {
    po::options_description input("\\______________________ Input", 80);
    input.add_options()
        ("in,i", po::value<std::vector<std::string> >(),
         "An input file or directory name. If the name identifies a "
         "directory, then all the files and symbolic links to files in "
         "the directory are treated as inputs. This option must be "
         "specified at least once.");
    opts.add(input);
}


InputLines const makeInputLines(po::variables_map & vm) {
    typedef vector<string>::const_iterator Iter;
    size_t blockSize = vm["mr.block-size"].as<size_t>();
    if (blockSize < 1 || blockSize > 1024) {
        throw runtime_error("The IO block size given by --mr.block-size "
                            "must be between 1 and 1024 MiB.");
    }
    if (vm.count("in") == 0) {
        throw runtime_error("At least one input file must be provided "
                            "using --in.");
    }
    vector<fs::path> paths;
    vector<string> const & in = vm["in"].as<vector<string> >();
    for (Iter s = in.begin(), se = in.end(); s != se; ++s) {
        fs::path p(*s);
        fs::file_status stat = fs::status(p);
        if (stat.type() == fs::regular_file && fs::file_size(p) > 0) {
            paths.push_back(p);
        } else if (stat.type() == fs::directory_file) {
            for (fs::directory_iterator d(p), de; d != de; ++d) {
                if (d->status().type() == fs::regular_file &&
                    fs::file_size(p) > 0) {
                    paths.push_back(d->path());
                }
            }
        }
    }
    if (paths.empty()) {
        throw runtime_error("No non-empty input files found among the "
                            "files and directories specified via --in.");
    }
    return InputLines(paths, blockSize*MiB, false);
}


void defineOutputOptions(po::options_description & opts) {
    po::options_description output("\\_____________________ Output", 80);
    output.add_options()
        ("out.dir", po::value<string>(),
         "The directory to write output files to.")
        ("out.num-nodes", po::value<uint32_t>()->default_value(1u),
         "The number of down-stream nodes that will be using the output "
         "files. If this is more than 1, then output files are assigned to "
         "nodes by hashing and are placed into a sub-directory of out.dir "
         "named node_XXXXX, where XXXXX is a logical node ID between 0 and "
         "out.num-nodes - 1.");
    opts.add(output);
}


void makeOutputDirectory(po::variables_map & vm, bool mayExist) {
    fs::path outDir;
    if (vm.count("out.dir") != 0) {
        outDir = vm["out.dir"].as<string>();
    }
    if (outDir.empty()) {
        cerr << "No output directory specified (use --out.dir)." << endl;
        exit(EXIT_FAILURE);
    }
    outDir = fs::system_complete(outDir);
    if (outDir.filename() == ".") {
        // fs::create_directories returns false for "does_not_exist/", even
        // when "does_not_exist" must be created. This is because the
        // trailing slash causes the last path component to be ".", which
        // exists once it is iterated to.
        outDir.remove_filename();
    }
    map<string, po::variable_value> & m = vm;
    po::variable_value & v = m["out.dir"];
    v.value() = outDir.string();
    if (fs::create_directories(outDir) == false && !mayExist) {
        cerr << "The output directory --out.dir=" << outDir.string()
             << " already exists - please choose another." << endl;
        exit(EXIT_FAILURE);
    }
}


void ensureOutputFieldExists(po::variables_map & vm, std::string const & opt) {
    if (vm.count(opt) == 0) {
        return;
    }
    vector<string> names;
    if (vm.count("out.csv.field") == 0) {
        if (vm.count("in.csv.field") == 0) {
            cerr << "Input CSV field names not specified." << endl;
            exit(EXIT_FAILURE);
        }
        names = vm["in.csv.field"].as<vector<string> >();
    } else {
        names = vm["out.csv.field"].as<vector<string> >();
    }
    string name = vm[opt].as<string>();
    if (find(names.begin(), names.end(), name) == names.end()) {
        names.push_back(name);
    }
    map<string, po::variable_value> & m = vm;
    po::variable_value & v = m["out.csv.field"];
    v.value() = names;
}


vector<int32_t> const chunksToDuplicate(Chunker const & chunker,
                                        po::variables_map const & vm)
{
    if (vm.count("chunk-id") != 0) {
        return vm["chunk-id"].as<vector<int32_t> >();
    }
    SphericalBox region(vm["lon-min"].as<double>(),
                        vm["lon-max"].as<double>(),
                        vm["lat-min"].as<double>(),
                        vm["lat-max"].as<double>());
    uint32_t node = 0;
    uint32_t numNodes = 1;
    if (vm.count("out.node") != 0) {
        node = vm["out.node"].as<uint32_t>();
        numNodes = vm["out.num-nodes"].as<uint32_t>();
        if (node >= numNodes) {
            runtime_error("The --out.node option value "
                          "must be less than --out.num-nodes.");
        }
    }
    return chunker.getChunksIn(region, node, numNodes);
}

}}}} // namespace lsst::qserv::admin::dupr
