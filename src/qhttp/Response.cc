/*
 * LSST Data Management System
 * Copyright 2017 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "qhttp/Response.h"

// System headers
#include <map>
#include <memory>
#include <string>
#include <sstream>
#include <string>

// Third-party headers
#include "boost/asio.hpp"
#include "boost/filesystem.hpp"
#include "boost/filesystem/fstream.hpp"

namespace asio = boost::asio;
namespace ip = boost::asio::ip;
namespace fs = boost::filesystem;

namespace {

std::map<unsigned int, const std::string> responseStringsByCode = {
    {100, "Continue"},                          {101, "Switching Protocols"},
    {102, "Processing"},                        {200, "OK"},
    {201, "Created"},                           {202, "Accepted"},
    {203, "Non-Authoritative Information"},     {204, "No Content"},
    {205, "Reset Content"},                     {206, "Partial Content"},
    {207, "Multi-Status"},                      {208, "Already Reported"},
    {226, "IM Used"},                           {300, "Multiple Choices"},
    {301, "Moved Permanently"},                 {302, "Found"},
    {303, "See Other"},                         {304, "Not Modified"},
    {305, "Use Proxy"},                         {307, "Temporary Redirect"},
    {308, "Permanent Redirect"},                {400, "Bad Request"},
    {401, "Unauthorized"},                      {402, "Payment Required"},
    {403, "Forbidden"},                         {404, "Not Found"},
    {405, "Method Not Allowed"},                {406, "Not Acceptable"},
    {407, "Proxy Authentication Required"},     {408, "Request Timeout"},
    {409, "Conflict"},                          {410, "Gone"},
    {411, "Length Required"},                   {412, "Precondition Failed"},
    {413, "Payload Too Large"},                 {414, "URI Too Long"},
    {415, "Unsupported Media Type"},            {416, "Range Not Satisfiable"},
    {417, "Expectation Failed"},                {421, "Misdirected Request"},
    {422, "Unprocessable Entity"},              {423, "Locked"},
    {424, "Failed Dependency"},                 {426, "Upgrade Required"},
    {428, "Precondition Required"},             {429, "Too Many Requests"},
    {431, "Request Header Fields Too Large"},   {500, "Internal Server Error"},
    {501, "Not Implemented"},                   {502, "Bad Gateway"},
    {503, "Service Unavailable"},               {504, "Gateway Timeout"},
    {505, "HTTP Version Not Supported"},        {506, "Variant Also Negotiates"},
    {507, "Insufficient Storage"},              {508, "Loop Detected"},
    {510, "Not Extended"},                      {511, "Network Authentication Required"},
};


std::unordered_map<std::string, const std::string> contentTypesByExtension = {
    {".css", "text/css"},
    {".gif", "image/gif"},
    {".htm", "text/html"},
    {".html", "text/html"},
    {".jpg", "image/jpeg"},
    {".js", "application/javascript"},
    {".png", "image/png"},
};

} // anon namespace


namespace lsst {
namespace qserv {
namespace qhttp {

Response::Response(
    std::shared_ptr<ip::tcp::socket> socket,
    DoneCallback const &doneCallback)
:
    _socket(socket),
    _doneCallback(doneCallback)
{
}


void Response::sendStatus(unsigned int status)
{
    this->status = status;
    std::string statusStr = responseStringsByCode[status];
    std::ostringstream entStr;
    entStr << "<html>" << std::endl;
    entStr << "<head><title>" << status << " " << statusStr << "</title></head>" << std::endl;
    entStr << "<body style=\"background-color:#E6E6FA\">" << std::endl;
    entStr << "<h1>" << status << " " << statusStr << "</h1>" << std::endl;
    entStr << "</body>" << std::endl;
    entStr << "</html>" << std::endl;
    send(entStr.str());
}


void Response::send(std::string const& content, std::string const& contentType)
{
    headers["Content-Type"] = contentType;
    headers["Content-Length"] = std::to_string(content.length());

    std::ostream responseStream(&_responsebuf);
    responseStream << _headers() << "\r\n" << content;

    auto self = shared_from_this();
    asio::async_write(*_socket, _responsebuf,
        [self](boost::system::error_code const& ec, std::size_t sent) {
            if (self->_doneCallback) {
                self->_doneCallback(ec, sent);
            }
        }
    );
}


void Response::sendFile(fs::path const& path)
{
    if (!fs::exists(path)) {
        sendStatus(404);
        return;
    }

    auto ct = contentTypesByExtension.find(path.extension().string());
    headers["Content-Type"] = (ct != contentTypesByExtension.end()) ? ct->second : "text/plain";

    headers["Content-Length"] = std::to_string(fs::file_size(path));
    fs::ifstream responseFile(path);

    std::ostream responseStream(&_responsebuf);
    responseStream << _headers() << "\r\n" << responseFile.rdbuf();

    auto self = shared_from_this();
    asio::async_write(*_socket, _responsebuf,
        [self](boost::system::error_code const& ec, std::size_t sent) {
            if (self->_doneCallback) {
                self->_doneCallback(ec, sent);
            }
        }
    );
}


std::string Response::_headers() const
{
    std::ostringstream headerst;
    headerst << "HTTP/1.1 ";

    auto r = responseStringsByCode.find(status);
    if (r == responseStringsByCode.end()) r = responseStringsByCode.find(500);
    headerst << r->first << " " << r->second << "\r\n";

    for(auto const& h : headers) {
        headerst << h.first << ": " << h.second << "\r\n";
    }

    return headerst.str();
}

}}} // namespace lsst::qserv::qhttp
