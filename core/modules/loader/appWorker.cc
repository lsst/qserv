// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 AURA/LSST.
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

// System headers
#include <iostream>
#include <unistd.h>
//#include <sys/types.h> // &&&
//#include <sys/socket.h> // &&&
//#include <netdb.h> // &&&

// qserv headers
#include "loader/CentralWorker.h"
#include "loader/Util.h"

// LSST headers
#include "lsst/log/Log.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.loader.appWorker");
}

using namespace lsst::qserv::loader;
using  boost::asio::ip::udp;


int main(int argc, char* argv[]) {
    std::string wCfgFile("core/modules/loader/config/worker1.cnf");
    if (argc > 1) {
        wCfgFile = argv[1];
    }
    LOGS(_log, LOG_LVL_INFO, "workerCfg=" << wCfgFile);

    boost::asio::io_service ioService;
    boost::asio::io_context ioContext;

    /* &&&
    if (!splitTest()) {
        LOGS(_log, LOG_LVL_ERROR, "split test failed! &&&");
        exit(1);
    }


    std::string const ourHost = boost::asio::ip::host_name();
    std::string ourHostIp;
    LOGS(_log, LOG_LVL_INFO, "ourHost=" << ourHost);
    boost::asio::io_service ioService;
    boost::asio::io_context ioContext;

    {
        char hostbuffer[256];
        char *IPbuffer;
        struct hostent *host_entry;
        int hostname;
        hostname = gethostname(hostbuffer, sizeof(hostbuffer));

        // To retrieve host information
        //host_entry = gethostbyname(hostbuffer);
        host_entry = gethostbyname(ourHost.c_str());

        // To convert an Internet network
        // address into ASCII string
        IPbuffer = inet_ntoa(*((struct in_addr*) // &&& replace with inet_ntop
                host_entry->h_addr_list[0]));
        LOGS(_log, LOG_LVL_ERROR, "hostname=" << hostname << " buf=" << hostbuffer);
        LOGS(_log, LOG_LVL_ERROR, "host_entry=" << host_entry);
        LOGS(_log, LOG_LVL_ERROR, "IPbuffer=" << IPbuffer);
        ourHostIp = IPbuffer;

        //gethostbyaddr(); &&&
        hostent *he;
        in_addr ipv4addr;
        //in6_addr ipv6addr;

        inet_pton(AF_INET, ourHostIp.c_str(), &ipv4addr);
        he = gethostbyaddr(&ipv4addr, sizeof ipv4addr, AF_INET);
        if (he == nullptr) {
            printf("he == nullptr\n");
        } else {
            printf("Host name: %s\n", he->h_name);
            LOGS(_log, LOG_LVL_INFO, " host name=" << he->h_name); // *** this gets the correct  full name

            for(int i=0; he->h_aliases[i] != NULL; ++i) {
                LOGS(_log, LOG_LVL_INFO, std::to_string(i) << " host=" << he->h_aliases[i]);
            }
        }

        //inet_pton(AF_INET6, "2001:db8:63b3:1::beef", &ipv6addr);
        //he = gethostbyaddr(&ipv6addr, sizeof ipv6addr, AF_INET6);
        //printf("Host name: %s\n", he->h_name);

    }


    {
        addrinfo hints, *info, *p;
        int gai_result;

        char hostname[1024];
        hostname[1023] = '\0';
        gethostname(hostname, 1023);
        //std::string hostname("127.0.0.1");
        printf("hostname0: %s\n", hostname);

        memset(&hints, 0, sizeof hints);
        hints.ai_family = AF_UNSPEC;
        //hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_CANONNAME;

        //if ((gai_result = getaddrinfo(hostname, "http", &hints, &info)) != 0) {
        if ((gai_result = getaddrinfo(hostname, NULL, &hints, &info)) != 0) {
            fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai_result));
            exit(1);
        }

        for(p = info; p != NULL; p = p->ai_next) {
            printf("hostname1: %s\n", p->ai_canonname);  // ** correct
            LOGS(_log, LOG_LVL_INFO, "*a*hostname1: " << p->ai_canonname);
        }

        freeaddrinfo(info);

    }

    {
            addrinfo hints, *info, *p;
            int gai_result;

            char hostname[1024];
            hostname[1023] = '\0';
            gethostname(hostname, 1023);
            //std::string hostname("127.0.0.1");
            printf("hostname0: %s\n", hostname);

            memset(&hints, 0, sizeof hints);
            hints.ai_family = AF_UNSPEC;
            //hints.ai_socktype = SOCK_STREAM;
            //hints.ai_flags = AI_CANONNAME;

            //if ((gai_result = getaddrinfo(hostname, "http", &hints, &info)) != 0) {
            if ((gai_result = getaddrinfo(hostname, NULL, &hints, &info)) != 0) {
                fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(gai_result));
                exit(1);
            }

            for(p = info; p != NULL; p = p->ai_next) {
                printf("hostname1: %s\n", p->ai_canonname);
                LOGS(_log, LOG_LVL_INFO, "*b*hostname1: " << p->ai_canonname);
            }

            freeaddrinfo(info);

        }

    {
        addrinfo hints;
        addrinfo *infoptr;
        hints.ai_family = AF_INET; // AF_INET means IPv4 only addresses

        //int result = getaddrinfo("www.bbc.com", NULL, &hints, &infoptr);
        //int result = getaddrinfo("127.0.0.1", NULL, &hints, &infoptr); // results in hostname "localhost"
        int result = getaddrinfo(ourHostIp.c_str(), NULL, &hints, &infoptr);
        if (result) {
            fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(result));
            exit(1);
        }


        struct addrinfo *p;
        char host[1024];

        for (p = infoptr; p != NULL; p = p->ai_next) {
            //getnameinfo(p->ai_addr, p->ai_addrlen, host, sizeof (host), NULL, 0, NI_NUMERICHOST);
            getnameinfo(p->ai_addr, p->ai_addrlen, host, sizeof (host), NULL, 0, 0);
            LOGS(_log, LOG_LVL_INFO, "getnameinfo host=" << host << " addr=" << p->ai_addr); // ** correct
        }

        freeaddrinfo(infoptr);



    }


    auto hostN2 = getOurHostName(2);
    LOGS(_log, LOG_LVL_INFO, "hostN2=" << hostN2);
    auto hostN1 = getOurHostName(1);
    LOGS(_log, LOG_LVL_INFO, "hostN1=" << hostN1);
    auto hostN0 = getOurHostName(0);
    LOGS(_log, LOG_LVL_INFO, "hostN0=" << hostN0);
    auto hostN10 = getOurHostName(10);
    LOGS(_log, LOG_LVL_INFO, "hostN10=" << hostN10);
    auto hostN4 = getOurHostName(4);
    LOGS(_log, LOG_LVL_INFO, "hostN4=" << hostN4);

    //exit(1);
*/

    std::string ourHostName = getOurHostName(0); // change to return shortest name that resolves. &&&
    LOGS(_log, LOG_LVL_INFO, "ourHostName=" << ourHostName);

    WorkerConfig wCfg(wCfgFile);
    CentralWorker cWorker(ioService, ioContext, ourHostName, wCfg);
    try {
        cWorker.start();
    } catch (boost::system::system_error const& e) {
        LOGS(_log, LOG_LVL_ERROR, "cWorker.start() failed e=" << e.what());
        return 1;
    }
    cWorker.runServer();

    bool loop = true;
    while(loop) {
        sleep(10);
    }
    ioService.stop(); // this doesn't seem to work cleanly
    LOGS(_log, LOG_LVL_INFO, "worker DONE");
}

