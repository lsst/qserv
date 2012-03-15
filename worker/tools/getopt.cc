
#include <iostream>

using std::cout;
using std::cerr;
using std::endl;

void printHelp() {
    std::cout
        << "\nUsage:\n\n"
        << "   registerDb -r -d <dbName> -t <tables>\n"
        << "   registerDb -g -a -b <baseDir>\n"
        << "   registerDb -g -d <dbName> -b <baseDir>\n"
        << "   registerDb -h\n"
        << "\nWhere:\n\n"
        << "  -r           - register database in qserv metadata\n"
        << "  -g           - generate export paths\n"
        << "  -a           - for all databases registered in qserv metadata\n"
        << "  -d <dbName>  - database name\n"
        << "  -t <tables>  - comma-separated list of partitioned tables\n"
        << "  -b <baseDir> - base directory\n"
        << "  -h           - prints help and exits\n"
        << endl;
}

int
main (int argc, char **argv) {
    std::string dbName;
    std::string pTables;
    std::string baseDir;

    bool flag_regDb = false;
    bool flag_genEp = false;
    bool flag_allDb = false;

    int c;
    opterr = 0;
    while ((c = getopt (argc, argv, "rgd:t:ab:h")) != -1) {
        switch (c) {
        case 'r': { flag_regDb = true; break;}
        case 'g': { flag_genEp = true; break;}
        case 'a': { flag_allDb = true; break;}
        case 'd': { dbName  = optarg; break;} 
        case 't': { pTables = optarg; break;}
        case 'b': { baseDir = optarg; break;}
        case 'h': { printHelp(); return 0;}
        case '?':
            if (optopt=='r'||optopt=='d'||
                optopt=='t'||optopt== 'b') {
                cerr << "Option -" << optopt << " requires an argument." << endl;
            } else if (isprint (optopt)) {
                cerr << "Unknown option `-" << optopt << "'" << endl;
            } else {
                cerr << "Unknown option character " << optopt << endl;
            }
            return -1;
        default: {
            return -2;
        }
        }
    }
    if ( flag_regDb ) {
        if ( dbName.empty() ) {
            cerr << "database name not specified "
                 << "(must use -d <dbName> with -r option)" << endl;
            return -3;
        } else if ( pTables.empty() ) {
            cerr << "partitioned tables not specified "
                 << "(must use -t <tables> with -r option)" << endl;
            return -4;
        }
        cout << "registering db: " << dbName
             << ", partTables: " << pTables << endl;
        // ...
        return 0;
    } else if ( flag_genEp ) {
        if ( baseDir.empty() ) {
            cerr << "base dir not specified "
                 << "(must use -b <baseDir> with -g option)" << endl;
            return -5;
        }       
        if ( !dbName.empty() ) {
            cout << "generating export paths for database: "
                 << dbName << ", baseDir is: " << baseDir << endl;
            // ...
        } else if ( flag_allDb ) {
            cout << "generating export paths for all databases "
                 << "registered in the qserv metadata, baseDir is: " 
                 << baseDir << endl;
            // ...
        } else {
            cerr << "\nDo you want to generate export paths for one "
                 << "database, or all? (hint: use -d <dbName or -a flag)" 
                 << endl;
            printHelp();
            return -6;
        }
    } else {
        printHelp();
    }        
    
    //for (int index = optind; index < argc; index++) {
    //    std::cout << "Non-option argument " << argv[index] << std::endl;
    //}
    return 0;
}
