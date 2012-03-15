
#include <iostream>

void printHelp() {
    std::cout
        << "\nUsage:\n"
        << "  -r          - register database in the qserv metadata\n"
        << "  -d <dbName> - database name\n"
        << "  -t <tables> - comma-separated list of partitioned tables\n"
        << "  -h          - prints help and exits\n"
        << "\nExamples:\n"
        << "   registerDb -r -d rplante_PT1_2_u_pt12prod_im3000_qserv -t 'Object,Source,ForcedSource'\n"
        << std::endl;
}

int
main (int argc, char **argv) {
    std::string dbName;
    std::string pTables;
    
    bool flag_regDb = false;

    int c;
    opterr = 0;
    while ((c = getopt (argc, argv, "rd:mt:h")) != -1) {
        switch (c) {
        case 'r': {
            flag_regDb = true;
            break;
        } 
        case 'd': {
            dbName = optarg;
            break;
        } 
        case 't': {
            pTables = optarg;
            break;
        }
        case 'h': {
            printHelp();
            return 0;
        }
        case '?':
            if (optopt == 'r' || optopt == 't') {
                std::cerr << "Option -" << optopt << " requires an argument."
                          << std::endl;
            } else if (isprint (optopt)) {
                std::cerr << "Unknown option `-" << optopt << "'" << std::endl;
            } else {
                std::cerr << "Unknown option character " << optopt << std::endl;
            }
            return 1;
        default: {
            return -1;
        }
        }
    }
    if ( flag_regDb ) {
        if ( dbName.empty() ) {
            std::cerr << "database name not specified "
                      << "(must use -d <dbName> with -r option)" << std::endl;
            return -2;
        }
        if ( pTables.empty() ) {
            std::cerr << "partitioned tables not specified "
                      << "(must use -t <tables> with -r option)" << std::endl;
            return -3;
        }
    }
    if ( flag_regDb ) {
        std::cout << "registering db: " << dbName
                  << ", partTables: " << pTables
                  << std::endl;
    } else {
        printHelp();
    }        
    
    //for (int index = optind; index < argc; index++) {
    //    std::cout << "Non-option argument " << argv[index] << std::endl;
    //}
    return 0;
}
