require.config({

    baseUrl: '..',

    waitSeconds: 15,
    urlArgs:     "bust="+new Date().getTime(),

    paths: {
        'jquery':     'https://code.jquery.com/jquery-3.3.1',
        'bootstrap':  'https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/js/bootstrap.bundle',
        'underscore': 'https://underscorejs.org/underscore-umd-min',
        'webfwk':     'webfwk/js',
        'qserv':      'qserv/js',
        'modules':    'modules/js'
    },
    shim: {
        'jquery':  {
            'deps': ['underscore']
        },
        'bootstrap':  {
            'deps': ['jquery','underscore']
        },/*
        'webfwk/*': {
            'deps': ['underscore']
        },
        'qserv/*': {
            'deps': ['underscore']
        },*/
        'underscore': {
            'exports': '_'
        }
    }
});
require([
    'webfwk/CSSLoader',
    'webfwk/Fwk',
    'webfwk/FwkTestApp',
    'qserv/StatusCatalogs',
    'qserv/StatusActiveChunksMap',
    'qserv/StatusReplicationLevel',
    'qserv/StatusWorkers',
    'qserv/StatusUserQueries',
    'qserv/QservWorkerSchedulers',
    'qserv/QservWorkerQueries',
    'qserv/ReplicationController',
    'qserv/ReplicationTools',
    'qserv/ReplicationConfigGeneral',
    'qserv/ReplicationConfigWorkers',
    'qserv/ReplicationConfigCatalogs',
    'qserv/ReplicationSchema',
    'qserv/IngestStatus',
    'qserv/IngestTransactions',
    'qserv/IngestContributions',
    'qserv/ToolsSql',

    // Make sure the core libraries are preloaded so that the applications
    // won't bother with loading them individually

    'bootstrap',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkTestApp,
         StatusCatalogs,
         StatusActiveChunksMap,
         StatusReplicationLevel,
         StatusWorkers,
         StatusUserQueries,
         QservWorkerSchedulers,
         QservWorkerQueries,
         ReplicationController,
         ReplicationTools,
         ReplicationConfigGeneral,
         ReplicationConfigWorkers,
         ReplicationConfigCatalogs,
         ReplicationSchema,
         IngestStatus,
         IngestTransactions,
         IngestContributions,
         ToolsSql) {

    CSSLoader.load('https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.css');
    CSSLoader.load('https://cdn.jsdelivr.net/npm/bootstrap-icons@1.5.0/font/bootstrap-icons.css');
    CSSLoader.load('qserv/css/QservPDAC.css');

    $(function() {

        function parseURLParameters() {

            let queryString = window.location.search;
            if (typeof queryString !== 'undefined' && queryString && queryString.length > 2) {
                let queries = queryString.substring(1).split("&");
                for (let i=0; i < queries.length; i++) {
                    let keyVal = queries[i].split('=');
                    if (keyVal.length === 2) {
                        let key = keyVal[0];
                        let val = decodeURIComponent(keyVal[1]);
                        if (key === 'page' && val.length > 2) {
                            let menus = val.split(':');
                            if (menus.length === 2) {
                                console.log("menus: ", menus);
                                return menus;
                            }
                        }
                    }
                }
            }
        }
        var apps = [
            {   name: 'Status',
                apps: [
                    new StatusCatalogs('Catalogs'),
                    new StatusReplicationLevel('Replication Level'),
                    new StatusWorkers('Workers'),
                    new StatusUserQueries('User Queries Monitor'),
                    new StatusActiveChunksMap('Active Chunks Map')
                ]
            },
            {   name: 'Replication',
                apps: [
                    new ReplicationController('Controller'),
                    new ReplicationTools('Tools'),
                    new ReplicationConfigGeneral('Config/General'),
                    new ReplicationConfigWorkers('Config/Workers'),
                    new ReplicationConfigCatalogs('Config/Catalogs'),
                    new ReplicationSchema('Schema')
                ]
            },
            {   name: 'Ingest',
                apps: [
                    new IngestStatus('Status'),
                    new IngestTransactions('Transactions'),
                    new IngestContributions('Contributions')
                ]
            },
            {   name: 'Tools',
                apps: [
                    new FwkTestApp('Query Qserv'),
                    new ToolsSql('Query Worker Databases')
                ]
            },
            {   name: 'Qserv Monitor',
                apps: [
                    new QservWorkerSchedulers('Schedulers'),
                    new QservWorkerQueries('Queries in Worker Queues')
                ]
            }
        ];
        Fwk.build(
            'Qserv',
            apps,
            function() {
                let menus = parseURLParameters();
                if (typeof menus !== 'undefined') {
                    Fwk.show(menus[0], menus[1]);
                } else {
                    Fwk.show('Status', 'User Queries Monitor');
                }
            }
        );
    });
});
