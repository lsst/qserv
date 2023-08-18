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
    'qserv/StatusActiveQueries',
    'qserv/StatusActiveChunksMap',
    'qserv/StatusPastQueries',
    'qserv/StatusQueryInspector',
    'qserv/StatusReplicationLevel',
    'qserv/StatusWorkers',
    'qserv/QservCzarMySQLQueries',
    'qserv/QservCzarStatistics',
    'qserv/QservCss',
    'qserv/QservMySQLConnections',
    'qserv/QservWorkerMySQLQueries',
    'qserv/QservWorkerQueries',
    'qserv/QservWorkerSchedulers',
    'qserv/QservWorkerSchedulerHist',
    'qserv/QservWorkerTasks',
    'qserv/QservWorkerTaskHist',
    'qserv/QservWorkerResultsFilesystem',
    'qserv/ReplicationController',
    'qserv/ReplicationTools',
    'qserv/ReplicationConfigGeneral',
    'qserv/ReplicationConfigWorkers',
    'qserv/ReplicationConfigCatalogs',
    'qserv/ReplicationSchema',
    'qserv/ReplicationTableIndexes',
    'qserv/IngestStatus',
    'qserv/IngestConfiguration',
    'qserv/IngestTransactions',
    'qserv/IngestTransactionsLog',
    'qserv/IngestContributions',
    'qserv/IngestContribInfo',
    'qserv/ToolsSql',

    // Make sure the core libraries are preloaded so that the applications
    // won't bother with loading them individually

    'bootstrap',
    'underscore'],

function(CSSLoader,
         Fwk,
         FwkTestApp,
         StatusCatalogs,
         StatusActiveQueries,
         StatusActiveChunksMap,
         StatusPastQueries,
         StatusQueryInspector,
         StatusReplicationLevel,
         StatusWorkers,
         QservCzarMySQLQueries,
         QservCzarStatistics,
         QservCss,
         QservMySQLConnections,
         QservWorkerMySQLQueries,
         QservWorkerQueries,
         QservWorkerSchedulers,
         QservWorkerSchedulerHist,
         QservWorkerTasks,
         QservWorkerTaskHist,
         QservWorkerResultsFilesystem,
         ReplicationController,
         ReplicationTools,
         ReplicationConfigGeneral,
         ReplicationConfigWorkers,
         ReplicationConfigCatalogs,
         ReplicationSchema,
         ReplicationTableIndexes,
         IngestStatus,
         IngestConfiguration,
         IngestTransactions,
         IngestTransactionsLog,
         IngestContributions,
         IngestContribInfo,
         ToolsSql) {

    CSSLoader.load('https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.css');
    CSSLoader.load('https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css');
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
                    new StatusActiveQueries('Active Queries Monitor'),
                    new StatusActiveChunksMap('Active Chunks Map'),
                    new StatusPastQueries('Past Queries'),
                    new StatusQueryInspector('Query Inspector'),
                    new StatusCatalogs('Catalogs'),
                    new StatusReplicationLevel('Replication Level'),
                    new StatusWorkers('Workers')
                ]
            },
            {   name: 'Czar',
                apps: [
                    new QservCzarMySQLQueries('MySQL Queries'),
                    new QservCzarStatistics('Statistics'),
                    new QservCss('CSS')
                ]
            },
            {   name: 'Workers',
                apps: [
                    new QservMySQLConnections('MySQL Connections'),
                    new QservWorkerMySQLQueries('MySQL Queries'),
                    new QservWorkerQueries('Queries in Worker Queues'),
                    new QservWorkerSchedulers('Schedulers'),
                    new QservWorkerSchedulerHist('Scheduler Histograms'),
                    new QservWorkerTasks('Tasks'),
                    new QservWorkerTaskHist('Task Histograms'),
                    new QservWorkerResultsFilesystem('Results Filesystem')
                ]
            },
            {   name: 'Replication',
                apps: [
                    new ReplicationController('Controller'),
                    new ReplicationConfigGeneral('Config/General'),
                    new ReplicationConfigWorkers('Config/Workers'),
                    new ReplicationConfigCatalogs('Config/Catalogs'),
                    new ReplicationSchema('Schema'),
                    new ReplicationTableIndexes('Table Indexes'),
                    new ReplicationTools('Tools')
                ]
            },
            {   name: 'Ingest',
                apps: [
                    new IngestStatus('Status'),
                    new IngestConfiguration('Configuration'),
                    new IngestTransactions('Transactions'),
                    new IngestTransactionsLog('Transaction Events Log'),
                    new IngestContributions('Contributions'),
                    new IngestContribInfo('Contribution Info')
                ]
            },
            {   name: 'Tools',
                apps: [
                    new FwkTestApp('Query Qserv'),
                    new ToolsSql('Query Worker Databases')
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
                    Fwk.show('Status', 'Active Queries Monitor');
                }
            }
        );
    });
});
