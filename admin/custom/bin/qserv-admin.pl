#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use Cwd;

use constant false => 0;
use constant true  => 1;

use constant START => "start";
use constant STOP  => "stop";
use constant STATUS  => "status";

Getopt::Long::config('bundling_override');
my %opts = ();
GetOptions( \%opts,
	"debug",
	"help|h",
	"set",
	"status|s",
	"stripes=i",
	"stop",
	"start",
	"substripes=i",
	"load",
        "mono-node",
	"delete-data",
	"source=s",
	"table=s",
	"stripedir|sdir=s",
	"mysql-proxy-port=s",
	"dbpass=s",
	"partition",
	"test"
);
usage(1) if ($Getopt::Long::error);
usage(0) if ($opts{'help'});

my $debug = $opts{'debug'} || 0;

# WARNING:  %(...) below are template variables !
# It  would  be  better  to  read  config files  instead  to  have  it
# hard-coded here. see https://dev.lsstcorp.org/trac/ticket/2566

my $install_dir = "%(QSERV_BASE_DIR)s";
my $init_dir = "$install_dir/etc/init.d";
my $mysql_proxy_port = "%(MYSQL_PROXY_PORT)s" || $opts{'mysql-proxy-port'} || 4040;
my $cluster_type = $opts{'mono-node'} || "mono-node" ;

print "Using $install_dir install.\n" if( $debug );

#mysql variables
my $mysqld_sock = "$install_dir/var/lib/mysql/mysql.sock";



if( $opts{'status'} ) {
	print "Checking on the status.\n" if( $debug );

        qserv_services(STATUS);

} elsif( $opts{'stop'} ) {

        qserv_services(STOP);

} elsif( $opts{'start'} ) {

        qserv_services(START);

} elsif( $opts{'partition'} ) {

	#need to partition raw data for loading into qserv.
	unless( $opts{'source'} ) {
		print "Error: you need to set the path to the source data with the --source option.\n";
		exit(1);
	}
	unless( $opts{'table'} ) {
		print "Error: you need to specify the table name for the source data with the --table option.\n";
		exit(1);
	}
	unless( $opts{'stripedir'} ) {
		print "Error: you need to specify the stripe dir path with the --stripedir option.\n";
		exit(1);
	}

	partition_data( $opts{'source'}, $opts{'stripedir'}, $opts{'table'} );

} elsif( $opts{'load'} ) {

	#need to partition raw data for loading into qserv.
	unless( $opts{'source'} ) {
		print "Error: you need to set the path to the source data with the --source option.\n";
		exit(1);
	}
	unless( $opts{'table'} ) {
		print "Error: you need to specify the table name for the source data with the --table option.\n";
		exit(1);
	}
	unless( $opts{'stripedir'} ) {
		print "Error: you need to specify the stripe dir path with the --stripedir option.\n";
		exit(1);
	}
	unless( $opts{'dbpass'} ) {
		print "Error: you need to specify the mysql root password with the --dbpass option.\n";
		exit(1);
	}

	#need to load data into qserv

	## TODO performs some checking before starting mysqld (already started)
	## and idem for stopping
	load_data( $opts{'source'}, $opts{'stripedir'}, $opts{'table'}, $opts{'dbpass'} );

} elsif( $opts{'set'} ) {

	unless( $opts{'stripes'} && $opts{'stripes'} =~ /\d+/ ) {
		print "Error: sorry you need to set the stripes you are using with the 'stripes' option\n";
		exit(1);
	}
	unless( $opts{'substripes'} && $opts{'substripes'} =~ /\d+/ ) {
		print "Error: sorry you need to set the substripes you are using with the 'substripes' option\n";
		exit(1);
	}

	set_stripes( $opts{'stripes'}, $opts{'substripes'} );

} elsif( $opts{'delete-data'} ) {

	unless( $opts{'dbpass'} ) {
		print "Error: you need to specify the mysql root password with the --dbpass option.\n";
		exit(1);
	}

	# deleting data from qserv
	delete_data( $opts{'source'}, $opts{'stripedir'}, $opts{'table'}, $opts{'dbpass'} );

}

exit( 0 );

#############################################################

sub initd {
        my ( $action, $prog) = @_;
        my $startup_script = "$init_dir/$prog $action";
	my $ret = system($startup_script);
        if ($ret==0) {
            return false;
        } else {
            return true;
        }
}

sub qserv_services {
        my ( $action ) = @_;
        my @service_list = ('mysqld', 'mysql-proxy', 'xrootd', 'qms', 'qserv-master');
        foreach my $service (@service_list)
        {
            initd($action, $service);
        }

}

sub set_stripes {
	my( $stripes, $substripes ) = @_;

	my $reply = `grep stripes $install_dir/etc/local.qserv.cnf`;

	if( $reply =~ /stripes\s*=\s*\d+/ ) {
		print "Error: sorry, you have already set the stripes to use for the data.\n";
		print "    please edit the local.qserv.cnf and remove the stripes value if you really want to change this.\n";
		return;
	}

	`perl -pi -e 's,^substripes\\s*=,substripes = $substripes,' $install_dir/etc/local.qserv.cnf`;
	`perl -pi -e 's,^stripes\\s*=,stripes = $stripes,' $install_dir/etc/local.qserv.cnf`;

}

#Partition the pt11 example data for use into chunks.  This is the example
#use of partitioning, and this should be more flexible to create different
#amounts of chunks, but works for now.
sub partition_data {
	my( $source_dir, $output_dir, $tablename ) = @_;

	my $stripes;
	my $substripes;

	my $reply = `grep stripes $install_dir/etc/local.qserv.cnf`;
	if( $reply =~ /^stripes\s*=\s*(\d+)/ ) {
		if( $opts{'stripes'} ) {
			unless( $opts{'stripes'} == $1 ) {
				print "Warning: you used $1 for stipes before.  Please check which stripes value you wish to use.\n";
				return;
			}
		}
		$stripes = $1;
	} else {
		if( $opts{'stripes'} ) {
			$stripes = $opts{'stripes'};
		} else {
			print "Error: sorry you need to set the stripes value before partitioning.\n";
			return;
		}
	}

	$reply = `grep substripes $install_dir/etc/local.qserv.cnf`;
	if( $reply =~ /substripes\s*=\s*(\d+)/ ) {
		if( $opts{'substripes'} ) {
			unless( $opts{'substripes'} == $1 ) {
				print "Warning: you used $1 for substipes before.  Please check which substripes value you wish to use.\n";
				return;
			}
		}
		$stripes = $1;
	} else {
		if( $opts{'substripes'} ) {
			$stripes = $opts{'substripes'};
		} else {
			print "Error: sorry you need to set the substripes value before partitioning.\n";
			return;
		}
	}

	my( $dataname ) = $source_dir =~ m!/([^/]+)$!;

	if( -d "$output_dir" ) {
		chdir "$output_dir";
	} else {
		print "Error: the stripe dir $output_dir doesn't exist.\n";
	}

	#need to have the various steps to partition data
	my $command = "$install_dir/bin/python $install_dir/qserv/master/examples/partition.py ".
		"-P$tablename -t 2 -p 4 $source_dir/${tablename}.txt -S $stripes -s $substripes";
	if( $opts{'test'} ) {
		print "$command\n";
	} else {
		run_command("$command");
	}
}

#Drop the loaded database from mysql
sub delete_data {
	my( $source_dir, $location, $tablename, $dbpass ) = @_;

        #delete database
        run_command("$install_dir/bin/mysql -S $install_dir/var/lib/mysql/mysql.sock -u root -p$dbpass -e 'Drop database if exists LSST;'");
}

#load the partitioned pt11 data into qserv for use.  This does a number of
#steps all in one command.
sub load_data {
	my( $source_dir, $location, $tablename, $dbpass ) = @_;

	#check the stripes value
	my $stripes = get_value( 'stripes' );

	unless( $stripes ) {
		print "Error: sorry, the stripes value is unknown, please set it with the 'set' option.\n";
		return;
	}

	#create database if it doesn't exist
	run_command("$install_dir/bin/mysql -S $install_dir/var/lib/mysql/mysql.sock -u root -p$dbpass -e 'Create database if not exists LSST;'");

	#check on the table def, and add need columns
	print "Copy and changing $source_dir/${tablename}.sql\n";
	my $tmptable = lc $tablename;
	`cp $source_dir/${tablename}.sql $install_dir/tmp`;
	`perl -pi -e 's,^.*_chunkId.*\n,,' $install_dir/tmp/${tablename}.sql`;
	`perl -pi -e 's,^.*_subChunkId.*\n,,' $install_dir/tmp/${tablename}.sql`;
	`perl -pi -e 's,chunkId,dummy1,' $install_dir/tmp/${tablename}.sql`;
	`perl -pi -e 's,subChunkId,dummy2,' $install_dir/tmp/${tablename}.sql`;
	`perl -pi -e 's!^\(\\s*PRIMARY\)!  chunkId int\(11\) default NULL,\\n\\1!' $install_dir/tmp/${tablename}.sql`;
	`perl -pi -e 's!^\(\\s*PRIMARY\)!  subChunkId int\(11\) default NULL,\\n\\1!' $install_dir/tmp/${tablename}.sql`;

	if( $tablename eq 'Object' ) {
		`perl -pi -e 's,^.*PRIMARY.*\n,,' $install_dir/tmp/${tablename}.sql`;
		open TMPFILE, ">>$install_dir/tmp/${tablename}.sql";
		print TMPFILE "\nCREATE INDEX obj_objectid_idx on Object ( objectId );\n";
		close TMPFILE;
	}

	#regress through looking for partitioned data, create loading script
	open LOAD, ">$install_dir/tmp/${tablename}_load.sql";

	my %chunkslist = ();
	opendir DIR, "$location";
	my @dirslist = readdir DIR;
	closedir DIR;

	#look for paritioned table chunks, and create the load data sqls.
	foreach my $dir ( sort @dirslist ) {
		next if( $dir =~ /^\./ );

		if( $dir =~ /^stripe/ ) {
			opendir DIR, "$location/$dir";
			my @filelist = readdir DIR;
			closedir DIR;

			foreach my $file ( sort @filelist ) {
				if( $file =~ /(\w+)_(\d+).csv/ ) {
					my $loadname = "${1}_$2";
					if( $1 eq $tablename ) {
					print LOAD "DROP TABLE IF EXISTS $loadname;\n";
					print LOAD "CREATE TABLE IF NOT EXISTS $loadname LIKE $tablename;\n";
					print LOAD "LOAD DATA INFILE '$location/$dir/$file' IGNORE INTO TABLE $loadname FIELDS TERMINATED BY ',';\n";

					$chunkslist{$2} = 1;
					}
					if( $tablename eq 'Object' && $1 =~ /^Object\S+Overlap/ ) {
						print LOAD "CREATE TABLE IF NOT EXISTS $loadname LIKE $tablename;\n";
						print LOAD "LOAD DATA INFILE '$location/$dir/$file' INTO TABLE $loadname FIELDS TERMINATED BY ',';\n";
					}
				}
			}
		}
	}
	print LOAD "CREATE TABLE IF NOT EXISTS ${tablename}_1234567890 LIKE $tablename;\n";
	close LOAD;

	#load the data into the mysql instance
	print "Loading data, this make take awhile...\n";
	run_command("$install_dir/bin/mysql -S $install_dir/var/lib/mysql/mysql.sock -u root -p$dbpass LSST < $install_dir/tmp/${tablename}.sql");
	run_command("$install_dir/bin/mysql -S $install_dir/var/lib/mysql/mysql.sock -u root -p$dbpass LSST < $install_dir/tmp/${tablename}_load.sql");

	#create the empty chunks file
	unless( -e "$install_dir/etc/emptyChunks.txt" ) {
		create_emptychunks( $stripes, \%chunkslist );
	}

	#create a setup file
	unless( -e "$install_dir/etc/setup.cnf" ) {
		open SETUP, ">$install_dir/etc/setup.cnf";
		print SETUP "host:localhost\n";
		print SETUP "port:$mysql_proxy_port\n";
		print SETUP "user:root\n";
		print SETUP "pass:$dbpass\n";
		print SETUP "sock:$install_dir/var/lib/mysql/mysql.sock\n";
		close SETUP;
	}

	#check if database is already registered, if it is it needs to get unreg. first.
	#register the database, export
	run_command("$install_dir/bin/fixExportDir.sh");

}

#Create the empty chucks list up to 1000, and print this into the
#empty chunks file in etc.
sub create_emptychunks {
	my( $stripes, $chunkslist ) = @_;

	my $top_chunk = 2 * $stripes * $stripes;

	open CHUNKS, ">$install_dir/etc/emptyChunks.txt";
	for( my $i = 0; $i < $top_chunk; $i++ ) {
		unless( defined $chunkslist->{$i} ) {
			print CHUNKS "$i\n";
		}
	}
	close CHUNKS;

}

#routine to get the value from the config file
sub get_value {
	my( $text ) = @_;

	open CNF, "<$install_dir/etc/local.qserv.cnf";
	while( my $line = <CNF> ) {
		if( $line =~ /^$text\s*=\s*(\d+)/ ) {
			close CNF;
			return $1;
		}
	}
	close CNF;

}

#help report for the --help option
sub usage {
  my($exit, $message) = @_;

        my($bin) = ($0 =~ m!([^/]+)$!);
        print STDERR $message if defined $message;
        print STDERR <<INLINE_LITERAL_TEXT;
usage: $bin [options]
  Help admin the qserv server install, starting, stopping, and checking of status
  Also supports the loading of pt11 example data into the server for use.

Options are:
      --debug         Print out debug messages.
  -h, --help          Print out this help message.
      --set           Set the stripes and substripes values, needs strips and substripes options.
  -s, --status        Print out the status of the server processes.
      --stop          Stop the servers.
	  --start         Start the servers.
	  --stripes       Number of stripes used to partition the data.
	  --substripes     Number of substripes used to partition the data.
	  --load          Load data into qserv, requires options source, stripedir, table
      --delete-data   Load data into qserv, requires options source, output, table
	  --source        Path to the pt11 exmple data
 --sdir, --stripedir  Path to the paritioned data
      --mysql-proxy-port  Port number to use for the mysql proxy.
	  --table         Table name for partitioning and loading
	  --parition      Partition the example pt11 data into chunks, requires source, stripedir, table
	  --test          test the use of the util, without performing the actions.

Examples: $bin --status

Comments to Douglas Smith <douglas\@slac.stanford.edu>.
INLINE_LITERAL_TEXT

	       exit($exit) if defined $exit;

}

sub run_command {
	my( $command ) = @_;
	my $cwd = cwd();
	my @return;
	print "-- Running: $command in $cwd\n";
	open( OUT, "$command |" ) || die "ERROR : can't fork $command : $!";
	while( <OUT> ) {
		print STDOUT $_;
		push( @return, $_ );
	}
	close( OUT ) || die "ERROR : $command exits with error code ($?)";
	return @return;
}
