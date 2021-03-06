#!/usr/bin/perl

use strict;
use warnings;
use 5.012;

use lib 'lib';

use PostgresTools;
use File::Basename;
use Getopt::Long;

our $PROGNAME = basename($0);

my $host = 'localhost';
my $user = 'postgres';
my $db;
my $pretend = 0;
my $verbose = 0;
my $progress;
my $jobs = 1;
my $date_string;
my $restore;
my $schema;
my $date;

GetOptions(
    "host|h=s"  => \$host,
    "user|U=s"  => \$user,
    "db=s"      => \$db,
    "jobs|j=i"  => \$jobs,
    "pretend|p" => \$pretend,
    "verbose|v" => \$verbose,
    "date=s"    => \$date_string,
    "progress"  => \$progress,
    "schema|s"  => \$schema,
    "restore=s" => \$restore,
    "date=s"    => \$date,
);

unless ( defined($db) ) {
    say "usage: $PROGNAME --host <host> --user <user> --db <db> -p --restore <target>\n";
    say "\thost|h => PostgreSQL host to connect to ( default: \'localhost\' )";
    say "\tuser|U => PostgreSQL user to use for connection ( default: \'postgres\' )";
    say "\tdb     => PostgreSQL database to connect to ( required )";
    say "\tpretend|p => boolean, if set only print commands";
    say "\trestore => define which db to restore ( default: value set in db )";
    exit(1);
}

my $tools = PostgresTools->new(
    host     => $host,
    user     => $user,
    db       => $db,
    pretend  => $pretend,
    verbose  => $verbose,
    forks    => $jobs,
    progress => $progress,
    restore  => $restore,
    schema   => $schema,
    date     => $date,
);

$tools->restore_dump;
