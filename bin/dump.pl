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
my $pretend  = 0;
my $verbose  = 0;
my $progress = 0;
my $jobs     = 1;
my $offset   = 35;

GetOptions(
    "host|h=s"   => \$host,
    "user|U=s"   => \$user,
    "db=s"       => \$db,
    "jobs|j=i"   => \$jobs,
    "offset|o=i" => \$offset,
    "pretend|p"  => \$pretend,
    "verbose|v"  => \$verbose,
    "progress"   => \$progress,
);

unless ( defined($db) ) {
    say "usage: $PROGNAME --host <host> --user <user> --db <db> -p\n";
    say "\thost|h => PostgreSQL host to connect to ( default: \'localhost\' )";
    say "\tuser|U => PostgreSQL user to use for connection ( default: \'postgres\' )";
    say "\tdb     => PostgreSQL database to connect to ( required )";
    say "\tpretend|p => boolean, if set only print commands";
    exit(1);
}

my $tools = PostgresTools->new(
    host     => $host,
    user     => $user,
    db       => $db,
    pretend  => $pretend,
    verbose  => $verbose,
    offset   => $offset,
    forks    => $jobs,
    progress => $progress,
);

$tools->dump;
