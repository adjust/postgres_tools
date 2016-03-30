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
my $base_dir = './base';
my $dst;
my $pretend  = 0;
my $verbose  = 0;
my $progress = 0;
my $jobs     = 1;
my $offset   = 0;
my $rsync    = 0;
my $port     = 5432;
my $exclude_partitions;
my $exclude_tables;
my $excludes;
my $ignore_offset;
my $textdump;

GetOptions(
    "host|h=s"           => \$host,
    "user|U=s"           => \$user,
    "db=s"               => \$db,
    "base_dir=s"         => \$base_dir,
    "dst=s"              => \$dst,
    "jobs|j=i"           => \$jobs,
    "offset|o=i"         => \$offset,
    "pretend|p"          => \$pretend,
    "verbose|v"          => \$verbose,
    "progress"           => \$progress,
    "rsync"              => \$rsync,
    "exclude_tables"     => \$exclude_tables,
    "exclude_partitions" => \$exclude_partitions,
    "excludes=s"         => \$excludes,
    "ignore_offset=s"    => \$ignore_offset,
    "port=i"             => \$port,
    "textdump"           => \$textdump,
);

unless ( defined($db) ) {
    say "usage: $PROGNAME --host <host> --user <user> --db <db> -p\n";
    say "\thost|h => PostgreSQL host to connect to ( default: \'localhost\' )";
    say
      "\tuser|U => PostgreSQL user to use for connection ( default: \'postgres\' )";
    say "\tdb     => PostgreSQL database to connect to ( required )";
    say "\tpretend|p => boolean, if set only print commands";
    exit(1);
}

if ( $rsync && !defined($dst) ) {
    say "you need to define <dst> if rsync is used";
    exit(1);
}

my @excludes_array = split ',', $excludes if $excludes;

my $tools = PostgresTools->new(
    host               => $host,
    user               => $user,
    db                 => $db,
    base_dir           => $base_dir,
    pretend            => $pretend,
    verbose            => $verbose,
    offset             => $offset,
    forks              => $jobs,
    progress           => $progress,
    rsync              => $rsync,
    dst                => $dst,
    exclude_partitions => $exclude_partitions,
    exclude_tables     => $exclude_tables,
    exclude            => \@excludes_array,
    ignore_offset      => $ignore_offset,
    port               => $port,
    textdump           => $textdump,
);

$tools->dump;
