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
my $offset = 30;

GetOptions(
    "host|h=s"   => \$host,
    "user|U=s"   => \$user,
    "db=s"       => \$db,
    "offset|o=i" => \$offset,
);

unless ( defined($db) ) {
    say "need db name";
    exit(1);
}

my $tools = PostgresTools->new(
    host   => $host,
    user   => $user,
    db     => $db,
    offset => $offset,
);

$tools->analyze;
