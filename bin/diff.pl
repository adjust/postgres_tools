#!/usr/bin/perl

use strict;
use warnings;
use 5.012;

use lib 'lib';

use PostgresTools;
use File::Basename;
use Getopt::Long;

our $PROGNAME = basename($0);

my $host1 = 'localhost';
my $host2 = 'localhost';
my $user1 = 'postgres';
my $user2 = 'postgres';
my $db1;
my $db2;
my $offset = 35;
my $verbose;

GetOptions(
    "host1|h1=s" => \$host1,
    "host2|h1=s" => \$host2,
    "user1|U1=s" => \$user1,
    "user2|U2=s" => \$user2,
    "db1=s"      => \$db1,
    "db2=s"      => \$db2,
    "offset|o=i" => \$offset,
    "verbose|v"  => \$verbose,
);

#TODO: extend help text
unless ( defined($db1) && defined($db2) ) {
    say "need db1 and db2 parameter";
    exit(1);
}

my $tools = PostgresTools->new(
    offset  => $offset,
    host    => $host1,
    host2   => $host2,
    user    => $user1,
    user2   => $user2,
    db      => $db1,
    db2     => $db2,
    verbose => $verbose,
);

$tools->diff;
