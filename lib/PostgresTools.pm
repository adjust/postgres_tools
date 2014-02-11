package PostgresTools;

use strict;
use warnings;
use 5.012;

use Moo;
use Parallel::ForkManager;
use File::Path qw(make_path);
use DateTime::Format::Strptime;

use PostgresTools::Database;
use PostgresTools::Date;

has user     => ( is => 'rw' );
has host     => ( is => 'rw' );
has db       => ( is => 'ro', required => 1 );
has dbh      => ( is => 'rw' );
has base_dir => ( is => 'rw' );
has dump_dir => ( is => 'rw' );
has forks    => ( is => 'rw' );
has offset   => ( is => 'rw' );
has exclude  => ( is => 'rw' );
has excludes => ( is => 'rw' );
has pretend  => ( is => 'rw' );

sub BUILD {
    my $self = shift;
    $self->user('postgres')  unless $self->user;
    $self->host('localhost') unless $self->host;
    my $dbh = PostgresTools::Database->new(
        db   => $self->{db},
        host => $self->{host},
        user => $self->{user},
    );
    $self->base_dir('./base') unless $self->base_dir;
    $self->_make_base;
    $self->dbh($dbh);
    $self->forks(1)   unless $self->forks;
    $self->offset(1)  unless $self->offset;
    $self->pretend(0) unless $self->pretend;
    $self->_create_excludes;
}

sub dump {
    my $self = shift;
    $self->_dump_partitions;
    $self->_dump_tables;
    $self->_dump_sequences;
}

sub _dump_partitions {
    my $self  = shift;
    my $parts = $self->dbh->partitions;
    my $date  = PostgresTools::Date->new;
    my $pm    = new Parallel::ForkManager( $self->forks );
    for my $part (@$parts) {
        next if $self->excludes->{$part};
        $pm->start and next;
        if ( $date->older_than_from_string( $part, $self->offset ) ) {
            $self->_make_dump($part);
        }
        $pm->finish;
    }
    $pm->wait_all_children;
}

sub _dump_tables {
    my $self   = shift;
    my $tables = $self->dbh->tables;
    my $pm     = new Parallel::ForkManager( $self->forks );
    for my $table (@$tables) {
        next if $self->excludes->{$table};
        $pm->start and next;
        $self->_make_dump($table);
        $pm->finish;
    }
    $pm->wait_all_children;
}

sub _dump_sequences {
    my $self = shift;
    my $seqs = $self->dbh->sequences;
    my $pm   = new Parallel::ForkManager( $self->forks );
    for my $seq (@$seqs) {
        next if $self->excludes->{$seq};
        $pm->start and next;
        $self->_make_dump($seq);
        $pm->finish;
    }
    $pm->wait_all_children;
}

sub _create_excludes {
    my $self = shift;
    $self->exclude( [] ) unless $self->exclude;
    my %excludes = map { $_ => 1 } @{ $self->exclude };
    $self->excludes( \%excludes );
    use Data::Dumper;
    print Dumper $self->excludes;
}

sub _make_base {
    my $self      = shift;
    my $formatter = DateTime::Format::Strptime->new( pattern => '%Y_%m_%d' );
    my $now       = DateTime->now( formatter => $formatter );
    $self->dump_dir( $self->{base_dir} . "/$now" );
    make_path( $self->{dump_dir} );
}

sub _make_dump {
    my $self    = shift;
    my $to_dump = shift;
    my $cmd = "pg_dump -U $self->{user} -h $self->{host} -c -F c -f $self->{dump_dir}/$to_dump $self->{db}";
    say $cmd;
    if ( !$self->pretend ) {
        eval {
            system($cmd) == 0 or die $!;
        };
    }
}

1;
