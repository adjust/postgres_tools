package PostgresTools;

use strict;
use warnings;
use 5.012;

use Moo;
use Parallel::ForkManager;
use File::Path qw(make_path);
use DateTime::Format::Strptime;
use Term::ProgressBar;
use autodie;

use PostgresTools::Database;
use PostgresTools::Date;

has user     => ( is => 'rw' );
has user2    => ( is => 'rw' );
has host     => ( is => 'rw' );
has host2    => ( is => 'rw' );
has db       => ( is => 'ro', required => 1 );
has db2      => ( is => 'ro', required => 1 );
has dbh      => ( is => 'rw' );
has dbh2     => ( is => 'rw' );
has date     => ( is => 'rw' );
has base_dir => ( is => 'rw' );
has dump_dir => ( is => 'rw' );
has forks    => ( is => 'rw' );
has offset   => ( is => 'rw' );
has exclude  => ( is => 'rw' );
has excludes => ( is => 'rw' );
has pretend  => ( is => 'rw' );
has verbose  => ( is => 'rw' );
has progress => ( is => 'rw' );
has iter     => ( is => 'rw' );
has bar      => ( is => 'rw' );
has count    => ( is => 'rw' );
has restore  => ( is => 'rw' );
has schema   => ( is => 'rw' );

sub BUILD {
    my $self = shift;
    $self->user('postgres')  unless $self->user;
    $self->host('localhost') unless $self->host;
    my $dbh = PostgresTools::Database->new(
        db   => $self->{db},
        host => $self->{host},
        user => $self->{user},
    );
    $self->dbh($dbh);
    $self->base_dir('./base') unless $self->base_dir;
    $self->_set_dump_dir;
    $self->forks(1)               unless $self->forks;
    $self->offset(0)              unless $self->offset;
    $self->pretend(0)             unless $self->pretend;
    $self->restore( $self->{db} ) unless $self->restore;
    $self->_create_excludes;
}

sub dump93 {
    my $self = shift;
    $self->_make_base;
    my $items = [];
    push( @$items, @{ $self->_get_new_partitions } );
    push( @$items, @{ $self->dbh->tables } );
    push( @$items, @{ $self->dbh->sequences } );
    my $cmd = "pg_dump";
    $cmd .= " -U $self->{user}";
    $cmd .= " -h $self->{host}";
    $cmd .= " -c -F d";
    $cmd .= " -f $self->{dump_dir}/$self->{db} $self->{db}";
    $cmd .= " -v " if $self->verbose;
    $cmd .= " -j $self->{forks}";

    for (@$items) {
        $cmd .= " -t $_ ";
    }
    if ( $self->{pretend} ) {
        say $cmd;
        exit(0);
    }
    system($cmd ) == 0 or die $!;
}

sub dump {
    my $self = shift;
    $self->_make_base;
    $self->_dump_schema;
    $self->_dump_partitions;
    $self->_dump_tables;
    $self->_dump_sequences;
}

sub restore93 {
    my $self = shift;
    my $cmd  = "pg_restore";
    $cmd .= " -c -d $self->{db}";
    $cmd .= " -h $self->{host}";
    $cmd .= " -U $self->{user}";
    $cmd .= " -j $self->{forks} ";
    $cmd .= " -v " if $self->verbose;
    $cmd .= "$self->{dump_dir}/$self->{restore}";
    if ( $self->{pretend} ) {
        say $cmd;
        exit(0);
    }
    system($cmd ) == 0 or die $cmd . " " . $!;
}

sub restore_dump {
    my $self = shift;
    $self->_load_schema if $self->schema;
    my $cmd = sprintf(
        "pg_restore -c -d %s -U %s ",
        $self->{db},
        $self->{user},
    );
    $cmd .= " -v " if $self->verbose;
    my @to_restore = glob "$self->{dump_dir}/$self->{restore}/*";
    say "restoring items..." if $self->progress;
    $self->_setup_progress( scalar @to_restore );
    my $pm = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );

    for my $item (@to_restore) {
        say $item;
        next if $self->excludes->{$item};
        $pm->start and next;
        say $cmd . $item if $self->verbose;
        if ( !$self->pretend ) {
            eval {
                system( $cmd. $item ) == 0 or die $!;
            };
        }
        $pm->finish;
    }
    $pm->wait_all_children;
    $self->_finish_progress;
}

sub diff {
    my $self = shift;
    my $dbh2 = PostgresTools::Database->new(
        db   => $self->{db2},
        host => $self->{host2},
        user => $self->{user2},
    );
    $self->dbh2($dbh2);
    my $items = [];
    push( @$items, @{ $self->_get_old_partitions } );
    push( @$items, @{ $self->dbh->tables } );
    for my $item (@$items) {
        my $val1 = $self->dbh->count($item);
        my $val2 = $self->dbh2->count($item);
        say "table $item differs count1: $val1 count2 $val2" if $val1 != $val2;
    }
}

sub _setup_progress {
    my $self = shift;
    return unless $self->progress;
    my $count = shift;
    $self->iter(0);
    $self->count($count);
    $self->bar( Term::ProgressBar->new( { count => $count } ) );
}

sub _update_progress {
    my $self = shift;
    return unless $self->progress;
    my $iter = $self->iter;
    $self->bar->update( $iter++ );
    $self->iter($iter);
}

sub _finish_progress {
    my $self = shift;
    return unless $self->progress;
    my $max = $self->count;
    $self->bar->update($max);
}

sub _get_new_partitions {
    my $self     = shift;
    my $parts    = $self->dbh->partitions;
    my $date     = PostgresTools::Date->new;
    my $filtered = [];
    for my $part (@$parts) {
        push( @$filtered, $part ) and next if $self->offset == 0;
        if ( $date->newer_than_from_string( $part, $self->offset ) ) {
            push( @$filtered, $part );
        }
    }
    return $filtered;
}

sub _get_old_partitions {
    my $self     = shift;
    my $parts    = $self->dbh->partitions;
    my $date     = PostgresTools::Date->new;
    my $filtered = [];
    for my $part (@$parts) {
        push( @$filtered, $part ) and next if $self->offset == 0;
        if ( $date->older_than_from_string( $part, $self->offset ) ) {
            push( @$filtered, $part );
        }
    }
    return $filtered;
}

sub _dump_schema {
    my $self = shift;
    my $cmd  = sprintf(
        "pg_dump -U %s -s -f %s %s",
        $self->{user},
        "$self->{dump_dir}/schema/schema.sql",
        $self->{db},
    );
    system($cmd) == 0 or die $!;
}

sub _load_schema {
    my $self = shift;
    my $cmd  = sprintf(
        "psql -U %s -f %s %s",
        $self->{user},
        "$self->{dump_dir}/schema/schema.sql",
        $self->{db},
    );
    system($cmd) == 0 or die $!;
}

sub _dump_partitions {
    my $self  = shift;
    my $parts = $self->_get_new_partitions;
    say "dumping partitions..." if $self->progress;
    $self->_dump_items($parts);
}

sub _dump_tables {
    my $self   = shift;
    my $tables = $self->dbh->tables;
    say "dumping tables..." if $self->progress;
    $self->_dump_items($tables);
}

sub _dump_sequences {
    my $self = shift;
    my $seqs = $self->dbh->sequences;
    say "dumping sequences..." if $self->progress;
    $self->_dump_items($seqs);
}

sub _dump_items {
    my $self  = shift;
    my $items = shift;
    $self->_setup_progress( scalar @{$items} );
    my $pm = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );
    for my $item (@$items) {
        next if $self->excludes->{$item};
        $pm->start and next;
        $self->_make_dump($item);
        $pm->finish;
    }
    $pm->wait_all_children;
    $self->_finish_progress;
}

sub _create_excludes {
    my $self = shift;
    $self->exclude( [] ) unless $self->exclude;
    my %excludes = map { $_ => 1 } @{ $self->exclude };
    $self->excludes( \%excludes );
}

sub _set_date {
    my $self = shift;
    my $formatter = DateTime::Format::Strptime->new( pattern => '%Y%m%d' );
    $self->date( DateTime->now( formatter => $formatter ) );
    $self->dump_dir( $self->{base_dir} . "/$self->{date}" );
}

sub _set_dump_dir {
    my $self = shift;
    $self->_set_date unless $self->date;
    $self->dump_dir( $self->{base_dir} . "/$self->{date}" );
}

sub _make_base {
    my $self = shift;
    make_path( $self->{dump_dir} );
    make_path( $self->{dump_dir} . "/schema" );
}

sub _make_dump {
    my $self    = shift;
    my $to_dump = shift;
    make_path("$self->{dump_dir}/$self->{db}");
    my $cmd = sprintf(
        "pg_dump -U %s -h %s -c -F c -t %s -f %s %s",
        $self->{user},
        $self->{host},
        $to_dump,
        "$self->{dump_dir}/$self->{db}/$to_dump",
        $self->{db},
    );
    $cmd .= " -v " if $self->verbose;
    say $cmd unless $self->progress;
    if ( !$self->pretend ) {
        eval {
            system($cmd) == 0 or die $!;
        };
    }
}

1;
