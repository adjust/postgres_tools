package PostgresTools;

use strict;
use warnings;
use 5.012;

use Moo;
use Parallel::ForkManager;
use File::Path qw(make_path);
use DateTime::Format::Strptime;
use Term::ProgressBar;

use PostgresTools::Database;
use PostgresTools::Date;

has user     => ( is => 'rw' );
has host     => ( is => 'rw' );
has db       => ( is => 'ro', required => 1 );
has dbh      => ( is => 'rw' );
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
    $self->_set_date unless $self->date;
    $self->dbh($dbh);
    $self->forks(1)   unless $self->forks;
    $self->offset(1)  unless $self->offset;
    $self->pretend(0) unless $self->pretend;
    $self->_create_excludes;
}

sub dump93 {
    my $self = shift;
    $self->_make_base;
    my $items = [];
    push( @$items, @{ $self->dbh->partitions } );
    push( @$items, @{ $self->dbh->tables } );
    push( @$items, @{ $self->dbh->sequences } );
    my $cmd = "pg_dump";
    $cmd .= " -U $self->{user}";
    $cmd .= " -h $self->{host}";
    $cmd .= " -c -F c";
    $cmd .= " -f $self->{dump_dir}/$self->{db} $self->{db}";
    $cmd .= " -v " if $self->verbose;
    $cmd .= " -j $self->{forks}";

    for (@$items) {
        $cmd .= " -t $_ ";
    }
    system($cmd ) == 0 or die $!;
}

sub dump {
    my $self = shift;
    $self->_make_base;
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
    $cmd .= "$self->{dump_dir}/$self->{db}";
    system($cmd ) == 0 or die $cmd . " " . $!;
}

sub restore {
    my $self = shift;
    my $cmd  = "pg_restore -c -d $self->{db} -U $self->{user} ";
    $cmd .= " -v " if $self->verbose;
    my @to_restore = glob "$self->{dump_dir}/*";
    say "restoring items..." if $self->progress;
    $self->_setup_progress( scalar @to_restore );
    my $pm = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );

    for my $item (@to_restore) {
        say $item;
        next if $self->excludes->{$item};
        $pm->start and next;
        say $cmd . $item unless $self->progress;
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

sub _dump_partitions {
    my $self  = shift;
    my $parts = $self->dbh->partitions;
    say "dumping partitions..." if $self->progress;
    $self->_setup_progress( scalar @{$parts} );
    my $date = PostgresTools::Date->new;
    my $pm   = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );
    for my $part (@$parts) {
        next if $self->excludes->{$part};
        $pm->start and next;
        if ( $date->older_than_from_string( $part, $self->offset ) ) {
            $self->_make_dump($part);
        }
        $pm->finish;
    }
    $pm->wait_all_children;
    $self->_finish_progress;
}

sub _dump_tables {
    my $self   = shift;
    my $tables = $self->dbh->tables;
    say "dumping tables..." if $self->progress;
    $self->_setup_progress( scalar @{$tables} );
    my $pm = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );
    for my $table (@$tables) {
        next if $self->excludes->{$table};
        $pm->start and next;
        $self->_make_dump($table);
        $pm->finish;
    }
    $pm->wait_all_children;
    $self->_finish_progress;
}

sub _dump_sequences {
    my $self = shift;
    my $seqs = $self->dbh->sequences;
    say "dumping sequences..." if $self->progress;
    $self->_setup_progress( scalar @{$seqs} );
    my $pm = new Parallel::ForkManager( $self->forks );
    $pm->run_on_finish( sub { $self->_update_progress } );
    for my $seq (@$seqs) {
        next if $self->excludes->{$seq};
        $pm->start and next;
        $self->_make_dump($seq);
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

sub _make_base {
    my $self = shift;
    make_path( $self->{dump_dir} );
}

sub _make_dump {
    my $self    = shift;
    my $to_dump = shift;
    my $cmd = "pg_dump -U $self->{user} -h $self->{host} -c -F c -t $to_dump -f $self->{dump_dir}/$to_dump $self->{db}";
    $cmd .= $cmd . " -v " if $self->verbose;
    say $cmd unless $self->progress;
    if ( !$self->pretend ) {
        eval {
            system($cmd) == 0 or die $!;
        };
    }
}

1;
