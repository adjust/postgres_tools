package PostgresTools::Database;

use strict;
use warnings;
use 5.012;

use Moo;
use DBD::Pg;

has db   => ( is => 'rw' );
has host => ( is => 'rw' );
has user => ( is => 'rw' );

sub BUILD { }

sub tables {
    my $self   = shift;
    my $result = [];
    for ( @{ $self->_make_request( $self->_get_tables_sql ) } ) {
        push( @$result, $_->[0] );
    }
    return $result;
}

sub sequences {
    my $self   = shift;
    my $result = [];
    for ( @{ $self->_make_request( $self->_get_sequences_sql ) } ) {
        push( @$result, $_->[0] );
    }
    return $result;
}

sub partitions {
    my $self   = shift;
    my $result = [];
    for ( @{ $self->_make_request( $self->_get_partitions_sql ) } ) {
        push( @$result, $_->[0] );
    }
    return $result;
}

sub _get_tables_sql {
    return qq(
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      ORDER BY table_schema, table_name;
    );
}

sub _get_sequences_sql {
    return qq(
      SELECT c.relname
      FROM pg_class c
      WHERE c.relkind = 'S';
    );
}

sub _get_partitions_sql {
    return qq(
    SELECT child.relname
      AS child_schema
    FROM pg_inherits
      JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
      JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
      JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
      JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace;
    );
}

sub _make_request {
    my $self = shift;
    my $dbh  = DBI->connect(
        "dbi:Pg:dbname=$self->{'db'};host=$self->{'host'}",
        $self->{'user'},
        '',
    );
    my $resp = $dbh->selectall_arrayref(shift);
    $dbh->disconnect;
    return $resp;
}

1;
