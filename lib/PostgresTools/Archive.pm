package PostgresTools::Archive;

use strict;
use warnings;
use 5.012;

use Moo;
use File::Path;
use PostgresTools::Date;

has keep_days => ( is => 'rw' );
has dst       => ( is => 'ro', required => 1 );
has base_dir  => ( is => 'ro', required => 1 );

sub BUILD {
    my $self = shift;
    unless ( $self->{dst} ) {
        die "archive destination is undefined or empty string";
    }
}

sub backup {
    my $self = shift;

    system("rsync --archive $self->{base_dir} $self->{dst}") == 0
        or die "$?, $!";
}

sub clean {
    my $self = shift;

    if ( $self->{keep_days} < 0 ) {
        return;
    }

    my $formatter = DateTime::Format::Strptime->new( pattern => '%Y%m%d' );
    my $date = PostgresTools::Date->new(
        formatter => $formatter,
    );
    my $delete_date = $date->offset2date( $self->{keep_days} );
    my $to_clean    = "$self->{base_dir}/$delete_date";

    if ( -d $to_clean ) {
        rmtree $to_clean or warn $!;
    }

    system("rsync --archive --delete $self->{base_dir} $self->{dst}") == 0
        or die "$?, $!";
}

1;
