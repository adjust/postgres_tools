package PostgresTools::Date;

use strict;
use warnings;
use 5.012;

use Moo;
use DateTime;

sub BUILD { }

sub date_from_string {
    my $self   = shift;
    my $string = shift;
    if ( $string =~ m/\d{4}_\d{2}_\d{2}/ ) {
        my ( $year, $month, $day ) = split( '_', $& );
        return DateTime->new(
            year  => $year,
            month => $month,
            day   => $day,
        );
    } else {
        say "could not parse date, creating epoch date";
        return DateTime->from_epoch(
            epoch => 0,
        );
    }
}

sub older_than {
    my $self     = shift;
    my $date     = shift;
    my $older    = shift;
    my $duration = DateTime::Duration->new( days => $older );
    my $old_date = DateTime->today()->subtract_duration($duration);
    return $date->subtract_datetime($old_date)->is_positive;
}

sub older_than_from_string {
    my $self  = shift;
    my $date  = $self->date_from_string(shift);
    my $older = shift;
    return $self->older_than( $date, $older );
}

1;
