package PostgresTools::Date;

use strict;
use warnings;
use 5.012;

use Moo;
use DateTime;

has now => ( is => 'rw' );

sub BUILD {
    my $self = shift;
    $self->now( DateTime->today( formatter => DateTime::Format::Strptime->new( pattern => '%Y_%m_%d' ) ) ) unless $self->now;
}

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
    }
    return undef;
}

sub string_date_from_string {
    my $self   = shift;
    my $string = shift;
    if ( $string =~ m/\d{4}_\d{2}_\d{2}/ ) {
        return $&;
    }
    return '';
}

sub newer_than {
    my $self = shift;
    my $date = shift;
    return if !defined($date);
    my $offset   = shift;
    my $duration = DateTime::Duration->new( days => $offset );
    my $old_date = $self->now->subtract_duration($duration);
    return $date->subtract_datetime($old_date)->is_positive;
}

sub older_than {
    my $self = shift;
    my $date = shift;
    return if !defined($date);
    my $offset   = shift;
    my $duration = DateTime::Duration->new( days => $offset );
    my $old_date = $self->now->subtract_duration($duration);
    return $date->subtract_datetime($old_date)->is_negative;
}

sub newer_than_from_string {
    my $self   = shift;
    my $date   = $self->date_from_string(shift);
    my $offset = shift;
    return $self->newer_than( $date, $offset );
}

sub older_than_from_string {
    my $self   = shift;
    my $date   = $self->date_from_string(shift);
    my $offset = shift;
    return $self->older_than( $date, $offset );
}

sub offset2date {
    my $self     = shift;
    my $offset   = shift;
    my $duration = DateTime::Duration->new( days => $offset );
    return $self->now->subtract_duration($duration);
}

1;
