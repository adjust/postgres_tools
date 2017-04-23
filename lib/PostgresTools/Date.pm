package PostgresTools::Date;

use strict;
use warnings;
use 5.012;

use Moo;
use DateTime;
use Storable qw(dclone);
$Storable::forgive_me = 42;

has _now_     => ( is => 'rw' );
has formatter => ( is => 'rw' );

sub BUILD {
    my $self = shift;
    $self->formatter( DateTime::Format::Strptime->new( pattern => '%Y_%m_%d' ) ) unless $self->formatter;
    $self->_now_( DateTime->today( formatter => $self->{formatter} ) ) unless $self->_now_;
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
    if ( $string =~ m/\d{4}_\d{2}/ ) {
        my ( $year, $month ) = split( '_', $& );
        return DateTime->new(
            year  => $year,
            month => $month,
            day   => 1,
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
    my $now      = dclone( $self->_now_ );
    my $duration = DateTime::Duration->new( days => $offset );
    my $old_date = $now->subtract_duration($duration);
    return $date->subtract_datetime($old_date)->is_positive;
}

sub older_than {
    my $self = shift;
    my $date = shift;
    return if !defined($date);
    my $offset   = shift;
    my $now      = dclone( $self->_now_ );
    my $duration = DateTime::Duration->new( days => $offset );
    my $old_date = $now->subtract_duration($duration);
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
    my $now      = dclone( $self->_now_ );
    return $now->subtract_duration($duration);
}

1;
