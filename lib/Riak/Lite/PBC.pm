package Riak::Lite::PBC;

use Mouse;
use IO::Socket;
use Data::MessagePack;
use Riak::PBC;
use Carp ();

our $VERSION = '0.01';

has server => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => '127.0.0.1',
);

has port => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => 8087,
);

has bucket => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
);

has timeout => (
    is      => 'rw',
    isa     => 'Num',
    lazy    => 1,
    default => 0.05,
);

has client => (
    is       => 'rw',
    isa      => 'IO::Handle',
    required => 1,
    default  => sub {
        IO::Socket::INET->new(
            PeerAddr => $_[0]->server,
            PeerPort => $_[0]->port,
            Proto    => 'tcp',
            Timeout  => $_[0]->timeout,
        ) or Carp::croak("failed to connect ".$_[0]->server.": $!");
    },
);

sub get {
    my ($self, $key) = @_;

    my $req = Riak::PBC::RpbGetReq->new;
    $req->set_bucket($self->bucket);
    $req->set_key($key);

    my $h = pack('c', 9) . $req->pack;
    use bytes;
    my $length = length $h;
    my $packed_request = pack('N', $length).$h;
    no bytes;

    my ($code, $msg) = $self->_send_request($packed_request);

    return if $code ne 10; # error case

    my $response = Riak::PBC::RpbGetResp->new;
    $response->unpack($msg);

    return unless $response->content; # value is now exists.
    return Data::MessagePack->unpack($response->content->value);
}

sub set {
    my ($self, $key, $value) = @_;

    my $req = Riak::PBC::RpbPutReq->new;
    $req->set_bucket($self->bucket);
    $req->set_key($key);

    my $content = Riak::PBC::RpbContent->new;

    $content->set_value(Data::MessagePack->pack($value));

    $req->set_content($content);

    my $h = pack('c', 11) . $req->pack;
    use bytes;
    my $length = length $h;
    my $packed_request = pack('N', $length).$h;
    no bytes;

    my ($code, $msg) = $self->_send_request($packed_request);

    if ($code eq 12) {
        return 1; # set success
    }
    return;
}

sub delete {
    my ($self, $key) = @_;

    my $req = Riak::PBC::RpbDelReq->new;
    $req->set_bucket($self->bucket);
    $req->set_key($key);

    my $h = pack('c', 13) . $req->pack;
    use bytes;
    my $length = length $h;
    my $packed_request = pack('N', $length).$h;
    no bytes;

    my ($code, $msg) = $self->_send_request($packed_request);

    if ($code eq 14) {
        return 1; # delete success
    }
    return;
}

sub DESTROY {
    my $self = shift;

    $self->client->close;
}

sub _send_request {
    my ($self, $packed_request) = @_;

    unless ($self->client->connected) {
        $self->client(IO::Socket::INET->new(
                PeerAddr => $self->server,
                PeerPort => $self->port,
                Proto    => 'tcp',
                Timeout  => $self->timeout,
            ) or Carp::croak("failed to connect ".$self->server.": $!")
        );
    }

    $self->client->print($packed_request);

    my ($len, $code, $msg);
    $self->client->read($len, 4);
    $len = unpack('N', $len);
    $self->client->read($code, 1);
    $code = unpack('c', $code);
    $self->client->read($msg, $len - 1);

    return ($code, $msg);
}

no Mouse;
__PACKAGE__->meta->make_immutable;
1;
__END__

=head1 NAME

Riak::Lite::PBC - simple and lightweight client interface to Riak PBC API

=head1 SYNOPSIS

  use Riak::Lite::PBC;

=head1 DESCRIPTION

Riak::Lite::PBC is simple and lightweight client interface to Riak PBC API

=head1 AUTHOR

Tatsuro Hisamori E<lt>myfinder@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
