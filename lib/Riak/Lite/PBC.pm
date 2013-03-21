package Riak::Lite::PBC;

use Mouse;
use IO::Socket;
use Data::MessagePack;
use Riak::PBC;

our $VERSION = '0.03';

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
    default => 0.5,
);

has client => (
    is       => 'rw',
    isa      => 'Maybe[IO::Handle]',
    required => 1,
    default  => sub {
        IO::Socket::INET->new(
            PeerAddr => $_[0]->server,
            PeerPort => $_[0]->port,
            Proto    => 'tcp',
            Timeout  => $_[0]->timeout,
        ) or die "failed to connect ".$_[0]->server.": $!";
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

    if ($code != 10) {
        warn "invalid response: $code";
        return;
    }

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

    if ($code == 12) {
        return 1; # set success
    }

    warn "invalid response: $code";

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

    if ($code == 14) {
        return 1; # delete success
    }

    warn "invalid response: $code";

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
            ) or die "failed to connect $self->server: $!"
        );
    }

    $self->client->print($packed_request);

    my ($len, $code, $msg);
    eval {
        _check($self->client->read($len, 4)) or die "can't read len";
        $len = unpack('N', $len);
        _check($self->client->read($code, 1)) or die "can't read code";
        $code = unpack('c', $code);
        _check($self->client->read($msg, $len - 1)) or die "can't read msg";
    };
    if ($@) {
        warn $@;
        $self->client->close;
        $self->client(undef);
        return(0, '');
    }

    return ($code, $msg);
}

sub _check {
    defined $_[0] or return;
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
