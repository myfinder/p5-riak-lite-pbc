package Riak::Lite::PBC;

use Mouse;
use IO::Socket;
use Data::MessagePack;
use Riak::PBC;
use Errno qw(EAGAIN EINTR EWOULDBLOCK ECONNRESET);
use Time::HiRes 'time';

our $VERSION = '0.05';

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

has read_quorum => (
    is      => 'rw',
    isa     => 'Num',
    lazy    => 1,
    default => 1,
);

has write_quorum => (
    is      => 'rw',
    isa     => 'Num',
    lazy    => 1,
    default => 3,
);

has sloppy_quorum => (
    is      => 'rw',
    isa     => 'Num',
    lazy    => 1,
    default => 1,
);

has n_val => (
    is      => 'rw',
    isa     => 'Num',
    lazy    => 1,
    default => 1,
);

has _active_socket => (
    is       => 'rw',
    isa      => 'Maybe[IO::Handle]',
);

sub get {
    my ($self, $key) = @_;

    my $req = Riak::PBC::RpbGetReq->new;
    $req->set_bucket($self->bucket);
    $req->set_key($key);
    $req->set_r($self->read_quorum);
    $req->set_sloppy_quorum($self->sloppy_quorum);
    # TODO : available to n_val
    #$req->set_n_val($self->n_val);

    my $h = pack('c', 9) . $req->pack;
    use bytes;
    my $length = length $h;
    my $packed_request = pack('N', $length).$h;
    no bytes;

    my ($code, $msg) = $self->_send_request($packed_request);

    if ($code != 10) {
        warn "invalid response: $code" if $code >= 0;
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
    $req->set_w($self->write_quorum);
    $req->set_sloppy_quorum($self->sloppy_quorum);
    # TODO : available to n_val
    #$req->set_n_val($self->n_val);

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

    warn "invalid response: $code" if $code >= 0;

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

    warn "invalid response: $code" if $code >= 0;

    return;
}

sub DESTROY {
    my $self = shift;

    my $socket = $self->_active_socket;
    $socket->close if $socket;
}

sub _connect_timeout {
    my ($self, $timeout_at) = @_;
    my $timeout = $timeout_at - time;

    my $client = IO::Socket::INET->new(
        PeerAddr => $self->server,
        PeerPort => $self->port,
        Proto    => 'tcp',
        Timeout  => $timeout,
        Blocking => 0,
    );
    unless ($client) {
        warn "failed to connect ${\ $self->server}: $!";
        return;
    }

    {
        # Don't use buffering
        my $orig = select;
        select $client;
        local $| = 1;
        select $orig;
    }

    $client;
}

sub _send_request {
    my ($self, $packed_request, $timeout_at) = @_;

    # Check timeout
    $timeout_at //= time + $self->timeout;

    my ($socket, $in_keep_alive);
    if ($self->_active_socket) {
        $socket = $self->_active_socket;
        $self->_active_socket(undef);
        $in_keep_alive++;
    } else {
        $socket = $self->_connect_timeout($timeout_at) or return (-1, '');
    }

    $self->write_timeout($socket, $packed_request, length($packed_request), 0, $timeout_at);

    my $len;
    my $n = $self->read_timeout($socket, \$len, 4, 0, $timeout_at);
    if (! $n) {
        if ($in_keep_alive && length $len == 0 &&
                                            (defined $n || $! == ECONNRESET)) {
            # Retry if the connection was the old one.
            return $self->_send_request($packed_request, $timeout_at);
        }
        return (-1, '');
    } elsif ($n != 4) {
        die "[BUG] unexpected \$len length: $n";
    }
    $len = unpack('N', $len);

    my $code;
    $n = $self->read_timeout($socket, \$code, 1, 0, $timeout_at);
    return (-1, '') unless $n;
    die "[BUG] unexpected \$code length: $n" unless $n == 1;
    $len -= $n;
    $code = unpack('c', $code);

    my $msg = '';
    while ($len > 0) {
        $n = $self->read_timeout($socket, \$msg, $len, length $msg, $timeout_at);
        return (-1, '') unless $n;
        $len -= $n;
    }

    # Keep the connection for next requests.
    $self->_active_socket($socket);

    return ($code, $msg);
}

sub _select {
    my ($self, $socket, $is_write, $timeout_at) = @_;

    while (1) {
        my $timeout = $timeout_at - time;
        if ($timeout <= 0) {
            $! = 0;
            return 0;
        }
        my($rfd, $wfd);
        my $efd = '';
        vec($efd, $socket->fileno, 1) = 1;
        if ($is_write) {
            $wfd = $efd;
        } else {
            $rfd = $efd;
        }
        my $nfound = select($rfd, $wfd, $efd, $timeout);
        return 1 if $nfound > 0;
    }
    die 'not reached';
}

sub read_timeout {
    my ($self, $socket, $buf, $len, $off, $timeout_at) = @_;
    my $res;

    while (1) {
        defined($res = $socket->sysread($$buf, $len, $off))
            and return $res;
        if ($! == EAGAIN || $! == EWOULDBLOCK || $! == EINTR) {
            # do nothing
        } else {
            return undef;
        }

        $self->_select($socket, 0, $timeout_at) or return undef;
    }
}

sub write_timeout {
    my ($self, $socket, $buf, $len, $off, $timeout_at) = @_;
    my $res;

    while (1) {
        defined($res = $socket->syswrite($buf, $len, $off))
            and return $res;
        if ($! == EAGAIN || $! == EWOULDBLOCK || $! == EINTR) {
            # do nothing
        } else {
            return undef;
        }

        $self->_select($socket, 1, $timeout_at) or return undef;
    }
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
