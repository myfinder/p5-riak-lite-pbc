use strict;
use warnings;
use Test::More;
use Test::TCP;
use Test::Exception;
use Test::Riak::Lite::PBC;

use Riak::Lite::PBC;

skip_unless_riak;

my $bucket_name = create_test_bucket_name;

subtest 'default 50ms timeout test' => sub {
    test_tcp(
        client => sub {
            my $port = shift;
            my $riak = Riak::Lite::PBC->new(
                port   => $port,
                bucket => $bucket_name,
            );
            dies_ok {
                $riak->set('key','value');
            } 'set time out ok';
            dies_ok {
                $riak->get('key');
            } 'get time out ok';
            dies_ok {
                $riak->delete('key');
            } 'delete time out ok';
        },
        server => sub {
            my $port = shift;
            DummyRiakServer->new($port)->run;
        },
    );
};

done_testing;

package DummyRiakServer;
use IO::Socket::INET;
use Time::HiRes qw/usleep/;

sub new {
    my ($class, $port) = @_;

    my $sock = IO::Socket::INET->new(
        LocalPort => $port,
        LocalAddr => '127.0.0.1',
        Proto     => 'tcp',
        Listen    => 5,
        Type      => SOCK_STREAM,
    ) or die "Cannot open server socket: $!";
    bless { sock => $sock }, $class;
}

sub run {
    my $self = shift;

    while (my $remote = $self->{sock}->accept) {
        usleep 1000000; # 100ms sleep.
    }
}
