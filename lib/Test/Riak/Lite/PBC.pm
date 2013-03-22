package Test::Riak::Lite::PBC;

use strict;
use warnings;

use Digest::MD5 qw/md5_hex/;
use Test::More;

use Sub::Exporter;

use Riak::Lite::PBC;

my @exports = qw/
    create_test_bucket_name
    skip_unless_riak
/;

Sub::Exporter::setup_exporter({
    exports => \@exports,
    groups  => { default => \@exports, }
});

sub create_test_bucket_name {
    my $prefix = shift || 'riak-lite-test';
    $prefix . '-' . md5_hex(scalar localtime);
}

sub skip_unless_riak {
    eval {
        Riak::Lite::PBC->new(bucket => 'test-riak')->_connect;
    };
    if ($@) {
        plan skip_all => 'no response from Riak, skip all tests';
    };
}

1;
