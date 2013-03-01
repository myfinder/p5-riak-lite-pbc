use strict;
use Test::More;
use Test::Riak::Lite::PBC;

use Riak::Lite::PBC;

skip_unless_riak;

my $bucket_name = create_test_bucket_name;

ok my $riak = Riak::Lite::PBC->new(
    bucket  => $bucket_name,
);

subtest 'simple key/value set/get/remove' => sub {
    ok ! defined $riak->get('key'), 'return undef ok';
    ok $riak->set('key', 'value'), 'simple set ok';
    ok $riak->get('key') eq 'value', 'return value ok';
    ok $riak->delete('key'), 'remove key/value ok';
    ok ! defined $riak->get('key'), 'return undef ok';
};

subtest 'structured value set/get/remove' => sub {
    my $value = {
        foo  => 'var',
        hoge => [ 'piyo', 'poyo' ],
    };

    ok ! defined $riak->get('key'), 'return undef ok';
    ok $riak->set('key', $value), 'structured value set ok';
    is_deeply $riak->get('key'), $value, 'return value ok';
    ok $riak->delete('key'), 'remove key/value ok';
    ok ! defined $riak->get('key'), 'return undef ok';
};

done_testing;
