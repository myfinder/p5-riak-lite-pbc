use inc::Module::Install;

name 'Riak-Lite-PBC';
all_from 'lib/Riak/Lite/PBC.pm';

requires 'Riak::PBC';
requires 'Mouse';
requires 'IO::Socket';
requires 'Data::MessagePack';

tests_recursive;
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::TCP';
test_requires 'Test::Exception';
auto_set_repository;
auto_include;
WriteAll;
