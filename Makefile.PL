use inc::Module::Install;
name 'Riak-Lite-PBC';
all_from 'lib/Riak/Lite/PBC.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
