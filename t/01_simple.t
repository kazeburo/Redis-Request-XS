use strict;
use Test::More;

use Redis::Request::XS;

is(Redis::Request::XS::build_request_redis(qw/set foofoo/), 
   join("\015\012",qw/*2 $3 set $6 foofoo/,""));
is(Redis::Request::XS::build_request_redis([qw/set foofoo/],['ping']),
   join("\015\012",qw/*2 $3 set $6 foofoo *1 $4 ping/,""));

is(Redis::Request::XS::build_request_redis('mget',"\xE5","\x{263A}"),
   join("\015\012",qw/*3 $4 mget $1/,"\xE5",qw/$3/,"\xE2\x98\xBA","")
);

is(Redis::Request::XS::build_request_redis_utf8('mget',"\xE5","\x{263A}"),
   join("\015\012",qw/*3 $4 mget $2/,"\xC3\xA5",qw/$3/,"\xE2\x98\xBA","")
);

done_testing;

