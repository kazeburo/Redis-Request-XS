#!/usr/bin/env perl

use strict;
use warnings;
use 5.10.0;
use Benchmark qw/cmpthese/;
use Redis::Request::XS;
my $CRLF="\015\012";

sub build_request_pp {
    my $msg='';
    if ( ref $_[0] eq 'ARRAY') {
        for my $msgs ( @_ ) {
            $msg .= '*'.scalar(@$msgs).$CRLF;
            for my $m (@$msgs) {
                $msg .= '$'.length($m).$CRLF.$m.$CRLF;
            }
        }
    }
    else {
        $msg .= '*'.scalar(@_).$CRLF;
        for my $m (@_) {
            $msg .= '$'.length($m).$CRLF.$m.$CRLF;
        }
    }
    $msg;
}

cmpthese(
    -1,
    {
        pp => sub {
            build_request_pp(qw/set foo bar/);
        },
        xs => sub {
            build_request_redis(qw/set foo bar/);
        },
    }
);

cmpthese(
    -1,
    {
        pp => sub {
            build_request_pp(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
        xs => sub {
            build_request_redis(
                [qw/del user-fail/],
                [qw/del ip-fail/],
                [qw/lpush user-log xxxxxxxxxxx/],
                [qw/lpush login-log yyyyyyyyyyy/]
            );
        },
    }
);

