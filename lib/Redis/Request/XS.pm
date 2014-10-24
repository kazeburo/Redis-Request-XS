package Redis::Request::XS;
use 5.008005;
use strict;
use warnings;
use base qw/Exporter/;

our $VERSION = "0.01";
our @EXPORT = qw/build_request_redis build_request_redis_utf8/;

use XSLoader;
XSLoader::load(__PACKAGE__, $VERSION);

1;
__END__

=encoding utf-8

=head1 NAME

Redis::Request::XS - It's new $module

=head1 SYNOPSIS

    use Redis::Request::XS;

=head1 DESCRIPTION

Redis::Request::XS is ...

=head1 LICENSE

Copyright (C) Masahiro Nagano.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masahiro Nagano E<lt>kazeburo@gmail.comE<gt>

=cut

