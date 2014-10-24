#ifdef __cplusplus
extern "C" {
#endif

#define PERL_NO_GET_CONTEXT /* we want efficiency */
#include <EXTERN.h>
#include <perl.h>
#include <XSUB.h>

#ifdef __cplusplus
} /* extern "C" */
#endif

#define NEED_newSVpvn_flags
#include "ppport.h"

static
void
memcat( char * dst, int *dst_len, const char * src, int src_len ) {
    int i;
    int dlen = *dst_len;
    for ( i=0; i<src_len; i++) {
        dst[dlen++] = src[i];
    }
    *dst_len = dlen;
}

static
void
memcopyset( char * dst, int dst_len, const char * src, int src_len ) {
    int i;
    int dlen = dst_len;
    for ( i=0; i<src_len; i++) {
        dst[dlen++] = src[i];
    }
}

static
char *
svpv2char(pTHX_ SV *string, STRLEN *len, int utf8) {
    char *str;
    STRLEN str_len;
    if ( utf8 == 1 ) {
        SvGETMAGIC(string);
        if (!SvUTF8(string)) {
            string = sv_mortalcopy(string);
            sv_utf8_encode(string);
        }
    }
    str = (char *)SvPV(string,str_len);
    *len = str_len;
    return str;
}

static
void
renewmem(pTHX_ char **d, int *cur, const int req) {
    if ( req > *cur ) {
        *cur = ((req % 256) + 1) * 256;
        Renew(*d, *cur, char);
    }
}


static
void
memcat_i( char * dst, int *dst_len, long long int snum ) {
    int i;
    int dlen = *dst_len;
    do {
        dst[dlen++] = '0' + (snum % 10);
    } while ( snum /= 10);
    *dst_len = dlen;
}


MODULE = Redis::Request::XS    PACKAGE = Redis::Request::XS

PROTOTYPES: DISABLE

SV *
build_request_redis(...)
  ALIAS:
    Redis::Request::XS::build_request_redis = 0
    Redis::Request::XS::build_request_redis_utf8 = 1
  PREINIT:
    int i, j, dest_len = 0, dest_size = 1024, fig=0;
    STRLEN command_arg_len;
    SSize_t av_size;
    char *dest, *command_arg_src;
    SV *command_arg;
    AV *a_list;
  CODE:
    Newx(dest, dest_size, char);

    if ( SvOK(ST(0)) && SvROK(ST(0)) && SvTYPE(SvRV(ST(0))) == SVt_PVAV ) {
      /* build_request([qw/set foo bar/],[qw/set bar baz/]) */
      for( j=0; j < items; j++ ) {
        a_list = (AV *)SvRV(ST(j));
        av_size = av_len(a_list);
        av_size++;
        fig = (int)log10(av_size) + 1;
        dest[dest_len++] = '*';
        memcat_i(dest, &dest_len, av_size);
        dest[dest_len++] = 13; // \r
        dest[dest_len++] = 10; // \n
        for (i=0; i<av_size; i++) {
          command_arg = *av_fetch(a_list,i,0);
          command_arg_src = svpv2char(aTHX_ command_arg, &command_arg_len, ix);
          fig = (int)log10(command_arg_len) + 1;
          /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
          renewmem(aTHX_ &dest, &dest_size, 1 + fig + 2 + command_arg_len + 2);
          dest[dest_len++] = '$';
          memcat_i(dest, &dest_len, command_arg_len);
          dest[dest_len++] = 13; // \r
          dest[dest_len++] = 10; // \n
          memcat(dest, &dest_len, command_arg_src, command_arg_len);
          dest[dest_len++] = 13; // \r
          dest[dest_len++] = 10; // \n
        }
      }
    }
    else {
      /* build_request(qw/set bar baz/)
      $msg .= '*'.scalar(@_).$CRLF;
      for my $m (@_) {
        utf8::encode($m) if $self->{utf8};
        $msg .= '$'.length($m).$CRLF.$m.$CRLF;
      }
      */
      fig = (int)log10(items) + 1;
      dest[dest_len++] = '*';
      memcat_i(dest, &dest_len, items);
      dest[dest_len++] = 13; // \r
      dest[dest_len++] = 10; // \n

      for( i=0; i < items; i++ ) {
        command_arg = ST(i);
        command_arg_src = svpv2char(aTHX_ command_arg, &command_arg_len, ix);
        fig = (int)log10(command_arg_len) + 1;
        /* 1($) + fig + 2(crlf) + command_arg_len + 2 */
        renewmem(aTHX_ &dest, &dest_size, 1 + fig + 2 + command_arg_len + 2);
        dest[dest_len++] = '$';
        memcat_i(dest, &dest_len, command_arg_len);
        dest[dest_len++] = 13; // \r
        dest[dest_len++] = 10; // \n
        memcat(dest, &dest_len, command_arg_src, command_arg_len);
        dest[dest_len++] = 13; // \r
        dest[dest_len++] = 10; // \n
      }
    }
    RETVAL = newSVpvn(dest, dest_len);
    SvPOK_only(RETVAL);
    Safefree(dest);
  OUTPUT:
    RETVAL

