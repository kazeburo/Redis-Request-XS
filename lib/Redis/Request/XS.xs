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
renewmem(pTHX_ char **d, ssize_t *cur, const ssize_t req) {
    if ( req > *cur ) {
        *cur = ((req % 256) + 1) * 256;
        Renew(*d, *cur, char);
    }
}


static
void
memcat_i(char * dst, int *dst_len, long long int snum ) {
    int i;
    int dlen = *dst_len;
    do {
        dst[dlen++] = '0' + (snum % 10);
    } while ( snum /= 10);
    *dst_len = dlen;
}

static
long int
_index_crlf(char * buf, ssize_t buf_len, ssize_t offset) {
  ssize_t ret = -1;
  ssize_t i;
  for ( i=offset; i < buf_len - 1; i++ ) {
    if (buf[i] == 13 && buf[i+1] == 10 ) {
      ret = i;
      break;
    }
  }
  return ret;
}


static
void
_hv_store(pTHX_ HV * data_hv, const char *key, int key_len, char * buf, ssize_t offset, ssize_t copy_len, int utf8) {
    char *d;
    SV * dst;
    ssize_t i;
    ssize_t dlen = 0;
    dst = newSV(0);
    (void)SvUPGRADE(dst, SVt_PV);
    d = SvGROW(dst, copy_len);
    for (i=offset; i<offset+copy_len; i++){
      d[dlen++] = buf[i];
    }
    SvCUR_set(dst, dlen);
    SvPOK_only(dst);
    if ( utf8 ) { SvUTF8_on(dst); }
    (void)hv_store(data_hv, key, key_len, dst, 0);
}

static
void
_av_push(pTHX_ AV * data_av, char * buf, ssize_t offset, ssize_t copy_len, int utf8) {
    char *d;
    SV * dst;
    ssize_t i;
    ssize_t dlen = 0;
    dst = newSV(0);
    (void)SvUPGRADE(dst, SVt_PV);
    d = SvGROW(dst, copy_len);
    for (i=offset; i<offset+copy_len; i++){
      d[dlen++] = buf[i];
    }
    SvCUR_set(dst, dlen);
    SvPOK_only(dst);
    if ( utf8 ) { SvUTF8_on(dst); }
    (void)av_push(data_av, dst);
}


/*
  == -2 incomplete
  == -1 broken
*/
static
long int
_parse_reply(pTHX_ char * buf, ssize_t buf_len, HV * data_hv, int utf8) {
  long int first_crlf;
  long int m_first_crlf;
  ssize_t v_size;
  ssize_t m_size;
  ssize_t m_v_size;
  ssize_t m_buf_len;
  ssize_t m_read;
  ssize_t j;
  char * m_buf;
  AV * av_list;

  if ( buf_len < 2 ) {
    return -2;
  }
  first_crlf = _index_crlf(buf,buf_len,0);
  if ( first_crlf < 0 ) {
    return -2;
  }

  if ( buf[0] == '+' || buf[0] == ':') {
    /* 1 line reply
    +foo\r\n */
    _hv_store(aTHX_ data_hv, "data", 4, buf, 1, first_crlf-1, utf8);
    return first_crlf + 2;
  }
  else if ( buf[0] == '-' ) {
    /* error
    -ERR unknown command 'a' */
    _hv_store(aTHX_ data_hv, "error", 5, buf, 1, first_crlf-1, utf8);
    return first_crlf + 2;
  }
  else if ( buf[0] == '$' ) {
    /* bulf
       C: get mykey
       S: $3
       S: foo
    */
    if ( buf[1] == '-' && buf[2] == '1' ) {
      (void)hv_store(data_hv, "data", 4, &PL_sv_undef, 0);
      return first_crlf + 2;
    }
    v_size = 0;
    for (j=1; j<first_crlf; j++ ) {
      v_size = v_size * 10 + (buf[j] - '0');
    }
    if ( buf_len - (first_crlf + 2) < v_size + 2 ) {
      return -2;
    }
    _hv_store(aTHX_ data_hv, "data", 4, buf, first_crlf+2, v_size, utf8);
    return first_crlf+2+v_size+2;
  }
  else if ( buf[0] == '*' ) {
    /* multibulk
       # *3
       # $3
       # foo
       # $-1
       # $3
       # baa
       #
       ## null list/timeout
       # *-1
       #
    */
    if ( buf[1] == '-' && buf[2] == '1' ) {
      (void)hv_store(data_hv, "data", 4, &PL_sv_undef, 0);
      return first_crlf + 2;
    }
    m_size = 0;
    for (j=1; j<first_crlf; j++ ) {
      m_size = m_size * 10 + (buf[j] - '0');
    }
    av_list = newAV();
    if ( m_size == 0 ) {
      (void)hv_store(data_hv, "data", 4, newRV_noinc((SV *) av_list), 0);
      return first_crlf + 2;
    }
    m_buf = &buf[first_crlf + 2];
    m_buf_len = buf_len - (first_crlf + 2);
    m_read = 0;
    while ( m_buf_len > m_read ) {
      if (m_buf[0] != '$' ) {
        return -1;
      }
      if (m_buf[1] == '-' && m_buf[2] == '1' ) {
        av_push(av_list, &PL_sv_undef);
        m_buf += 5;
        m_read += 5;
        continue;
      }
      m_first_crlf = _index_crlf(m_buf, m_buf_len - m_read, 0);
      if ( m_first_crlf < 0 ) {
        return -2;
      }
      m_v_size = 0;
      for (j=1; j<m_first_crlf; j++ ) {
        m_v_size = m_v_size * 10 + (m_buf[j] - '0');
      }
      if ( m_buf_len - m_read - (m_first_crlf + 2) < m_v_size + 2 ) {
        return -2;
      }
      _av_push(aTHX_ av_list, m_buf, m_first_crlf+2, m_v_size, utf8);
      m_buf += m_first_crlf+2+m_v_size+2;
      m_read += m_first_crlf+2+m_v_size+2;
    }
    if ( av_len(av_list) + 1 < m_size ) {
      return -2;
    }
    (void)hv_store(data_hv, "data", 4, newRV_noinc((SV *) av_list), 0);
    return first_crlf + 2 + m_read;
  }
  else {
    return -1;
  }
}

static
ssize_t
_write_timeout(int fileno, double timeout, char * write_buf, int write_len ) {
    int rv;
    int nfound;
    fd_set wfds;
    struct timeval tv;
    struct timeval tv_start;
    struct timeval tv_end;
  DO_WRITE:
    rv = write(fileno, write_buf, write_len);
    if ( rv > 0 ) {
      return rv;
    }
    if ( rv < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK ) {
      return rv;
    }
  WAIT_WRITE:
    while (1) {
       FD_ZERO(&wfds);
       FD_SET(fileno, &wfds);
       tv.tv_sec = (int)timeout;
       tv.tv_usec = (timeout - (int)timeout) * 1000000;
       gettimeofday(&tv_start, NULL);
       nfound = select(fileno+1, NULL, &wfds, NULL, &tv);
       gettimeofday(&tv_end, NULL);
       tv.tv_sec = tv_end.tv_sec - tv_start.tv_sec;
       tv.tv_usec = tv_end.tv_usec - tv_start.tv_usec;
       if ( nfound == 1 ) {
         break;
       }
       if ( tv.tv_sec <= 0 && tv.tv_usec <= 0 ) {
         return -1;
       }
    }
    goto DO_WRITE;
}


static
ssize_t
_read_timeout(int fileno, double timeout, char * read_buf, int read_len ) {
    int rv;
    int nfound;
    fd_set rfds;
    struct timeval tv;
    struct timeval tv_start;
    struct timeval tv_end;
  DO_READ:
    rv = read(fileno, read_buf, read_len);
    if ( rv > 0 ) {
      return rv;
    }
    if ( rv < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK ) {
      return rv;
    }
  WAIT_READ:
    while (1) {
       FD_ZERO(&rfds);
       FD_SET(fileno, &rfds);
       tv.tv_sec = (int)timeout;
       tv.tv_usec = (timeout - (int)timeout) * 1000000;
       gettimeofday(&tv_start, NULL);
       nfound = select(fileno+1, &rfds, NULL, NULL, &tv);
       gettimeofday(&tv_end, NULL);
       tv.tv_sec = tv_end.tv_sec - tv_start.tv_sec;
       tv.tv_usec = tv_end.tv_usec - tv_start.tv_usec;
       if ( nfound == 1 ) {
         break;
       }
       if ( tv.tv_sec <= 0 && tv.tv_usec <= 0 ) {
         return -1;
       }
    }
    goto DO_READ;
}



MODULE = Redis::Request::XS    PACKAGE = Redis::Request::XS

PROTOTYPES: DISABLE

SV *
build_request_redis(...)
  ALIAS:
    Redis::Request::XS::build_request_redis = 0
    Redis::Request::XS::build_request_redis_utf8 = 1
  PREINIT:
    int i, j, dest_len = 0, fig=0;
    STRLEN command_arg_len;
    ssize_t dest_size = 1024;
    ssize_t av_size;
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


SV *
send_request_redis(fileno, timeout, ...)
    int fileno
    double timeout
  ALIAS:
    Redis::Request::XS::send_request_redis = 0
    Redis::Request::XS::send_request_redis_utf8 = 1
  PREINIT:
    int i, j, dest_len=0, fig=0, write_len=0, write_off=0;
    STRLEN command_arg_len;
    ssize_t av_size, written;
    ssize_t dest_size=1024;
    char *dest, *command_arg_src, *write_buf;
    SV *command_arg;
    AV *a_list;
  CODE:
    Newx(dest, dest_size, char);

    if ( SvOK(ST(2)) && SvROK(ST(2)) && SvTYPE(SvRV(ST(2))) == SVt_PVAV ) {
      /* build_request([qw/set foo bar/],[qw/set bar baz/]) */
      for( j=2; j < items; j++ ) {
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
      fig = (int)log10(items-2) + 1;
      dest[dest_len++] = '*';
      memcat_i(dest, &dest_len, items-2);
      dest[dest_len++] = 13; // \r
      dest[dest_len++] = 10; // \n

      for( i=2; i < items; i++ ) {
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

    written = 0;
    write_off = 0;
    write_buf = &dest[0];
    while ( (write_len = dest_len - write_off) > 0 ) {
      written = _write_timeout(fileno, timeout, write_buf, write_len);
      if ( written < 0 ) {
        break;
      }
      write_off += written;
      write_buf = &dest[write_off];
    }

    if (written < 0) {
      ST(0) = &PL_sv_undef;
    }
    else {
      ST(0) = sv_newmortal();
      sv_setnv( ST(0), (unsigned long) written);
    }
    Safefree(dest);

ssize_t
phantom_read_redis(fileno)
    int fileno
  PREINIT:
    int buf_size=131072;
  CODE:
    RETVAL = read(fileno, NULL, buf_size);
  OUTPUT:
    RETVAL

SV *
parse_reply(buf, array)
  PROTOTYPE: $$
  PREINIT:
    ssize_t buf_len;
    char * buf;
    HV * data_hv;
    AV * res_av;
    long int ret;
    long int readed;
  CODE:
    buf_len = SvCUR(ST(0));
    buf = SvPV_nolen(ST(0));
    res_av = (AV *) SvRV (ST(1));
    readed = 0;
    while ( buf_len > 0 ) {
      data_hv = newHV();
      ret = _parse_reply(aTHX_ buf, buf_len, data_hv, 0);
      if ( ret == -1 ) {
        XSRETURN_UNDEF;
      }
      else if ( ret == -2 ) {
        break;
      }
      else {
        av_push(res_av, newRV_noinc((SV *) data_hv));
        readed += ret;
        buf_len -= ret;
        buf = &buf[ret];
      }
    }
    RETVAL = newSViv(ret);
  OUTPUT:
    RETVAL

SV *
read_message_redis(fileno, timeout, av_list, required)
    int fileno
    double timeout
    AV * av_list
    ssize_t required
  ALIAS:
    Redis::Request::XS::read_message_redis = 0
    Redis::Request::XS::read_message_redis_utf8 = 1
  PREINIT:
    int has_error=0;
    long int read_max=131072;
    long int read_buf_len=0;
    long int buf_len;
    long int ret;
    long int readed;
    ssize_t parse_result;
    ssize_t parse_offset = 0;
    char *read_buf;
    HV *data_hv;
  CODE:
    Newx(read_buf, read_max, char);
    buf_len = read_max;
    while (1) {
      ret = _read_timeout(fileno, timeout, &read_buf[read_buf_len], read_max);
      if ( ret < 0 ) {
        /* timeout */
        has_error = -2;
        goto do_result;
      }
      read_buf_len += ret;
      while ( read_buf_len > parse_offset ) {
        data_hv = newHV();
        parse_result = _parse_reply(aTHX_ &read_buf[parse_offset], read_buf_len - parse_offset, data_hv, ix);
        if ( parse_result == -1 ) {
          /* corruption */
          has_error = -1;
          goto do_result;
        }
        else if ( parse_result == -2 ) {
          break;
        }
        else {
          parse_offset += parse_result;
          av_push(av_list, newRV_noinc((SV *) data_hv));
        }
      }
      if ( av_len(av_list) + 1 >= required ) {
        break;
      }
      renewmem(aTHX_ &read_buf, &buf_len, read_buf_len + read_max);

    }
    do_result:
    /*
     == -2 timeout
     == -1 message corruption
    */
    if ( has_error < 0 ) {
      RETVAL = newSViv(has_error);
    }
    else {
      RETVAL = newSViv(has_error);
    }
    Safefree(read_buf);
  OUTPUT:
    RETVAL






