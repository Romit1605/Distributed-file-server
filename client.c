/* ============================================================
 *  client.c                           COMP-8567  Winter 2026
 *
 *  Distinctive design choices on the client side:
 *
 *    - A CHARACTER-BY-CHARACTER state-machine tokenizer
 *      (IN_SPACE / IN_TOKEN / DONE) instead of strtok().
 *    - A CONSTRAINT TABLE that attaches a per-command
 *      validator function pointer to every command.  Each
 *      validator is a tiny function that reports why input
 *      is bad — no inline validation anywhere.
 *    - File reception in fixed 4 KB chunks with a
 *      \r-repainted ASCII progress bar.
 *    - Redirect-aware connect(): when the main server
 *      answers with "REDIRECT <host> <port>\n", the client
 *      tears down and reconnects automatically.
 *    - Received files are written to $HOME/project/ which
 *      is created on first use with getenv("HOME") + manual
 *      bounds-checked path construction (no hard-coded /home).
 * ============================================================ */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>

#define DEFAULT_HOST  "127.0.0.1"
#define DEFAULT_PORT  19081
#define BUFSZ         4096
#define MAX_ARGS      8

/* ============================================================
 *  A.  Character-by-character tokenizer (explicit FSM)
 * ============================================================ */
typedef enum { TOK_IN_SPACE, TOK_IN_TOKEN, TOK_DONE } tok_state_t;

static int sm_tokenize(char *line, char **argv, int max) {
    tok_state_t st = TOK_IN_SPACE;
    int argc = 0;
    char *p = line;
    while (st != TOK_DONE) {
        char c = *p;
        switch (st) {
        case TOK_IN_SPACE:
            if (c == '\0' || c == '\n')             st = TOK_DONE;
            else if (c == ' ' || c == '\t')         p++;
            else {
                if (argc >= max) { st = TOK_DONE; break; }
                argv[argc++] = p;
                st = TOK_IN_TOKEN;
            }
            break;
        case TOK_IN_TOKEN:
            if (c == '\0' || c == '\n')             { *p = 0; st = TOK_DONE; }
            else if (c == ' ' || c == '\t')         { *p++ = 0; st = TOK_IN_SPACE; }
            else                                    p++;
            break;
        case TOK_DONE: break;
        }
    }
    return argc;
}

/* ============================================================
 *  B.  Per-command validators (function pointers)
 *
 *      Each returns 1 if ok, 0 otherwise, and on failure
 *      writes a human-readable reason into errmsg.
 * ============================================================ */
typedef int (*validator_fn)(char **argv, int argc, char *errmsg, size_t emlen);

static int only_digits(const char *s) {
    if (!*s) return 0;
    for (; *s; s++) if (!isdigit((unsigned char)*s)) return 0;
    return 1;                                                       /* signal true / match */
}
static int looks_like_date(const char *s) {
    /* YYYY-MM-DD */
    if (strlen(s) != 10) return 0;
    if (s[4] != '-' || s[7] != '-') return 0;
    for (int i = 0; i < 10; i++)
        if (i != 4 && i != 7 && !isdigit((unsigned char)s[i])) return 0;
    return 1;                                                       /* signal true / match */
}

static int v_dirlist(char **argv, int argc, char *em, size_t n) {
    if (argc != 2) { snprintf(em, n, "dirlist expects exactly one flag"); return 0; } /* safe formatted write */
    if (strcmp(argv[1], "-a") && strcmp(argv[1], "-t")) {           /* string compare */
        snprintf(em, n, "dirlist flag must be -a or -t"); return 0; /* safe formatted write */
    }
    return 1;                                                       /* signal true / match */
}
static int v_fn(char **argv, int argc, char *em, size_t n) {
    (void)argv;
    if (argc != 2) { snprintf(em, n, "fn expects exactly one filename"); return 0; } /* safe formatted write */
    return 1;                                                       /* signal true / match */
}
static int v_fz(char **argv, int argc, char *em, size_t n) {
    if (argc != 3) { snprintf(em, n, "fz expects size1 size2"); return 0; } /* safe formatted write */
    if (!only_digits(argv[1]) || !only_digits(argv[2])) {
        snprintf(em, n, "fz sizes must be non-negative integers"); return 0; /* safe formatted write */
    }
    long long a = atoll(argv[1]), b = atoll(argv[2]);
    if (a < 0 || b < 0 || a > b) {
        snprintf(em, n, "fz requires 0 <= size1 <= size2"); return 0; /* safe formatted write */
    }
    return 1;                                                       /* signal true / match */
}
static int v_ft(char **argv, int argc, char *em, size_t n) {
    (void)argv;
    if (argc < 2 || argc > 4) {
        snprintf(em, n, "ft expects 1..3 extensions"); return 0;    /* safe formatted write */
    }
    return 1;                                                       /* signal true / match */
}
static int v_fdate(char **argv, int argc, char *em, size_t n) {
    if (argc != 2) { snprintf(em, n, "date command expects one date"); return 0; } /* safe formatted write */
    if (!looks_like_date(argv[1])) {
        snprintf(em, n, "date must be YYYY-MM-DD"); return 0;       /* safe formatted write */
    }
    return 1;                                                       /* signal true / match */
}
static int v_quitc(char **argv, int argc, char *em, size_t n) {
    (void)argv;(void)em;(void)n;
    return argc == 1;
}

/* ---------- constraint table ---------- */
struct Constraint {
    const char   *name;
    validator_fn  validate;
};
static struct Constraint ctab[] = {
    { "dirlist", v_dirlist },
    { "fn",      v_fn      },
    { "fz",      v_fz      },
    { "ft",      v_ft      },
    { "fdb",     v_fdate   },
    { "fda",     v_fdate   },
    { "quitc",   v_quitc   },
    { NULL, NULL }
};
static validator_fn find_validator(const char *name) {
    for (int i = 0; ctab[i].name; i++)
        if (strcmp(ctab[i].name, name) == 0) return ctab[i].validate; /* string compare */
    return NULL;
}

/* ============================================================
 *  C.  Socket + framed-I/O helpers
 * ============================================================ */
static ssize_t send_all(int s, const void *buf, size_t n) {
    const char *p = buf; size_t left = n;
    while (left) {                                                  /* keep going until all bytes handled */
        ssize_t k = send(s, p, left, 0);
        if (k <= 0) { if (errno == EINTR) continue; return -1; }    /* handle interrupt, else give up */
        p += k; left -= k;
    }
    return (ssize_t)n;
}
static int recv_line(int s, char *out, int max) {
    int i = 0; char c;
    while (i < max - 1) {
        ssize_t k = recv(s, &c, 1, 0);
        if (k <= 0) return -1;
        if (c == '\n') break;
        out[i++] = c;
    }
    out[i] = 0;
    return i;
}
static int recv_exact(int s, void *buf, size_t n) {
    char *p = buf; size_t left = n;
    while (left) {                                                  /* keep going until all bytes handled */
        ssize_t k = recv(s, p, left, 0);
        if (k <= 0) { if (errno == EINTR) continue; return -1; }    /* handle interrupt, else give up */
        p += k; left -= k;
    }
    return 0;                                                       /* signal success */
}

/* ============================================================
 *  D.  Manual bounded path: $HOME/project[/fname]
 * ============================================================ */
static int build_home_path(const char *leaf, char *out, size_t outsz) {
    const char *home = getenv("HOME"); if (!home) home = ".";
    size_t hl = strlen(home), ll = strlen("/project/") + strlen(leaf) + 1;
    if (hl + ll >= outsz) return -1;
    size_t i = 0;
    memcpy(out + i, home, hl);            i += hl;                  /* raw byte copy */
    memcpy(out + i, "/project", 8);       i += 8;                   /* raw byte copy */
    if (*leaf) { out[i++] = '/'; memcpy(out + i, leaf, strlen(leaf)); i += strlen(leaf); } /* raw byte copy */
    out[i] = 0;
    return 0;                                                       /* signal success */
}
static void ensure_project_dir(void) {
    char dir[PATH_MAX];
    if (build_home_path("", dir, sizeof(dir)) < 0) return;
    mkdir(dir, 0755);                                               /* best-effort; ignore EEXIST */
}

/* ============================================================
 *  E.  Chunked file receive with progress bar
 * ============================================================ */
static void draw_bar(long long got, long long total) {
    int width = 30;
    int filled = total > 0 ? (int)((got * width) / total) : width;
    int pct    = total > 0 ? (int)((got * 100) / total) : 100;
    fputc('\r', stdout);
    fputc('[', stdout);
    for (int i = 0; i < width; i++) fputc(i < filled ? '#' : ' ', stdout);
    printf("] %3d%%  (%lld / %lld bytes)", pct, got, total);
    fflush(stdout);
}

static int receive_file_chunked(int s, long long total, const char *savepath) {
    int fd = open(savepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);    /* overwrite any existing file */
    if (fd < 0) { perror("open"); return -1; }                      /* open/socket failed */
    char chunk[BUFSZ];
    long long got = 0;
    while (got < total) {
        size_t want = (total - got) > (long long)sizeof(chunk)
                      ? sizeof(chunk) : (size_t)(total - got);
        ssize_t k = recv(s, chunk, want, 0);
        if (k <= 0) { close(fd); return -1; }
        if (write(fd, chunk, k) != k) { close(fd); return -1; }
        got += k;
        draw_bar(got, total);
    }
    fputc('\n', stdout);
    close(fd);
    return 0;                                                       /* signal success */
}

/* ============================================================
 *  F.  Response handler (reads TEXT / FILE / ERR / BYE frames)
 * ============================================================ */
static int read_response(int s) {
    char tag[16];
    if (recv_line(s, tag, sizeof(tag)) <= 0) return -1;

    if (strcmp(tag, "BYE") == 0) {                                  /* string compare */
        char len[32]; recv_line(s, len, sizeof(len));
        long n = atol(len);
        char tmp[256];
        if (n > 0 && n < (long)sizeof(tmp)) recv_exact(s, tmp, n);
        return 1;  /* signal quit */
    }
    if (strcmp(tag, "TEXT") == 0 || strcmp(tag, "ERR") == 0) {      /* string compare */
        char len[32]; recv_line(s, len, sizeof(len));
        long n = atol(len);
        if (n < 0) return -1;
        char *buf = malloc(n + 1);                                  /* allocate on the heap */
        if (n > 0 && recv_exact(s, buf, n) < 0) { free(buf); return -1; } /* release heap memory */
        buf[n] = 0;
        if (strcmp(tag, "ERR") == 0) printf("[server] %s\n", buf);  /* string compare */
        else                          fputs(buf, stdout);
        free(buf);                                                  /* release heap memory */
        return 0;                                                   /* signal success */
    }
    if (strcmp(tag, "FILE") == 0) {                                 /* string compare */
        char len[32]; recv_line(s, len, sizeof(len));
        long long n = atoll(len);
        ensure_project_dir();
        char savepath[PATH_MAX];
        if (build_home_path("temp.tar.gz", savepath, sizeof(savepath)) < 0) {
            printf("path too long\n"); return -1;
        }
        printf("Receiving %lld bytes into %s\n", n, savepath);
        if (receive_file_chunked(s, n, savepath) < 0) {
            printf("file receive failed\n"); return -1;
        }
        printf("Saved: %s\n", savepath);
        return 0;                                                   /* signal success */
    }
    printf("[client] unknown response tag: %s\n", tag);
    return -1;                                                      /* signal failure to caller */
}

/* ============================================================
 *  G.  Connect (with automatic REDIRECT follow-up)
 * ============================================================ */
static int dial(const char *host, int port) {
    int s = socket(AF_INET, SOCK_STREAM, 0);                        /* plain TCP socket */
    if (s < 0) return -1;
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;                                        /* IPv4 address family */
    sa.sin_port   = htons(port);                                    /* port in network byte order */
    if (inet_pton(AF_INET, host, &sa.sin_addr) <= 0) { close(s); return -1; } /* parse dotted-quad IPv4 string */
    if (connect(s, (struct sockaddr*)&sa, sizeof(sa)) < 0) { close(s); return -1; } /* open the TCP connection */
    return s;
}

static int connect_with_redirect(const char *host, int port) {
    for (int hop = 0; hop < 3; hop++) {
        int s = dial(host, port);
        if (s < 0) { perror("connect"); return -1; }
        char line[128];
        if (recv_line(s, line, sizeof(line)) <= 0) { close(s); return -1; }
        if (strncmp(line, "REDIRECT ", 9) == 0) {                   /* gateway routed us elsewhere */
            static char nhost[64]; int nport = 0;
            if (sscanf(line + 9, "%63s %d", nhost, &nport) == 2) {
                printf("[client] redirected to %s:%d\n", nhost, nport);
                close(s);
                host = nhost; port = nport;
                continue;
            }
            close(s); return -1;
        }
        printf("[client] connected: %s\n", line);
        return s;
    }
    fprintf(stderr, "[client] too many redirects\n");
    return -1;                                                      /* signal failure to caller */
}

/* ============================================================
 *  H.  REPL
 * ============================================================ */
int main(int argc, char **argv) {
    const char *host = (argc > 1) ? argv[1] : DEFAULT_HOST;
    int         port = (argc > 2) ? atoi(argv[2]) : DEFAULT_PORT;

    int s = connect_with_redirect(host, port);
    if (s < 0) return 1;

    char line[BUFSZ];
    for (;;) {
        printf("w26client$ ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;

        char raw[BUFSZ];
        strncpy(raw, line, sizeof(raw)-1); raw[sizeof(raw)-1]=0;
        size_t rl = strcspn(raw, "\n"); raw[rl] = 0;
        if (rl == 0) continue;

        /* FSM tokenize a working copy */
        char work[BUFSZ];
        strncpy(work, line, sizeof(work)-1); work[sizeof(work)-1]=0;
        char *tok[MAX_ARGS];
        int   tc = sm_tokenize(work, tok, MAX_ARGS);
        if (tc == 0) continue;

        validator_fn v = find_validator(tok[0]);
        if (!v) { printf("unknown command: %s\n", tok[0]); continue; }
        char emsg[128] = {0};
        if (!v(tok, tc, emsg, sizeof(emsg))) {
            printf("syntax error: %s\n", emsg);
            continue;
        }

        /* Send the RAW line (plus newline) — the server re-parses it */
        char wire[BUFSZ];
        int wl = snprintf(wire, sizeof(wire), "%s\n", raw);         /* safe formatted write */
        if (send_all(s, wire, wl) < 0) { perror("send"); break; }

        int r = read_response(s);
        if (r == 1) break;       /* BYE from server */
        if (r < 0) { printf("[client] connection lost\n"); break; }
    }

    close(s);
    return 0;                                                       /* signal success */
}
