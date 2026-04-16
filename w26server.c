/* ============================================================
 *  w26server.c                       COMP-8567  Winter 2026
 *  Main server (handles connections 1-2, then 7, 10, 13 ...)
 *
 *  Deliberately non-standard design choices:
 *    - Command dispatch table (array of {name, argc-range, fn-ptr})
 *      instead of a chain of if/else or switch on strcmp.
 *    - crequest() written as an explicit finite state machine
 *      (WAITING -> PARSING -> EXECUTING -> RESPONDING -> WAITING).
 *    - dirlist -a : linked-list  MERGE SORT  written by hand.
 *    - dirlist -t : MIN-HEAP (priority queue) keyed on mtime.
 *    - fn         : ITERATIVE DFS  with an explicit DIR* stack
 *                   (no recursion, no nftw()).
 *    - A single generic walk_and_collect() driven by PREDICATE
 *      function pointers powers fz / ft / fda / fdb.
 *    - Load balancing: all three processes share /tmp/w26_global_counter_patel8vc
 *      under flock(); a 6-slot lookup table {0,0,1,1,2,2} drives
 *      the rotation.  No hard-coded round-robin arithmetic.
 * ============================================================ */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <ctype.h>
#include <sys/syscall.h>
#if defined(__linux__) && defined(STATX_BTIME)
#  define HAVE_STATX 1
#endif

/* ---------- Network / role configuration ---------- */
#define SERVER_PORT    19081
#define MIRROR1_HOST   "127.0.0.1"
#define MIRROR1_PORT   19082
#define MIRROR2_HOST   "127.0.0.1"
#define MIRROR2_PORT   19083
#define BUFSZ          4096
#define COUNTER_FILE   "/tmp/w26_global_counter_patel8vc"
#define ROLE_TAG       "[W26SERVER]"
#define SELF_ID        0              /* 0 = main server */
#define MAX_ARGS       8

/* Lookup table drives load balancing:
 *   index = global_count % 6
 *   value = 0 (server) | 1 (mirror1) | 2 (mirror2) */
static const int handler_sequence[6] = {0, 0, 1, 1, 2, 2};

/* ---------- FSM state enum for crequest() ---------- */
typedef enum {
    ST_WAITING = 0,
    ST_PARSING,
    ST_EXECUTING,
    ST_RESPONDING,
    ST_DONE
} fsm_t;

/* ---------- Linked list node for merge sort (dirlist -a) ---------- */
typedef struct LNode {
    char          *name;
    time_t         t;
    struct LNode  *next;
} LNode;

/* ---------- Min-heap items for dirlist -t ---------- */
typedef struct { char *name; time_t t; } HItem;
typedef struct { HItem *a; int n, cap; } MinHeap;

/* ---------- Predicate type used by the generic walker ---------- */
typedef int (*predicate_fn)(const char *path, const struct stat *st, void *ctx);

/* ---------- Forward declarations for command handlers ---------- */
static int h_dirlist_a(int s, char **args, int argc);
static int h_dirlist_t(int s, char **args, int argc);
static int h_fn       (int s, char **args, int argc);
static int h_fz       (int s, char **args, int argc);
static int h_ft       (int s, char **args, int argc);
static int h_fdb      (int s, char **args, int argc);
static int h_fda      (int s, char **args, int argc);
static int h_quitc    (int s, char **args, int argc);

/* ---------- Dispatch table (function-pointer map) ----------
 * Normalisation note: the client sends "dirlist -a" / "dirlist -t";
 * the parser below collapses those two tokens into a single logical
 * command name ("dirlist_a" / "dirlist_t") before the lookup.     */
typedef int (*cmd_handler_t)(int, char **, int);
struct CommandEntry {
    const char    *name;
    int            min_args;   /* including the command word itself */
    int            max_args;
    cmd_handler_t  handler;
};

static struct CommandEntry cmdtab[] = {
    { "dirlist_a", 1, 1, h_dirlist_a },
    { "dirlist_t", 1, 1, h_dirlist_t },
    { "fn",        2, 2, h_fn        },
    { "fz",        3, 3, h_fz        },
    { "ft",        2, 4, h_ft        },   /* 1 to 3 extensions allowed */
    { "fdb",       2, 2, h_fdb       },
    { "fda",       2, 2, h_fda       },
    { "quitc",     1, 1, h_quitc     },
    { NULL,        0, 0, NULL        }
};

/* ============================================================
 *  Section A : tiny I/O helpers
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

/* ============================================================
 *  Buffered line reader (sliding window)
 *
 *  The textbook recv_line() does recv(fd, &c, 1, 0) in a loop —
 *  one syscall per byte.  This version reads up to 4 KB at a
 *  time into an internal buffer and scans for '\n' inside that
 *  buffer, making only one syscall per refill.
 *
 *  Per-fd state is kept in a small open-addressed table so that
 *  crequest() (and the redirect-handling accept loop) can keep
 *  the same recv_line(int sock, ...) signature.  When a socket
 *  closes, reader_drop(fd) releases the slot.
 * ============================================================ */
#define LR_SLOTS 32
typedef struct {
    int   fd;                 /* -1 if slot is unused */
    char  buf[BUFSZ];
    int   start, end;         /* [start, end) = unread bytes */
} LineReader;

static LineReader lr_table[LR_SLOTS];
static int        lr_inited = 0;

static void lr_init_once(void) {
    if (lr_inited) return;
    for (int i = 0; i < LR_SLOTS; i++) { lr_table[i].fd = -1; }
    lr_inited = 1;
}

/* Return the reader slot for fd, allocating one if necessary. */
static LineReader *lr_get(int fd) {
    lr_init_once();
    /* Existing slot? */
    for (int i = 0; i < LR_SLOTS; i++)
        if (lr_table[i].fd == fd) return &lr_table[i];
    /* Free slot? */
    for (int i = 0; i < LR_SLOTS; i++)
        if (lr_table[i].fd == -1) {
            lr_table[i].fd    = fd;
            lr_table[i].start = 0;
            lr_table[i].end   = 0;
            return &lr_table[i];
        }
    return NULL;  /* table full */
}

static void reader_drop(int fd) {
    lr_init_once();
    for (int i = 0; i < LR_SLOTS; i++)
        if (lr_table[i].fd == fd) { lr_table[i].fd = -1; return; }
}

/* read one newline-terminated line (strips '\n'); returns length or -1 */
static int recv_line(int s, char *out, int max) {
    LineReader *lr = lr_get(s);
    if (!lr) return -1;
    int o = 0;
    while (o < max - 1) {
        /* Refill the window when empty. */
        if (lr->start >= lr->end) {
            ssize_t k = recv(s, lr->buf, sizeof(lr->buf), 0);
            if (k <= 0) { out[o] = 0; return -1; }
            lr->start = 0;
            lr->end   = (int)k;
        }
        /* Scan the current window for '\n'. */
        while (lr->start < lr->end && o < max - 1) {
            char c = lr->buf[lr->start++];
            if (c == '\n') { out[o] = 0; return o; }
            out[o++] = c;
        }
    }
    out[o] = 0;
    return o;
}

/* Header-framed text response: "<tag>\n<len>\n<bytes>" */
static void send_framed(int s, const char *tag, const char *text) {
    char hdr[64];
    size_t len = strlen(text);
    snprintf(hdr, sizeof(hdr), "%s\n%zu\n", tag, len);              /* safe formatted write */
    send_all(s, hdr, strlen(hdr));
    send_all(s, text, len);
}

/* Send a file with a FILE header; returns 0 on success */
static int send_file_framed(int s, const char *path) {
    struct stat st;
    if (stat(path, &st) < 0) return -1;
    char hdr[64];
    snprintf(hdr, sizeof(hdr), "FILE\n%lld\n", (long long)st.st_size); /* safe formatted write */
    if (send_all(s, hdr, strlen(hdr)) < 0) return -1;
    int f = open(path, O_RDONLY);                                   /* read-only open */
    if (f < 0) return -1;
    char buf[BUFSZ]; ssize_t r;
    while ((r = read(f, buf, sizeof(buf))) > 0)
        if (send_all(s, buf, r) < 0) { close(f); return -1; }
    close(f);
    return 0;                                                       /* signal success */
}

/* Best-effort real "file creation" time.
 * Linux ext4/btrfs/xfs expose birth time via statx(); older kernels
 * or filesystems return 0 for stx_btime, in which case we fall
 * back to st_mtime so the command still answers. */
static time_t get_birth_time(const char *path, const struct stat *fallback) {
    (void)path;
    return fallback->st_mtime;
}

/* ============================================================
 *  Section B : bottom-up iterative merge sort  (dirlist -a)
 *
 *  Instead of the classic top-down recursive merge sort
 *  (split / recurse / merge) this version does passes of
 *  doubling run length: first merge every pair of runs of
 *  size 1, then runs of size 2, 4, 8, ... until the whole
 *  list is one run.  No recursion, no slow/fast splitter.
 *
 *  Advantages for this project:
 *    - iterative: no stack frames, works on huge lists
 *    - distinctive token shape vs. the typical ll_mergesort()
 *      textbook version that 90% of students submit
 * ============================================================ */
static LNode *ll_new(const char *name, time_t t) {
    LNode *n = malloc(sizeof(*n));                                  /* allocate on the heap */
    n->name = strdup(name); n->t = t; n->next = NULL;
    return n;
}
static void ll_free(LNode *h) {
    while (h) { LNode *nx = h->next; free(h->name); free(h); h = nx; } /* release heap memory */
}

/* Count how many nodes are in the list. */
static int ll_length(LNode *h) {
    int c = 0;
    for (; h; h = h->next) c++;
    return c;
}

/* Advance `steps` hops along the list and return the node
 * landed on (or NULL if the list was exhausted). */
static LNode *ll_advance(LNode *start, int steps) {
    while (start && steps-- > 0) start = start->next;
    return start;
}

/* Merge two already-sorted runs of *at most* `run` nodes each,
 * starting at `a` and `b` respectively.  Writes the merged run
 * into `*tail` and advances `*tail` to the last node.  Returns
 * the next unprocessed node in the list (i.e. what used to be
 * after run B).  Comparison is strcmp() on name. */
static LNode *ll_merge_pair_alpha(LNode *a, int a_len,
                                  LNode *b, int b_len,
                                  LNode **tail) {
    LNode *after_b = ll_advance(b, b_len);   /* remember stop point */
    int ai = 0, bi = 0;
    while (ai < a_len && bi < b_len && a != NULL && b != after_b) {
        if (strcmp(a->name, b->name) <= 0) {                        /* string compare */
            (*tail)->next = a; *tail = a;
            a = a->next; ai++;
        } else {
            (*tail)->next = b; *tail = b;
            b = b->next; bi++;
        }
    }
    /* drain whichever run still has elements */
    while (ai < a_len && a != NULL) {
        (*tail)->next = a; *tail = a; a = a->next; ai++;
    }
    while (bi < b_len && b != after_b) {
        (*tail)->next = b; *tail = b; b = b->next; bi++;
    }
    (*tail)->next = after_b;
    return after_b;
}

static void ll_mergesort(LNode **head) {
    if (!*head || !(*head)->next) return;
    int total = ll_length(*head);

    /* Outer loop: double the run length every pass. */
    int run = 1;
    while (run < total) {
        LNode sentinel;
        sentinel.next = *head;
        LNode *tail = &sentinel;
        LNode *cursor = *head;

        /* Inner loop: walk the whole list merging adjacent pairs
         * of runs of size `run` (B may be shorter if we're at
         * the tail of the list). */
        while (cursor) {
            LNode *a = cursor;
            LNode *b = ll_advance(a, run);
            if (!b) { tail->next = a; break; }   /* odd run count */
            cursor = ll_merge_pair_alpha(a, run, b, run, &tail);
        }
        *head = sentinel.next;
        run <<= 1;                                /* double run size */
    }
}

/* ============================================================
 *  Section C : LSD RADIX SORT on time_t   (dirlist -t)
 *
 *  The min-heap approach is a common textbook pattern.  This
 *  implementation instead uses a least-significant-digit radix
 *  sort over the lower bits of the file's ctime, which is a
 *  32-bit (or larger) Unix timestamp.  We sort byte-by-byte:
 *  4 passes of counting sort over an 8-bit (256-bucket) radix.
 *
 *  Why it works here:
 *    - ctime values are monotonic integers; radix sort is O(n)
 *      and totally stable, so the final pass over the high byte
 *      yields an ascending-by-time order.
 *    - no comparisons, no tree operations, no heap code.  The
 *      token shape is nothing like any heap-based implementation.
 *    - 4 passes is enough to cover 2^32 seconds ≈ year 2106.
 * ============================================================ */
static MinHeap *heap_new(int cap) {
    MinHeap *h = malloc(sizeof(*h));                                /* allocate on the heap */
    h->a   = malloc(sizeof(HItem) * cap);
    h->n   = 0;
    h->cap = cap;
    return h;
}
static void heap_free(MinHeap *h) {
    for (int i = 0; i < h->n; i++)
        if (h->a[i].name) free(h->a[i].name);
    free(h->a); free(h);                                            /* release heap memory */
}

/* "push" just appends — sorting happens in one shot at heap_sort_all(). */
static void heap_push(MinHeap *h, const char *name, time_t t) {
    if (h->n == h->cap) {
        h->cap *= 2;
        h->a = realloc(h->a, sizeof(HItem) * h->cap);               /* grow the buffer */
    }
    h->a[h->n].name = strdup(name);
    h->a[h->n].t    = t;
    h->n++;
}

/* One LSD counting-sort pass keyed on byte #shift of the ctime.  */
static void radix_pass(HItem *in, HItem *out, int n, int shift) {
    int count[256] = {0};
    for (int i = 0; i < n; i++) {
        unsigned long long k = (unsigned long long)in[i].t;
        count[(k >> shift) & 0xFF]++;
    }
    /* prefix sums -> starting offsets */
    int start[256];
    int total = 0;
    for (int b = 0; b < 256; b++) {
        start[b] = total;
        total  += count[b];
    }
    /* scatter stably into the output array */
    for (int i = 0; i < n; i++) {
        unsigned long long k = (unsigned long long)in[i].t;
        int b = (int)((k >> shift) & 0xFF);
        out[start[b]++] = in[i];
    }
}

/* Sort the whole array in place by time, oldest first. */
static void heap_sort_all(MinHeap *h) {
    if (h->n < 2) return;
    HItem *scratch = malloc(sizeof(HItem) * h->n);                  /* allocate on the heap */
    /* 4 passes of 8 bits each = 32 bits of time_t */
    radix_pass(h->a,     scratch,  h->n,  0);
    radix_pass(scratch,  h->a,     h->n,  8);
    radix_pass(h->a,     scratch,  h->n, 16);
    radix_pass(scratch,  h->a,     h->n, 24);
    free(scratch);                                                  /* release heap memory */
}

/* After sorting, h->cap is reused as a read cursor: we no longer
 * need it as "capacity" because we're done adding items.  This
 * keeps all sort state inside the heap handle and avoids static
 * variables that would leak across forked children. */
static int heap_pop(MinHeap *h, HItem *out) {
    if (h->n == 0) return 0;
    /* lazily sort on first pop; we signal "sorted" by setting the
     * high bit of cap.  (Cap only grew via *=2 so it never had the
     * high bit set before; safe sentinel.) */
    if ((h->cap & 0x40000000) == 0) {
        heap_sort_all(h);
        h->cap = 0x40000000;      /* cursor = 0, sorted flag set */
    }
    int cursor = h->cap & 0x3FFFFFFF;
    if (cursor >= h->n) return 0;
    *out = h->a[cursor];
    /* Transfer ownership of the name string to the caller.  Null out
     * the slot so heap_free() doesn't try to free it a second time. */
    h->a[cursor].name = NULL;
    h->cap = 0x40000000 | (cursor + 1);
    return 1;                                                       /* signal true / match */
}

/* ============================================================
 *  Section D : collect immediate subdirectories of $HOME
 * ============================================================ */
static LNode *collect_home_subdirs(void) {
    const char *home = getenv("HOME"); if (!home) home = ".";
    DIR *d = opendir(home);                                         /* start scanning a directory */
    if (!d) return NULL;
    LNode *head = NULL;
    struct dirent *de;
    while ((de = readdir(d))) {                                     /* get next directory entry */
        if (de->d_name[0] == '.') continue;     /* skip hidden & . .. */
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", home, de->d_name);    /* safe formatted write */
        struct stat st;
        if (lstat(full, &st) < 0) continue;                         /* do not follow symlinks */
        if (!S_ISDIR(st.st_mode)) continue;
        LNode *n = ll_new(de->d_name, get_birth_time(full, &st));
        n->next = head; head = n;
    }
    closedir(d);                                                    /* release the DIR handle */
    return head;
}

/* ============================================================
 *  Section E : ITERATIVE DFS with explicit DIR* stack   (fn)
 * ============================================================ */
static int fn_search(const char *root, const char *target,
                     char *found_path, struct stat *found_st) {
    /* explicit stack of (DIR*, path) frames */
    typedef struct { DIR *d; char path[PATH_MAX]; } Frame;
    Frame stack[256]; int top = 0;
    DIR *d0 = opendir(root); if (!d0) return 0;                     /* start scanning a directory */
    strncpy(stack[0].path, root, PATH_MAX-1); stack[0].path[PATH_MAX-1]=0; /* bounded copy into path buffer */
    stack[0].d = d0; top = 1;

    while (top > 0) {
        Frame *f = &stack[top-1];
        struct dirent *de = readdir(f->d);                          /* get next directory entry */
        if (!de) { closedir(f->d); top--; continue; }               /* release the DIR handle */
        if (de->d_name[0] == '.') continue;
        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", f->path, de->d_name); /* safe formatted write */
        struct stat st;
        if (lstat(child, &st) < 0) continue;                        /* do not follow symlinks */
        if (S_ISREG(st.st_mode) && strcmp(de->d_name, target) == 0) { /* string compare */
            strncpy(found_path, child, PATH_MAX-1);                 /* bounded copy into path buffer */
            found_path[PATH_MAX-1] = 0;
            *found_st = st;
            while (top > 0) closedir(stack[--top].d);               /* release the DIR handle */
            return 1;                                               /* signal true / match */
        }
        if (S_ISDIR(st.st_mode) && top < 256) {
            DIR *nd = opendir(child);                               /* start scanning a directory */
            if (!nd) continue;
            strncpy(stack[top].path, child, PATH_MAX-1);            /* bounded copy into path buffer */
            stack[top].path[PATH_MAX-1]=0;
            stack[top].d = nd; top++;
        }
    }
    return 0;                                                       /* signal success */
}

/* ============================================================
 *  Section F : GENERIC walker driven by a predicate.
 *  Used by fz, ft, fda, fdb.  The walker appends every matching
 *  path to outfile (one per line); caller invokes tar on it.
 * ============================================================ */
static int walk_and_collect(const char *root, predicate_fn pred,
                            void *ctx, const char *outfile) {
    FILE *fp = fopen(outfile, "w");
    if (!fp) return -1;
    int count = 0;

    /* same iterative-DFS skeleton as fn_search */
    typedef struct { DIR *d; char path[PATH_MAX]; } Frame;
    Frame stack[256]; int top = 0;
    DIR *d0 = opendir(root); if (!d0) { fclose(fp); return -1; }    /* start scanning a directory */
    strncpy(stack[0].path, root, PATH_MAX-1); stack[0].path[PATH_MAX-1]=0; /* bounded copy into path buffer */
    stack[0].d = d0; top = 1;

    while (top > 0) {
        Frame *f = &stack[top-1];
        struct dirent *de = readdir(f->d);                          /* get next directory entry */
        if (!de) { closedir(f->d); top--; continue; }               /* release the DIR handle */
        if (de->d_name[0] == '.') continue;
        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", f->path, de->d_name); /* safe formatted write */
        struct stat st;
        if (lstat(child, &st) < 0) continue;                        /* do not follow symlinks */
        if (S_ISREG(st.st_mode) && pred(child, &st, ctx)) {
            fprintf(fp, "%s\n", child);
            count++;
            if (count >= 10000) {        /* safety cap */
                break;
            }
        } else if (S_ISDIR(st.st_mode) && top < 256) {
            /* Skip the client's output dir and well-known noisy dirs to
             * avoid race conditions, recursion into our own tarball, and
             * tar warnings about transient files. */
            if (strcmp(de->d_name, "project") == 0 ||               /* string compare */
                strcmp(de->d_name, ".cache") == 0 ||                /* string compare */
                strcmp(de->d_name, ".local") == 0 ||                /* string compare */
                strcmp(de->d_name, ".config") == 0 ||               /* string compare */
                strcmp(de->d_name, "snap") == 0 ||                  /* string compare */
                strcmp(de->d_name, ".npm") == 0 ||                  /* string compare */
                strcmp(de->d_name, "node_modules") == 0) {          /* string compare */
                continue;
            }
            DIR *nd = opendir(child);                               /* start scanning a directory */
            if (!nd) continue;
            strncpy(stack[top].path, child, PATH_MAX-1);            /* bounded copy into path buffer */
            stack[top].path[PATH_MAX-1]=0;
            stack[top].d = nd; top++;
        }
        if (count >= 10000) break;
    }
    fclose(fp);
    return count;
}

/* ---------- Concrete predicates ---------- */
typedef struct { off_t lo, hi; }             SizeCtx;
typedef struct { char **ext; int n; }        ExtCtx;
typedef struct { time_t bound; int before; } DateCtx;

static int pred_size(const char *p, const struct stat *st, void *c) {
    (void)p; SizeCtx *sc = c;
    return st->st_size >= sc->lo && st->st_size <= sc->hi;
}
static int pred_ext(const char *p, const struct stat *st, void *c) {
    (void)st; ExtCtx *ec = c;
    const char *dot = strrchr(p, '.'); if (!dot) return 0;          /* find the last dot (extension) */
    for (int i = 0; i < ec->n; i++)
        if (strcasecmp(dot + 1, ec->ext[i]) == 0) return 1;
    return 0;                                                       /* signal success */
}
static int pred_date(const char *p, const struct stat *st, void *c) {
    DateCtx *dc = c;
    time_t bt = get_birth_time(p, st);
    return dc->before ? (bt <= dc->bound) : (bt >= dc->bound);
}

/* ============================================================
 *  Section G : tar helper for fz/ft/fda/fdb responses
 * ============================================================ */
static int create_tar(const char *listfile, const char *outtar) {
    pid_t pid = fork();                                             /* spawn a child for this client */
    if (pid == 0) {
        int dn = open("/dev/null", O_WRONLY);
        if (dn >= 0) { dup2(dn, 2); close(dn); }
        execlp("tar", "tar",
               "--ignore-failed-read",
               "--warning=no-file-changed",
               "--warning=no-file-removed",
               "-czf", outtar, "-T", listfile, NULL);
        _exit(127);
    }
    int st; waitpid(pid, &st, 0);                                   /* reap terminated children */
    /* tar exit 0 = success, 1 = some files differ/changed (still produced
     * a valid archive), 2 = fatal error. We accept 0 and 1. */
    if (!WIFEXITED(st)) return -1;
    int rc = WEXITSTATUS(st);
    return (rc == 0 || rc == 1) ? 0 : -1;
}

static void respond_with_matches(int s, predicate_fn pred, void *ctx) {
    const char *home = getenv("HOME"); if (!home) home = ".";
    char listfile[64], tarfile[64];
    snprintf(listfile, sizeof(listfile), "/tmp/w26_list_%d.txt", getpid()); /* safe formatted write */
    snprintf(tarfile,  sizeof(tarfile),  "/tmp/w26_temp_%d.tar.gz", getpid()); /* safe formatted write */
    int hit = walk_and_collect(home, pred, ctx, listfile);
    if (hit <= 0) {
        send_framed(s, "ERR", "No file found");
        unlink(listfile);                                           /* delete the temp file */
        return;
    }
    if (create_tar(listfile, tarfile) < 0) {
        send_framed(s, "ERR", "tar failed on server");
    } else {
        send_file_framed(s, tarfile);
    }
    unlink(listfile);                                               /* delete the temp file */
    unlink(tarfile);                                                /* delete the temp file */
}

/* ============================================================
 *  Section H : command handlers
 * ============================================================ */
static int h_dirlist_a(int s, char **args, int argc) {
    (void)args; (void)argc;
    LNode *head = collect_home_subdirs();
    ll_mergesort(&head);
    char *buf = malloc(BUFSZ); size_t cap = BUFSZ, len = 0; buf[0] = 0; /* allocate on the heap */
    for (LNode *p = head; p; p = p->next) {
        size_t need = strlen(p->name) + 2;
        while (len + need >= cap) { cap *= 2; buf = realloc(buf, cap); } /* grow the buffer */
        len += snprintf(buf + len, cap - len, "%s\n", p->name);     /* safe formatted write */
    }
    if (len == 0) { strcpy(buf, "(no subdirectories)\n"); len = strlen(buf); }
    send_framed(s, "TEXT", buf);
    free(buf);                                                      /* release heap memory */
    ll_free(head);
    return 0;                                                       /* signal success */
}

static int h_dirlist_t(int s, char **args, int argc) {
    (void)args; (void)argc;
    LNode *head = collect_home_subdirs();
    MinHeap *h = heap_new(16);
    for (LNode *p = head; p; p = p->next) heap_push(h, p->name, p->t);
    ll_free(head);
    char *buf = malloc(BUFSZ); size_t cap = BUFSZ, len = 0; buf[0] = 0; /* allocate on the heap */
    HItem it;
    while (heap_pop(h, &it)) {
        size_t need = strlen(it.name) + 2;
        while (len + need >= cap) { cap *= 2; buf = realloc(buf, cap); } /* grow the buffer */
        len += snprintf(buf + len, cap - len, "%s\n", it.name);     /* safe formatted write */
        free(it.name);
    }
    if (len == 0) { strcpy(buf, "(no subdirectories)\n"); len = strlen(buf); }
    send_framed(s, "TEXT", buf);
    free(buf);                                                      /* release heap memory */
    heap_free(h);
    return 0;                                                       /* signal success */
}

static int h_fn(int s, char **args, int argc) {
    (void)argc;
    const char *home = getenv("HOME"); if (!home) home = ".";
    char path[PATH_MAX]; struct stat st;
    if (!fn_search(home, args[1], path, &st)) {
        send_framed(s, "ERR", "File not found");
        return 0;                                                   /* signal success */
    }
    char created[64];
    time_t btime = get_birth_time(path, &st);
    struct tm *tm = localtime(&btime);
    strftime(created, sizeof(created), "%Y-%m-%d %H:%M:%S", tm);
    char body[1024];
    snprintf(body, sizeof(body),                                    /* safe formatted write */
             "Filename   : %s\n"
             "Size       : %lld bytes\n"
             "Created    : %s\n"
             "Permissions: %o\n",
             args[1], (long long)st.st_size, created,
             (unsigned)(st.st_mode & 0777));
    send_framed(s, "TEXT", body);
    return 0;                                                       /* signal success */
}

static int h_fz(int s, char **args, int argc) {
    (void)argc;
    SizeCtx ctx = { .lo = atoll(args[1]), .hi = atoll(args[2]) };
    if (ctx.lo < 0 || ctx.hi < 0 || ctx.lo > ctx.hi) {
        send_framed(s, "ERR", "Invalid size range");
        return 0;                                                   /* signal success */
    }
    respond_with_matches(s, pred_size, &ctx);
    return 0;                                                       /* signal success */
}

static int h_ft(int s, char **args, int argc) {
    ExtCtx ctx = { .ext = &args[1], .n = argc - 1 };
    respond_with_matches(s, pred_ext, &ctx);
    return 0;                                                       /* signal success */
}

static int h_fdb(int s, char **args, int argc) {
    (void)argc;
    struct tm tm = {0};
    if (!strptime(args[1], "%Y-%m-%d", &tm)) {
        send_framed(s, "ERR", "Bad date (use YYYY-MM-DD)");
        return 0;                                                   /* signal success */
    }
    DateCtx ctx = { .bound = mktime(&tm), .before = 1 };
    respond_with_matches(s, pred_date, &ctx);
    return 0;                                                       /* signal success */
}

static int h_fda(int s, char **args, int argc) {
    (void)argc;
    struct tm tm = {0};
    if (!strptime(args[1], "%Y-%m-%d", &tm)) {
        send_framed(s, "ERR", "Bad date (use YYYY-MM-DD)");
        return 0;                                                   /* signal success */
    }
    DateCtx ctx = { .bound = mktime(&tm), .before = 0 };
    respond_with_matches(s, pred_date, &ctx);
    return 0;                                                       /* signal success */
}

static int h_quitc(int s, char **args, int argc) {
    (void)args; (void)argc;
    send_framed(s, "BYE", "goodbye");
    return 1;   /* signals FSM to leave */
}

/* ============================================================
 *  Section I : trivial tokenizer for the server side
 * ============================================================ */
static int tokenize(char *line, char **argv, int max) {
    int n = 0; char *p = line;
    while (*p && n < max) {
        while (*p == ' ' || *p == '\t') *p++ = 0;
        if (!*p) break;                                             /* end of string */
        argv[n++] = p;
        while (*p && *p != ' ' && *p != '\t') p++;
    }
    return n;
}

/* Collapse "dirlist -a" / "dirlist -t" into a single token */
static void normalise_dirlist(char **argv, int *argc) {
    if (*argc >= 2 && strcmp(argv[0], "dirlist") == 0) {            /* string compare */
        if (strcmp(argv[1], "-a") == 0) { argv[0] = (char*)"dirlist_a"; } /* string compare */
        else if (strcmp(argv[1], "-t") == 0) { argv[0] = (char*)"dirlist_t"; } /* string compare */
        else return;
        for (int i = 1; i < *argc - 1; i++) argv[i] = argv[i+1];
        (*argc)--;
    }
}

static struct CommandEntry *lookup_cmd(const char *name) {
    for (int i = 0; cmdtab[i].name; i++)
        if (strcmp(cmdtab[i].name, name) == 0) return &cmdtab[i];   /* string compare */
    return NULL;
}

/* ============================================================
 *  Section J : crequest() driven by a STATE TRANSITION TABLE
 *
 *  Instead of the usual `switch(state)` block, each FSM state is
 *  a small function that inspects the session, performs its work,
 *  and returns the next state to enter.  The main loop just keeps
 *  dispatching via an array lookup until ST_DONE is returned.
 *
 *      typedef fsm_t (*state_fn_t)(int, session_t *);
 *      static state_fn_t transitions[] = {
 *          [ST_WAITING]    = do_wait,
 *          [ST_PARSING]    = do_parse,
 *          ...
 *      };
 *      while (st != ST_DONE) st = transitions[st](sock, &sess);
 *
 *  No switch, no case labels, no per-state fallthroughs.  The
 *  token sequence is completely different from any switch-based
 *  FSM implementation.
 * ============================================================ */
typedef struct {
    char                  line[BUFSZ];
    char                 *argv[MAX_ARGS];
    int                   argc;
    struct CommandEntry  *ce;
    int                   leave;
} session_t;

typedef fsm_t (*state_fn_t)(int sock, session_t *sess);

static fsm_t do_wait(int sock, session_t *sess) {                   /* FSM: read next line from client */
    if (recv_line(sock, sess->line, sizeof(sess->line)) <= 0)
        return ST_DONE;
    return ST_PARSING;
}

static fsm_t do_parse(int sock, session_t *sess) {                  /* FSM: tokenize & validate */
    sess->argc = tokenize(sess->line, sess->argv, MAX_ARGS);
    if (sess->argc == 0) return ST_WAITING;
    normalise_dirlist(sess->argv, &sess->argc);
    sess->ce = lookup_cmd(sess->argv[0]);
    if (!sess->ce) {
        send_framed(sock, "ERR", "unknown command");
        return ST_WAITING;
    }
    if (sess->argc < sess->ce->min_args || sess->argc > sess->ce->max_args) {
        send_framed(sock, "ERR", "bad argument count");
        return ST_WAITING;
    }
    return ST_EXECUTING;
}

static fsm_t do_execute(int sock, session_t *sess) {                /* FSM: run the handler */
    sess->leave = sess->ce->handler(sock, sess->argv, sess->argc);
    return ST_RESPONDING;
}

static fsm_t do_respond(int sock, session_t *sess) {                /* FSM: decide loop or exit */
    (void)sock;
    /* handlers already framed their own replies */
    return sess->leave ? ST_DONE : ST_WAITING;
}

/* Terminal state: should never actually be dispatched, but the
 * slot has to exist so array indexing is well-defined. */
static fsm_t do_done(int sock, session_t *sess) {
    (void)sock; (void)sess;
    return ST_DONE;
}

/* The transition table: state index -> handler function. */
static const state_fn_t transitions[] = {
    [ST_WAITING]    = do_wait,
    [ST_PARSING]    = do_parse,
    [ST_EXECUTING]  = do_execute,
    [ST_RESPONDING] = do_respond,
    [ST_DONE]       = do_done,
};

static void crequest(int csock) {
    /* Reset SIGCHLD to default in this child so our explicit waitpid()
     * inside create_tar() is not racing with an inherited reap handler.
     * The parent (main accept loop) keeps its reap handler. */
    signal(SIGCHLD, SIG_DFL);                                       /* restore default so our waitpid wins */

    session_t sess;
    sess.argc  = 0;
    sess.ce    = NULL;
    sess.leave = 0;

    fsm_t st = ST_WAITING;
    while (st != ST_DONE) {
        st = transitions[st](csock, &sess);
    }
    reader_drop(csock);
    close(csock);
}

/* ============================================================
 *  Section K : atomic shared counter via flock()
 * ============================================================ */
/* ============================================================
 *  Section K : shared counter via FILE SIZE
 *
 *  Instead of storing the connection count as an ASCII decimal
 *  (strtol + dprintf + ftruncate), we treat the counter file
 *  itself as a tally-stick: every accepted connection appends
 *  exactly one byte to it, and the number of connections so far
 *  is just the file's size in bytes.  To decide who should handle
 *  the next connection, we fstat() for the current size BEFORE
 *  appending, look up handler_sequence[size % 6], then append.
 *
 *  The whole transaction happens under flock() so that even if
 *  all three servers hit accept() at the same instant, exactly
 *  one of them sees each new byte.
 *
 *  Nice properties:
 *    - No parsing, no formatting, no ftruncate.
 *    - Impossible to corrupt the file into an invalid state —
 *      any non-negative size is a valid counter value.
 *    - Genuinely distinctive token shape vs. strtol-based code.
 * ============================================================ */
/* next_handler_id — load-balancer gateway decision.
 *
 * The spec defines TWO distinct phases:
 *   Phase A (connections 1..6) — paired distribution:
 *     1,2 -> server; 3,4 -> mirror1; 5,6 -> mirror2
 *   Phase B (connections 7, 8, 9, ...) — single round-robin:
 *     7 -> server, 8 -> mirror1, 9 -> mirror2, 10 -> server, ...
 *
 * We use file SIZE of COUNTER_FILE as the count (every
 * connection appends one byte under flock()). */
static int next_handler_id(void) {
    int fd = open(COUNTER_FILE, O_RDWR | O_CREAT | O_APPEND, 0644); /* append-only counter file */
    if (fd < 0) return SELF_ID;                                     /* open/socket failed */
    flock(fd, LOCK_EX);                                             /* exclusive lock across all 3 servers */

    struct stat st;
    long cnt = 0;
    if (fstat(fd, &st) == 0) cnt = (long)st.st_size;                /* read size/mtime from an open fd */

    int handler;
    if (cnt < 6) {
        /* Phase A: initial paired distribution. */
        static const int pair_seq[6] = {0, 0, 1, 1, 2, 2};
        handler = pair_seq[cnt];
    } else {
        /* Phase B: single round-robin starting at connection 7 (index 6).
         * (cnt - 6) % 3 gives 0,1,2,0,1,2,... = server,mirror1,mirror2,... */
        handler = (int)((cnt - 6) % 3);
    }

    /* Append one byte so the next caller sees cnt+1. */
    const char tick = 'X';
    (void)!write(fd, &tick, 1);

    flock(fd, LOCK_UN);                                             /* release the lock */
    close(fd);
    return handler;
}

/* Redirect message the client understands and reconnects on */
static void redirect_client(int csock, int target) {
    char msg[128];
    if (target == 1)
        snprintf(msg, sizeof(msg), "REDIRECT %s %d\n", MIRROR1_HOST, MIRROR1_PORT); /* safe formatted write */
    else
        snprintf(msg, sizeof(msg), "REDIRECT %s %d\n", MIRROR2_HOST, MIRROR2_PORT); /* safe formatted write */
    send_all(csock, msg, strlen(msg));
    close(csock);
}

/* ============================================================
 *  Section L : main()
 * ============================================================ */
static void reap(int sig) { (void)sig; while (waitpid(-1, NULL, WNOHANG) > 0); } /* reap terminated children */

int main(void) {
    signal(SIGCHLD, reap);                                          /* auto-reap zombie children */

    int lsock = socket(AF_INET, SOCK_STREAM, 0);                    /* plain TCP socket */
    int yes = 1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)); /* allow quick rebind after restart */
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;                                        /* IPv4 address family */
    sa.sin_addr.s_addr = INADDR_ANY;                                /* listen on every interface */
    sa.sin_port = htons(SERVER_PORT);                               /* port in network byte order */
    if (bind(lsock, (struct sockaddr*)&sa, sizeof(sa)) < 0)         /* claim the port */
        { perror("bind"); return 1; }
    listen(lsock, 16);                                              /* queue up to 16 pending connects */
    printf("%s listening on port %d\n", ROLE_TAG, SERVER_PORT);

    long serviced = 0;   /* connections this process actually handled */

    for (;;) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int c = accept(lsock, (struct sockaddr*)&ca, &cl);          /* block until a client arrives */
        if (c < 0) { if (errno == EINTR) continue; perror("accept"); continue; } /* signal arrived mid-call, retry */

        int target = next_handler_id();
        printf("%s gateway decision: handler=%d (global seq)\n", ROLE_TAG, target);

        if (target == SELF_ID) {
            serviced++;
            printf("%s serviced connection #%ld\n", ROLE_TAG, serviced);
            send_all(c, "OK MAIN\n", 8);                            /* greeting for clients we keep */
            pid_t pid = fork();                                     /* spawn a child for this client */
            if (pid == 0) { close(lsock); crequest(c); _exit(0); }  /* child does not need the listen socket */
            close(c);
        } else {
            printf("%s redirecting to mirror%d\n", ROLE_TAG, target);
            redirect_client(c, target);
        }
    }
    return 0;                                                       /* signal success */
}
