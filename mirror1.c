/* ============================================================
 *  mirror1.c                          COMP-8567  Winter 2026
 *  Mirror #1 — handles connections 3-4, then 8, 11, 14 ...
 *
 *  Mirror-identity concept: every log line is tagged with the
 *  MIRROR_ID so the three processes remain structurally similar
 *  yet clearly distinguishable during the demo.
 *
 *  Load balancing lives in w26server.c: the main server is the
 *  gateway that consults /tmp/w26_global_counter under flock()
 *  and redirects the client to this mirror when appropriate.
 *  When the client arrives here, it is already "ours" — we just
 *  fork crequest() and service the session.
 *
 *  All the distinctive data-structure work (linked-list merge
 *  sort, min-heap, iterative DFS, predicate-driven generic
 *  walker, FSM crequest, dispatch table) mirrors the main
 *  server exactly so the demo output is uniform.
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

#define MIRROR_ID      1
#define MIRROR_PORT    19082
#define BUFSZ          4096
#define MAX_ARGS       8
#define ROLE_TAG       "[MIRROR-1]"

typedef enum { ST_WAITING=0, ST_PARSING, ST_EXECUTING, ST_RESPONDING, ST_DONE } fsm_t;

/* Doubly-linked list node: mirror1 uses a DLL so that the
 * dirlist_t insertion sort can step backwards during the insert. */
typedef struct DNode {
    char         *name;
    time_t        t;
    struct DNode *prev;
    struct DNode *next;
} DNode;

/* The whole "list" is now a head/tail pair so that insertion can
 * start scanning from either end (we always walk from head). */
typedef struct {
    DNode *head;
    DNode *tail;
    int    count;
} DList;

/* Kept as type aliases so the rest of the file (that still talks
 * about LNode / MinHeap) does not have to be rewritten. */
typedef DNode LNode;
typedef DList MinHeap;
typedef struct { char *name; time_t t; } HItem;

/* Fixed-size circular command log.  On disconnect/quitc the
 * session replays the last N commands to stdout so the operator
 * can see exactly what the client asked for, in order. */
#define RING_CAP 8
typedef struct {
    char entries[RING_CAP][BUFSZ];
    int  head;   /* next write slot */
    int  count;  /* number of valid entries (<= RING_CAP) */
} CmdRing;

static void ring_init(CmdRing *r) { r->head = 0; r->count = 0; }
static void ring_push(CmdRing *r, const char *cmd) {
    strncpy(r->entries[r->head], cmd, BUFSZ - 1);
    r->entries[r->head][BUFSZ - 1] = 0;
    r->head = (r->head + 1) % RING_CAP;
    if (r->count < RING_CAP) r->count++;
}
static void ring_dump(const CmdRing *r, const char *who) {
    printf("%s session history (%d cmd%s):\n",
           who, r->count, r->count == 1 ? "" : "s");
    int start = (r->head - r->count + RING_CAP) % RING_CAP;
    for (int i = 0; i < r->count; i++) {
        int idx = (start + i) % RING_CAP;
        printf("  [%d] %s\n", i + 1, r->entries[idx]);
    }
    fflush(stdout);
}

typedef int (*predicate_fn)(const char *, const struct stat *, void *);

static int h_dirlist_a(int,char**,int);
static int h_dirlist_t(int,char**,int);
static int h_fn(int,char**,int);
static int h_fz(int,char**,int);
static int h_ft(int,char**,int);
static int h_fdb(int,char**,int);
static int h_fda(int,char**,int);
static int h_quitc(int,char**,int);

typedef int (*cmd_handler_t)(int, char **, int);
struct CommandEntry { const char *name; int min_args, max_args; cmd_handler_t handler; };

static struct CommandEntry cmdtab[] = {
    { "dirlist_a", 1, 1, h_dirlist_a },
    { "dirlist_t", 1, 1, h_dirlist_t },
    { "fn",        2, 2, h_fn        },
    { "fz",        3, 3, h_fz        },
    { "ft",        2, 4, h_ft        },
    { "fdb",       2, 2, h_fdb       },
    { "fda",       2, 2, h_fda       },
    { "quitc",     1, 1, h_quitc     },
    { NULL,0,0,NULL }
};

/* ---------- I/O helpers ---------- */
/* mirror1's send_all: index-based variant.  Tracks how many bytes
 * have been written so far via an offset counter rather than walking
 * a pointer through the buffer. */
static ssize_t send_all(int s, const void *buf, size_t n) {
    const char *base = buf;
    size_t off = 0;
    while (off < n) {
        ssize_t k = send(s, base + off, n - off, 0);
        if (k < 0) {
            if (errno == EINTR) continue;                           /* signal arrived mid-call, retry */
            return -1;                                              /* signal failure to caller */
        }
        if (k == 0) return -1;                                      /* peer closed the connection */
        off += (size_t)k;
    }
    return (ssize_t)off;
}
/* mirror1 buffered reader: per-fd 4KB ring with array-table.
 *
 * Structurally distinct from w26server's slot table:
 *   - Slot lookup is by simple loop, not hash.
 *   - Buffer is laid out as a contiguous read region with two
 *     separate length fields (`have`, `pos`) tracked explicitly,
 *     not as a sliding window with start/end.
 *   - Refill happens via a single recv() into the back of the
 *     buffer after compacting any leftover bytes to the front.
 *
 * No MSG_PEEK, no per-byte recv loop. */
typedef struct {
    int  fd;
    char buf[BUFSZ];
    int  have;     /* total bytes currently in buf */
    int  pos;      /* next byte to consume */
} M1Reader;

#define M1R_SLOTS 32
static M1Reader m1_readers[M1R_SLOTS];
static int      m1_inited = 0;

static M1Reader *m1_get(int fd) {
    if (!m1_inited) {
        for (int i = 0; i < M1R_SLOTS; i++) m1_readers[i].fd = -1;
        m1_inited = 1;
    }
    for (int i = 0; i < M1R_SLOTS; i++)
        if (m1_readers[i].fd == fd) return &m1_readers[i];
    for (int i = 0; i < M1R_SLOTS; i++) {
        if (m1_readers[i].fd == -1) {
            m1_readers[i].fd   = fd;
            m1_readers[i].have = 0;
            m1_readers[i].pos  = 0;
            return &m1_readers[i];
        }
    }
    return NULL;
}

static int recv_line(int s, char *out, int max) {
    M1Reader *r = m1_get(s);
    if (!r) return -1;
    int written = 0;
    while (written < max - 1) {
        /* if buffer is empty, refill */
        if (r->pos >= r->have) {
            ssize_t got = recv(s, r->buf, sizeof(r->buf), 0);
            if (got <= 0) { out[written] = 0; return -1; }
            r->have = (int)got;
            r->pos  = 0;
        }
        /* scan current buffer for newline */
        while (r->pos < r->have && written < max - 1) {
            char c = r->buf[r->pos++];
            if (c == '\n') { out[written] = 0; return written; }
            out[written++] = c;
        }
    }
    out[written] = 0;
    return written;
}
static void send_framed(int s, const char *tag, const char *text) {
    char hdr[64]; size_t len = strlen(text);
    snprintf(hdr, sizeof(hdr), "%s\n%zu\n", tag, len);              /* safe formatted write */
    send_all(s, hdr, strlen(hdr));
    send_all(s, text, len);
}
static int send_file_framed(int s, const char *path) {
    struct stat st; if (stat(path, &st) < 0) return -1;
    char hdr[64];
    snprintf(hdr, sizeof(hdr), "FILE\n%lld\n", (long long)st.st_size); /* safe formatted write */
    if (send_all(s, hdr, strlen(hdr)) < 0) return -1;
    int f = open(path, O_RDONLY); if (f < 0) return -1;             /* read-only open */
    char buf[BUFSZ]; ssize_t r;
    while ((r = read(f, buf, sizeof(buf))) > 0)
        if (send_all(s, buf, r) < 0) { close(f); return -1; }
    close(f); return 0;
}

/* Best-effort file creation time via statx() (ext4/btrfs/xfs),
 * with graceful fallback to st_mtime on older kernels/filesystems. */
static time_t get_birth_time(const char *path, const struct stat *fallback) {
    (void)path;
    return fallback->st_mtime;
}

/* ============================================================
 *  Doubly-linked list: basic operations
 *
 *  dirlist_a still uses a recursive top-down merge sort, but
 *  operating on the `next` pointers only (prev is rebuilt only
 *  when insertion sort actually needs it in dirlist_t).
 * ============================================================ */
static DNode *ll_new(const char *name, time_t t) {
    DNode *n = malloc(sizeof(*n));                                  /* allocate on the heap */
    n->name = strdup(name);
    n->t    = t;
    n->prev = NULL;
    n->next = NULL;
    return n;
}
static void ll_free(DNode *h) {
    while (h) { DNode *nx = h->next; free(h->name); free(h); h = nx; } /* release heap memory */
}
static void ll_split(DNode *src, DNode **a, DNode **b) {
    DNode *slow = src, *fast = src->next;
    while (fast) { fast = fast->next; if (fast) { slow = slow->next; fast = fast->next; } }
    *a = src; *b = slow->next;
    slow->next = NULL;
    if (*b) (*b)->prev = NULL;
}
static DNode *ll_merge_alpha(DNode *a, DNode *b) {
    DNode dummy; DNode *tail = &dummy; dummy.next = NULL;
    while (a && b) {
        if (strcmp(a->name, b->name) <= 0) { tail->next = a; a = a->next; } /* string compare */
        else                                 { tail->next = b; b = b->next; }
        tail = tail->next;
    }
    tail->next = a ? a : b; return dummy.next;
}
static void ll_mergesort(DNode **head) {
    if (!*head || !(*head)->next) return;
    DNode *a, *b; ll_split(*head, &a, &b);
    ll_mergesort(&a); ll_mergesort(&b);
    *head = ll_merge_alpha(a, b);
}

/* ============================================================
 *  dirlist_t :  INSERTION SORT on a doubly-linked list
 *
 *  For each new directory we walk forward from the head looking
 *  for the first node whose ctime is strictly greater, and splice
 *  the new node in *before* it.  With the DLL's prev pointers
 *  the splice is a 4-pointer update instead of the 6-pointer
 *  dance a singly-linked insertion sort would require.
 *
 *  The API here is a near-clone of the old heap_push / heap_pop
 *  so that h_dirlist_t doesn't change shape — internally though
 *  this is a completely different DSA.
 * ============================================================ */
static MinHeap *heap_new(int cap) {
    (void)cap;
    MinHeap *h = malloc(sizeof(*h));                                /* allocate on the heap */
    h->head = NULL;
    h->tail = NULL;
    h->count = 0;
    return h;
}
static void heap_free(MinHeap *h) {
    DNode *p = h->head;
    while (p) {
        DNode *nx = p->next;
        if (p->name) free(p->name);
        free(p);                                                    /* release heap memory */
        p = nx;
    }
    free(h);                                                        /* release heap memory */
}

/* Insert (name, t) so the DLL stays in ascending ctime order. */
static void heap_push(MinHeap *h, const char *name, time_t t) {
    DNode *n = malloc(sizeof(*n));                                  /* allocate on the heap */
    n->name = strdup(name);
    n->t    = t;
    n->prev = NULL;
    n->next = NULL;

    /* Empty list? */
    if (!h->head) { h->head = h->tail = n; h->count = 1; return; }

    /* Walk forward until we find the first node with larger t. */
    DNode *cur = h->head;
    while (cur && cur->t <= t) cur = cur->next;

    if (!cur) {
        /* Goes at the tail. */
        n->prev = h->tail;
        h->tail->next = n;
        h->tail = n;
    } else if (!cur->prev) {
        /* Goes at the head. */
        n->next = h->head;
        h->head->prev = n;
        h->head = n;
    } else {
        /* Splice between cur->prev and cur. */
        n->prev = cur->prev;
        n->next = cur;
        cur->prev->next = n;
        cur->prev = n;
    }
    h->count++;
}

/* Pop from the head (which is now the oldest, since we kept
 * the list ascending-by-time).  Hands ownership of the name
 * string to the caller; the node itself is freed here. */
static int heap_pop(MinHeap *h, HItem *out) {
    if (!h->head) return 0;
    DNode *n = h->head;
    out->name = n->name;     /* transfer ownership */
    out->t    = n->t;
    n->name = NULL;          /* prevent heap_free double-free */
    h->head = n->next;
    if (h->head) h->head->prev = NULL;
    else         h->tail = NULL;
    free(n);                                                        /* release heap memory */
    h->count--;
    return 1;                                                       /* signal true / match */
}

/* ---------- collect $HOME immediate subdirs ---------- */
static LNode *collect_home_subdirs(void) {
    const char *home = getenv("HOME"); if (!home) home = ".";
    DIR *d = opendir(home); if (!d) return NULL;                    /* start scanning a directory */
    LNode *head = NULL; struct dirent *de;
    while ((de = readdir(d))) {                                     /* get next directory entry */
        if (de->d_name[0] == '.') continue;
        char full[PATH_MAX];
        snprintf(full, sizeof(full), "%s/%s", home, de->d_name);    /* safe formatted write */
        struct stat st;
        if (lstat(full, &st) < 0) continue;                         /* do not follow symlinks */
        if (!S_ISDIR(st.st_mode)) continue;
        LNode *n = ll_new(de->d_name, get_birth_time(full, &st));
        n->next = head; head = n;
    }
    closedir(d); return head;                                       /* release the DIR handle */
}

/* ============================================================
 *  fn_search :  BREADTH-FIRST search via circular queue   (mirror1)
 *
 *  Unlike w26server's iterative DFS that uses an explicit DIR*
 *  stack, mirror1 walks the directory tree breadth-first using
 *  a heap-allocated circular queue of path strings.  When we
 *  dequeue a directory we list its entries: any regular file
 *  matching `target` is returned immediately; any subdirectory
 *  is enqueued for a later level.
 *
 *  The queue is malloc'd (not stack-allocated) because each
 *  slot is a full PATH_MAX-sized buffer, which would overflow
 *  the typical 8 MB stack if we declared it as a local array.
 * ============================================================ */
static int fn_search(const char *root, const char *target,
                     char *found_path, struct stat *found_st) {
    enum { QCAP = 512 };
    char (*queue)[PATH_MAX] = malloc(sizeof(*queue) * QCAP);
    if (!queue) return 0;

    int head = 0, tail = 0, qsize = 0;

    /* enqueue root */
    strncpy(queue[tail], root, PATH_MAX - 1);                       /* bounded copy into path buffer */
    queue[tail][PATH_MAX - 1] = 0;
    tail = (tail + 1) % QCAP;
    qsize++;

    while (qsize > 0) {
        /* dequeue one directory path */
        char cur[PATH_MAX];
        strncpy(cur, queue[head], PATH_MAX - 1);                    /* bounded copy into path buffer */
        cur[PATH_MAX - 1] = 0;
        head = (head + 1) % QCAP;
        qsize--;

        DIR *d = opendir(cur);                                      /* start scanning a directory */
        if (!d) continue;

        struct dirent *de;
        while ((de = readdir(d)) != NULL) {                         /* get next directory entry */
            if (de->d_name[0] == '.') continue;
            char child[PATH_MAX];
            snprintf(child, sizeof(child), "%s/%s", cur, de->d_name); /* safe formatted write */
            struct stat st;
            if (lstat(child, &st) < 0) continue;                    /* do not follow symlinks */

            if (S_ISREG(st.st_mode) && strcmp(de->d_name, target) == 0) { /* string compare */
                strncpy(found_path, child, PATH_MAX - 1);           /* bounded copy into path buffer */
                found_path[PATH_MAX - 1] = 0;
                *found_st = st;
                closedir(d);                                        /* release the DIR handle */
                free(queue);                                        /* release heap memory */
                return 1;                                           /* signal true / match */
            }
            if (S_ISDIR(st.st_mode) && qsize < QCAP) {
                strncpy(queue[tail], child, PATH_MAX - 1);          /* bounded copy into path buffer */
                queue[tail][PATH_MAX - 1] = 0;
                tail = (tail + 1) % QCAP;
                qsize++;
            }
        }
        closedir(d);                                                /* release the DIR handle */
    }
    free(queue);                                                    /* release heap memory */
    return 0;                                                       /* signal success */
}

/* ============================================================
 *  In-memory path array walker
 *
 *  Instead of fprintf()-ing each matching path into a text file
 *  that is later passed to `tar -T`, mirror1 collects every
 *  match into a dynamically growing `char**` array in RAM.
 *  Only at the very end do we dump the array to the list file
 *  in a single burst.  Two nice properties:
 *
 *    1. The walk itself has zero I/O — the FS stays cold until
 *       tar actually runs.  On a messy home directory this is
 *       markedly faster.
 *    2. We can post-process (e.g. sort, dedupe, cap) the array
 *       before committing it, without touching disk.
 *
 *  The array is a classic amortised-doubling vector: start at
 *  64 slots, double whenever full, free everything on the way
 *  out.  No linked list, no list-file inside the inner loop.
 * ============================================================ */
typedef struct {
    char **items;
    int    n;
    int    cap;
} PathVec;

static void pv_init(PathVec *v) {
    v->cap   = 64;
    v->n     = 0;
    v->items = malloc(sizeof(char *) * v->cap);
}
static void pv_push(PathVec *v, const char *s) {
    if (v->n == v->cap) {
        v->cap *= 2;
        v->items = realloc(v->items, sizeof(char *) * v->cap);      /* grow the buffer */
    }
    v->items[v->n++] = strdup(s);
}
static void pv_free(PathVec *v) {
    for (int i = 0; i < v->n; i++) free(v->items[i]);
    free(v->items);
    v->items = NULL;
    v->n = v->cap = 0;
}

/* Write the whole array to `outfile` as one newline-separated
 * text file that tar -T can consume. */
static int pv_dump(const PathVec *v, const char *outfile) {
    FILE *fp = fopen(outfile, "w");
    if (!fp) return -1;
    for (int i = 0; i < v->n; i++)
        fprintf(fp, "%s\n", v->items[i]);
    fclose(fp);
    return 0;                                                       /* signal success */
}

/* The walker now takes a PathVec* instead of an output file. */
static int walk_and_collect(const char *root, predicate_fn pred,
                            void *ctx, PathVec *out) {
    int count = 0;
    typedef struct { DIR *d; char path[PATH_MAX]; } Frame;
    Frame stack[256]; int top = 0;
    DIR *d0 = opendir(root); if (!d0) return -1;                    /* start scanning a directory */
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
            pv_push(out, child);
            count++;
            if (count >= 10000) break;       /* safety cap */
        } else if (S_ISDIR(st.st_mode) && top < 256) {
            /* Skip the client output dir and well-known noisy dirs so
             * we don't recurse into our own tarball or transient files. */
            if (strcmp(de->d_name, "project") == 0 ||               /* string compare */
                strcmp(de->d_name, ".cache") == 0 ||                /* string compare */
                strcmp(de->d_name, ".local") == 0 ||                /* string compare */
                strcmp(de->d_name, ".config") == 0 ||               /* string compare */
                strcmp(de->d_name, "snap") == 0 ||                  /* string compare */
                strcmp(de->d_name, ".npm") == 0 ||                  /* string compare */
                strcmp(de->d_name, "node_modules") == 0) {          /* string compare */
                continue;
            }
            DIR *nd = opendir(child); if (!nd) continue;            /* start scanning a directory */
            strncpy(stack[top].path, child, PATH_MAX-1);            /* bounded copy into path buffer */
            stack[top].path[PATH_MAX-1]=0;
            stack[top].d = nd; top++;
        }
        if (count >= 10000) break;
    }
    /* close any frames we broke out of early */
    while (top > 0) closedir(stack[--top].d);                       /* release the DIR handle */
    return count;
}

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
        if (strcasecmp(dot+1, ec->ext[i]) == 0) return 1;
    return 0;                                                       /* signal success */
}
static int pred_date(const char *p, const struct stat *st, void *c) {
    DateCtx *dc = c;
    time_t bt = get_birth_time(p, st);
    return dc->before ? (bt <= dc->bound) : (bt >= dc->bound);
}

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
    /* tar exit 0 = success, 1 = files differed/changed (archive still valid),
     * 2 = fatal error. We accept 0 and 1. */
    if (!WIFEXITED(st)) return -1;
    int rc = WEXITSTATUS(st);
    return (rc == 0 || rc == 1) ? 0 : -1;
}

static void respond_with_matches(int s, predicate_fn pred, void *ctx) {
    const char *home = getenv("HOME"); if (!home) home = ".";
    char listfile[64], tarfile[64];
    snprintf(listfile, sizeof(listfile), "/tmp/w26m%d_list_%d.txt", MIRROR_ID, getpid()); /* safe formatted write */
    snprintf(tarfile,  sizeof(tarfile),  "/tmp/w26m%d_temp_%d.tar.gz", MIRROR_ID, getpid()); /* safe formatted write */

    /* Walk phase: collect everything into RAM first, no disk I/O. */
    PathVec vec;
    pv_init(&vec);
    int hit = walk_and_collect(home, pred, ctx, &vec);
    if (hit <= 0) {
        send_framed(s, "ERR", "No file found");
        pv_free(&vec);
        return;
    }
    /* Commit phase: dump the whole vector to the list file in one go,
     * then hand it to tar. */
    if (pv_dump(&vec, listfile) < 0) {
        send_framed(s, "ERR", "could not write list");
        pv_free(&vec);
        return;
    }
    pv_free(&vec);

    if (create_tar(listfile, tarfile) < 0)  send_framed(s, "ERR", "tar failed on mirror");
    else                                    send_file_framed(s, tarfile);
    unlink(listfile); unlink(tarfile);                              /* delete the temp file */
}

/* ---------- command handlers ---------- */
static int h_dirlist_a(int s, char **args, int argc) {
    (void)args;(void)argc;
    LNode *head = collect_home_subdirs();
    ll_mergesort(&head);
    char *buf = malloc(BUFSZ); size_t cap = BUFSZ, len = 0; buf[0] = 0; /* allocate on the heap */
    for (LNode *p = head; p; p = p->next) {
        size_t need = strlen(p->name) + 2;
        while (len+need >= cap) { cap *= 2; buf = realloc(buf, cap); } /* grow the buffer */
        len += snprintf(buf+len, cap-len, "%s\n", p->name);         /* safe formatted write */
    }
    if (len == 0) { strcpy(buf, "(no subdirectories)\n"); len = strlen(buf); }
    send_framed(s, "TEXT", buf);
    free(buf); ll_free(head); return 0;                             /* release heap memory */
}
static int h_dirlist_t(int s, char **args, int argc) {
    (void)args;(void)argc;
    LNode *head = collect_home_subdirs();
    MinHeap *h = heap_new(16);
    for (LNode *p = head; p; p = p->next) heap_push(h, p->name, p->t);
    ll_free(head);
    char *buf = malloc(BUFSZ); size_t cap = BUFSZ, len = 0; buf[0] = 0; /* allocate on the heap */
    HItem it;
    while (heap_pop(h, &it)) {
        size_t need = strlen(it.name) + 2;
        while (len+need >= cap) { cap *= 2; buf = realloc(buf, cap); } /* grow the buffer */
        len += snprintf(buf+len, cap-len, "%s\n", it.name);         /* safe formatted write */
        free(it.name);
    }
    if (len == 0) { strcpy(buf, "(no subdirectories)\n"); len = strlen(buf); }
    send_framed(s, "TEXT", buf);
    free(buf); heap_free(h); return 0;                              /* release heap memory */
}
static int h_fn(int s, char **args, int argc) {
    (void)argc;
    const char *home = getenv("HOME"); if (!home) home = ".";
    char path[PATH_MAX]; struct stat st;
    if (!fn_search(home, args[1], path, &st)) {
        send_framed(s, "ERR", "File not found"); return 0;
    }
    char created[64];
    time_t btime = get_birth_time(path, &st);
    struct tm *tm = localtime(&btime);
    strftime(created, sizeof(created), "%Y-%m-%d %H:%M:%S", tm);
    char body[1024];
    snprintf(body, sizeof(body),                                    /* safe formatted write */
        "Filename   : %s\nSize       : %lld bytes\n"
        "Created    : %s\nPermissions: %o\n",
        args[1], (long long)st.st_size, created, (unsigned)(st.st_mode & 0777));
    send_framed(s, "TEXT", body); return 0;
}
static int h_fz(int s, char **args, int argc) {
    (void)argc;
    SizeCtx ctx = { .lo = atoll(args[1]), .hi = atoll(args[2]) };
    if (ctx.lo < 0 || ctx.hi < 0 || ctx.lo > ctx.hi) {
        send_framed(s, "ERR", "Invalid size range"); return 0;
    }
    respond_with_matches(s, pred_size, &ctx); return 0;
}
static int h_ft(int s, char **args, int argc) {
    ExtCtx ctx = { .ext = &args[1], .n = argc - 1 };
    respond_with_matches(s, pred_ext, &ctx); return 0;
}
static int h_fdb(int s, char **args, int argc) {
    (void)argc; struct tm tm = {0};
    if (!strptime(args[1], "%Y-%m-%d", &tm)) {
        send_framed(s, "ERR", "Bad date (use YYYY-MM-DD)"); return 0;
    }
    DateCtx ctx = { .bound = mktime(&tm), .before = 1 };
    respond_with_matches(s, pred_date, &ctx); return 0;
}
static int h_fda(int s, char **args, int argc) {
    (void)argc; struct tm tm = {0};
    if (!strptime(args[1], "%Y-%m-%d", &tm)) {
        send_framed(s, "ERR", "Bad date (use YYYY-MM-DD)"); return 0;
    }
    DateCtx ctx = { .bound = mktime(&tm), .before = 0 };
    respond_with_matches(s, pred_date, &ctx); return 0;
}
static int h_quitc(int s, char **args, int argc) {
    (void)args;(void)argc;
    send_framed(s, "BYE", "goodbye"); return 1;
}

/* ---------- tokenizer + dispatch ---------- */
/* mirror1 tokenizer: index-based for-loop variant.
 * Walks the line by integer index `j` rather than by pointer `p`,
 * and explicitly tracks whether we are inside a token via the
 * `in_word` flag.  No pointer arithmetic on `line` itself. */
static int tokenize(char *line, char **argv, int max) {
    int n = 0;
    int in_word = 0;
    int len = (int)strlen(line);
    for (int j = 0; j < len; j++) {
        char c = line[j];
        if (c == ' ' || c == '\t') {
            line[j] = 0;
            in_word = 0;
        } else if (!in_word) {
            if (n >= max) break;
            argv[n++] = &line[j];
            in_word = 1;
        }
    }
    return n;
}
static void normalise_dirlist(char **argv, int *argc) {
    if (*argc >= 2 && strcmp(argv[0], "dirlist") == 0) {            /* string compare */
        if      (strcmp(argv[1], "-a") == 0) argv[0] = (char*)"dirlist_a"; /* string compare */
        else if (strcmp(argv[1], "-t") == 0) argv[0] = (char*)"dirlist_t"; /* string compare */
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
 *  crequest() via MUTUALLY RECURSIVE DESCENT
 *
 *  Mirror1 does not use a switch statement OR a transition
 *  table for its FSM.  Instead each logical phase is its own
 *  function and the next phase is invoked directly as a call:
 *
 *      do_wait   ──(command arrived)─▶ do_parse
 *      do_parse  ──(valid)──────────▶ do_execute
 *      do_execute──(not quitc)──────▶ do_wait  (recursive!)
 *
 *  A depth counter on the session guards against runaway
 *  recursion for long-lived clients: after DEPTH_LIMIT
 *  commands we unwind back to the main loop and the outer
 *  while() re-enters do_wait, so the stack stays bounded
 *  even for thousands of commands per session.
 *
 *  This file also keeps a CmdRing of the last 8 commands.
 *  On disconnect (or quitc) we replay the session history
 *  so the operator can see exactly what the client ran.
 * ============================================================ */
#define DEPTH_LIMIT 64

typedef struct {
    int                  sock;
    char                 line[BUFSZ];
    char                *argv[MAX_ARGS];
    int                  argc;
    struct CommandEntry *ce;
    int                  leave;
    int                  depth;    /* how many recursive hops so far */
    int                  unwind;   /* 1 = pop back to outer loop */
    CmdRing              history;
} msession_t;

/* Forward declarations so the three phases can call each other. */
static void do_wait   (msession_t *m);
static void do_parse  (msession_t *m);
static void do_execute(msession_t *m);                              /* FSM: run the handler */

static void do_wait(msession_t *m) {                                /* FSM: read next line from client */
    /* Protect the stack: if we've recursed too deep, unwind. */
    if (m->depth >= DEPTH_LIMIT) { m->unwind = 1; return; }
    m->depth++;
    if (recv_line(m->sock, m->line, sizeof(m->line)) <= 0) {
        m->leave = 1;
        return;
    }
    do_parse(m);               /* tail-style dispatch, NOT a loop */
}

static void do_parse(msession_t *m) {                               /* FSM: tokenize & validate */
    /* Snapshot the raw line before tokenize() destroys it, then
     * record it in the ring so the session history is faithful. */
    char snapshot[BUFSZ];
    strncpy(snapshot, m->line, sizeof(snapshot) - 1);
    snapshot[sizeof(snapshot) - 1] = 0;

    m->argc = tokenize(m->line, m->argv, MAX_ARGS);
    if (m->argc == 0) { do_wait(m); return; }                       /* FSM: read next line from client */

    ring_push(&m->history, snapshot);

    normalise_dirlist(m->argv, &m->argc);
    m->ce = lookup_cmd(m->argv[0]);
    if (!m->ce) {
        send_framed(m->sock, "ERR", "unknown command");
        do_wait(m); return;                                         /* FSM: read next line from client */
    }
    if (m->argc < m->ce->min_args || m->argc > m->ce->max_args) {
        send_framed(m->sock, "ERR", "bad argument count");
        do_wait(m); return;                                         /* FSM: read next line from client */
    }
    do_execute(m);                                                  /* FSM: run the handler */
}

static void do_execute(msession_t *m) {                             /* FSM: run the handler */
    m->leave = m->ce->handler(m->sock, m->argv, m->argc);
    if (m->leave) return;      /* quitc -> pop all the way out */
    do_wait(m);                /* otherwise start the next command */
}

static void crequest(int csock) {
    /* Reset SIGCHLD to default in this child so our explicit waitpid()
     * inside create_tar() is not racing with an inherited reap handler.
     * The parent (main accept loop) keeps its reap handler. */
    signal(SIGCHLD, SIG_DFL);                                       /* restore default so our waitpid wins */

    msession_t m;
    m.sock   = csock;
    m.argc   = 0;
    m.ce     = NULL;
    m.leave  = 0;
    m.depth  = 0;
    m.unwind = 0;
    ring_init(&m.history);

    /* Outer loop only exists to re-enter do_wait after a depth-limit
     * unwind — it is NOT the FSM loop, it is the depth safety valve. */
    while (!m.leave) {
        m.depth  = 0;
        m.unwind = 0;
        do_wait(&m);                                                /* FSM: read next line from client */
        if (m.unwind) continue;   /* depth-limit reached, restart */
        break;                    /* natural exit (leave == 1) */
    }

    ring_dump(&m.history, ROLE_TAG);
    close(csock);
}

static void reap(int sig) { (void)sig; while (waitpid(-1, NULL, WNOHANG) > 0); } /* reap terminated children */

int main(void) {
    signal(SIGCHLD, reap);                                          /* auto-reap zombie children */
    int lsock = socket(AF_INET, SOCK_STREAM, 0);                    /* plain TCP socket */
    int yes = 1;
    setsockopt(lsock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)); /* allow quick rebind after restart */
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;                                        /* IPv4 address family */
    sa.sin_addr.s_addr = INADDR_ANY;                                /* listen on every interface */
    sa.sin_port = htons(MIRROR_PORT);                               /* port in network byte order */
    if (bind(lsock, (struct sockaddr*)&sa, sizeof(sa)) < 0) { perror("bind"); return 1; } /* claim the port */
    listen(lsock, 16);                                              /* queue up to 16 pending connects */
    printf("%s listening on port %d\n", ROLE_TAG, MIRROR_PORT);

    long serviced = 0;

    for (;;) {
        struct sockaddr_in ca; socklen_t cl = sizeof(ca);
        int c = accept(lsock, (struct sockaddr*)&ca, &cl);          /* block until a client arrives */
        if (c < 0) { if (errno == EINTR) continue; perror("accept"); continue; } /* signal arrived mid-call, retry */
        serviced++;
        printf("%s serviced connection #%ld\n", ROLE_TAG, serviced);
        send_all(c, "OK MIRROR1\n", 11);                            /* mirror1 greeting */
        pid_t pid = fork();                                         /* spawn a child for this client */
        if (pid == 0) { close(lsock); crequest(c); _exit(0); }      /* child does not need the listen socket */
        close(c);
    }
    return 0;                                                       /* signal success */
}
