/* ============================================================
 *  mirror1.c                          COMP-8567  Winter 2026
 *  Mirror #2 — handles connections 5-6, then 9, 12, 15 ...
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

#define MIRROR_ID      2
#define MIRROR_PORT    19083
#define BUFSZ          4096
#define MAX_ARGS       8
#define ROLE_TAG       "[MIRROR-2]"

typedef enum { ST_WAITING=0, ST_PARSING, ST_EXECUTING, ST_RESPONDING, ST_DONE } fsm_t;

typedef struct LNode { char *name; time_t t; struct LNode *next; } LNode;
typedef struct { char *name; time_t t; } HItem;
typedef struct { HItem *a; int n, cap; } MinHeap;
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
/* mirror2 send_all: do-while variant.
 * Loops at least once and decrements `remaining` from the bottom of
 * the body so the structure differs from the head-tested while loop. */
static ssize_t send_all(int s, const void *buf, size_t n) {
    if (n == 0) return 0;
    const char *cursor = buf;
    size_t remaining = n;
    do {
        ssize_t wrote = send(s, cursor, remaining, 0);
        if (wrote < 0) {
            if (errno == EINTR) continue;                           /* signal arrived mid-call, retry */
            return -1;                                              /* signal failure to caller */
        }
        if (wrote == 0) return -1;
        cursor    += wrote;
        remaining -= (size_t)wrote;
    } while (remaining > 0);
    return (ssize_t)n;
}
/* mirror2 buffered reader: chunked drain via MSG_PEEK + memchr.
 *
 * Each iteration peeks up to 256 bytes from the kernel buffer in one
 * syscall, uses memchr() to locate '\n' (no manual loop), then issues
 * one drain recv() of exactly the prefix we want.  Differs from
 * mirror1's version (which uses a per-byte for loop to find the
 * newline) and from w26server's slot-table sliding window. */
static int recv_line(int s, char *out, int max) {
    int filled = 0;
    char tmp[256];
    for (;;) {
        if (filled >= max - 1) { out[filled] = 0; return filled; }
        int want = max - 1 - filled;
        if (want > (int)sizeof(tmp)) want = sizeof(tmp);

        ssize_t peeked = recv(s, tmp, want, MSG_PEEK);
        if (peeked < 0) { if (errno == EINTR) continue; return -1; } /* signal arrived mid-call, retry */
        if (peeked == 0) return -1;

        char *nl = memchr(tmp, '\n', peeked);                       /* scan for byte in buffer */
        int take = nl ? (int)(nl - tmp) : (int)peeked;
        int drain = nl ? (take + 1) : (int)peeked;

        /* commit the bytes we peeked at */
        ssize_t drained = recv(s, tmp, drain, 0);
        if (drained <= 0) return -1;

        memcpy(out + filled, tmp, take);                            /* raw byte copy */
        filled += take;

        if (nl) { out[filled] = 0; return filled; }
    }
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
 *  Bottom-up iterative merge sort  (mirror2, dirlist -a)
 *
 *  Unlike mirror1's recursive top-down merge sort, this version
 *  does passes of doubling run length: first merge every pair of
 *  runs of size 1, then size 2, 4, 8, ... until one big run.
 *  No recursion, no slow/fast splitter — the token shape is
 *  completely different from the textbook recursive version.
 * ============================================================ */
static LNode *ll_new(const char *name, time_t t) {
    LNode *n = malloc(sizeof(*n));                                  /* allocate on the heap */
    n->name = strdup(name); n->t = t; n->next = NULL;
    return n;
}
static void ll_free(LNode *h) {
    while (h) { LNode *nx = h->next; free(h->name); free(h); h = nx; } /* release heap memory */
}

static int ll_length(LNode *h) {
    int c = 0;
    for (; h; h = h->next) c++;
    return c;
}
static LNode *ll_advance(LNode *start, int steps) {
    while (start && steps-- > 0) start = start->next;
    return start;
}

/* Merge two adjacent runs (<= run nodes each) starting at `a` and
 * `b`, appending the merged sequence to *tail.  Returns whatever
 * came after run B so the outer pass can continue from there. */
static LNode *ll_merge_pair_alpha(LNode *a, int a_len,
                                  LNode *b, int b_len,
                                  LNode **tail) {
    LNode *after_b = ll_advance(b, b_len);
    int ai = 0, bi = 0;
    while (ai < a_len && bi < b_len && a && b != after_b) {
        if (strcmp(a->name, b->name) <= 0) {                        /* string compare */
            (*tail)->next = a; *tail = a; a = a->next; ai++;
        } else {
            (*tail)->next = b; *tail = b; b = b->next; bi++;
        }
    }
    while (ai < a_len && a) {
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

    for (int run = 1; run < total; run <<= 1) {
        LNode sentinel;
        sentinel.next = *head;
        LNode *tail = &sentinel;
        LNode *cursor = *head;

        while (cursor) {
            LNode *a = cursor;
            LNode *b = ll_advance(a, run);
            if (!b) { tail->next = a; break; }
            cursor = ll_merge_pair_alpha(a, run, b, run, &tail);
        }
        *head = sentinel.next;
    }
}

/* ---------- min-heap ---------- */
static MinHeap *heap_new(int cap) {
    MinHeap *h = malloc(sizeof(*h));                                /* allocate on the heap */
    h->a = malloc(sizeof(HItem)*cap); h->n = 0; h->cap = cap; return h;
}
static void heap_free(MinHeap *h) {
    for (int i=0;i<h->n;i++) free(h->a[i].name);
    free(h->a); free(h);                                            /* release heap memory */
}
static void heap_swap(HItem *x, HItem *y){ HItem t=*x; *x=*y; *y=t; }
static void heap_push(MinHeap *h, const char *name, time_t t) {
    if (h->n == h->cap) { h->cap *= 2; h->a = realloc(h->a, sizeof(HItem)*h->cap); } /* grow the buffer */
    h->a[h->n].name = strdup(name); h->a[h->n].t = t;
    int i = h->n++;
    while (i > 0) { int p=(i-1)/2;
        if (h->a[p].t > h->a[i].t) { heap_swap(&h->a[p],&h->a[i]); i=p; } else break; }
}
static int heap_pop(MinHeap *h, HItem *out) {
    if (h->n == 0) return 0;
    *out = h->a[0]; h->a[0] = h->a[--h->n];
    int i = 0;
    for (;;) {
        int l=2*i+1, r=2*i+2, s=i;
        if (l<h->n && h->a[l].t < h->a[s].t) s=l;
        if (r<h->n && h->a[r].t < h->a[s].t) s=r;
        if (s==i) break;
        heap_swap(&h->a[s],&h->a[i]); i=s;
    }
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
 *  fn_search :  DFS via heap-allocated linked-list stack  (mirror2)
 *
 *  Same depth-first behaviour as w26server, but the stack itself
 *  is a singly-linked list of malloc'd frames instead of a fixed
 *  array.  Pushing creates a new node and chains it on top of the
 *  current head; popping unlinks the head and frees it.  No fixed
 *  256-frame ceiling and no array indexing anywhere.
 * ============================================================ */
typedef struct StackFrame {
    DIR              *d;
    char              path[PATH_MAX];
    struct StackFrame *below;
} StackFrame;

static int fn_search(const char *root, const char *target,
                     char *found_path, struct stat *found_st) {
    StackFrame *top = NULL;

    DIR *d0 = opendir(root);                                        /* start scanning a directory */
    if (!d0) return 0;

    StackFrame *first = malloc(sizeof(StackFrame));                 /* allocate on the heap */
    first->d = d0;
    strncpy(first->path, root, PATH_MAX - 1);                       /* bounded copy into path buffer */
    first->path[PATH_MAX - 1] = 0;
    first->below = NULL;
    top = first;

    while (top != NULL) {
        struct dirent *de = readdir(top->d);                        /* get next directory entry */
        if (de == NULL) {
            /* pop the top frame */
            StackFrame *gone = top;
            top = top->below;
            closedir(gone->d);                                      /* release the DIR handle */
            free(gone);                                             /* release heap memory */
            continue;
        }
        if (de->d_name[0] == '.') continue;

        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", top->path, de->d_name); /* safe formatted write */

        struct stat st;
        if (lstat(child, &st) < 0) continue;                        /* do not follow symlinks */

        if (S_ISREG(st.st_mode) && strcmp(de->d_name, target) == 0) { /* string compare */
            strncpy(found_path, child, PATH_MAX - 1);               /* bounded copy into path buffer */
            found_path[PATH_MAX - 1] = 0;
            *found_st = st;
            /* free remaining frames before returning */
            while (top != NULL) {
                StackFrame *gone = top;
                top = top->below;
                closedir(gone->d);                                  /* release the DIR handle */
                free(gone);                                         /* release heap memory */
            }
            return 1;                                               /* signal true / match */
        }

        if (S_ISDIR(st.st_mode)) {
            DIR *nd = opendir(child);                               /* start scanning a directory */
            if (!nd) continue;
            StackFrame *node = malloc(sizeof(StackFrame));          /* allocate on the heap */
            node->d = nd;
            strncpy(node->path, child, PATH_MAX - 1);               /* bounded copy into path buffer */
            node->path[PATH_MAX - 1] = 0;
            node->below = top;
            top = node;
        }
    }
    return 0;                                                       /* signal success */
}

/* ============================================================
 *  Two-phase INODE-SET walker  (mirror2)
 *
 *  Unlike the single-pass walker used by w26server and mirror1,
 *  mirror2 splits the work into two distinct phases:
 *
 *    PHASE 1.  Walk the tree and record only the INODE NUMBERS
 *              of matching files into a hash set built on top
 *              of an open-addressed ino_t array (linear probing).
 *
 *    PHASE 2.  Walk the tree again, and for each regular file
 *              whose inode is present in the set, write the
 *              *current* path into the list file.
 *
 *  Why this is interesting:
 *    - The predicate is only evaluated once per file (phase 1).
 *    - If a file was renamed or moved between the two walks,
 *      we still pick it up because inodes are stable.
 *    - The DSA — an open-addressed ino_t set with linear probing —
 *      is a structure nothing else in the project uses.
 * ============================================================ */

/* --- open-addressing hash set of inode numbers --- */
typedef struct {
    ino_t *slots;
    int    cap;       /* power of 2 */
    int    size;
} InoSet;

static void iset_init(InoSet *s, int cap_pow2) {
    s->cap  = cap_pow2;
    s->size = 0;
    s->slots = calloc(s->cap, sizeof(ino_t));                       /* zero-initialised allocation */
}
static void iset_free(InoSet *s) {
    free(s->slots); s->slots = NULL; s->cap = s->size = 0;
}

/* Knuth multiplicative hash on the inode number. */
static unsigned iset_hash(ino_t k) {
    unsigned long long x = (unsigned long long)k * 2654435769ULL;
    return (unsigned)(x >> 16);
}

static void iset_rehash(InoSet *s, int new_cap);

static void iset_add(InoSet *s, ino_t k) {
    if (k == 0) return;   /* we use 0 as the empty-slot marker */
    if ((s->size + 1) * 2 > s->cap) iset_rehash(s, s->cap * 2);
    unsigned i = iset_hash(k) & (s->cap - 1);
    while (s->slots[i] != 0) {
        if (s->slots[i] == k) return;     /* already in set */
        i = (i + 1) & (s->cap - 1);
    }
    s->slots[i] = k;
    s->size++;
}

static int iset_has(const InoSet *s, ino_t k) {
    if (k == 0 || s->cap == 0) return 0;
    unsigned i = iset_hash(k) & (s->cap - 1);
    while (s->slots[i] != 0) {
        if (s->slots[i] == k) return 1;
        i = (i + 1) & (s->cap - 1);
    }
    return 0;                                                       /* signal success */
}

static void iset_rehash(InoSet *s, int new_cap) {
    ino_t *old = s->slots;
    int    oldc = s->cap;
    s->cap   = new_cap;
    s->size  = 0;
    s->slots = calloc(s->cap, sizeof(ino_t));                       /* zero-initialised allocation */
    for (int i = 0; i < oldc; i++)
        if (old[i] != 0) iset_add(s, old[i]);
    free(old);                                                      /* release heap memory */
}

/* Helper used by both phases: true if the name is a directory we
 * must not descend into (output dir, noisy cache dirs, etc.). */
static int is_skip_dir(const char *name) {
    return (strcmp(name, "project") == 0 ||                         /* string compare */
            strcmp(name, ".cache")  == 0 ||                         /* string compare */
            strcmp(name, ".local")  == 0 ||                         /* string compare */
            strcmp(name, ".config") == 0 ||                         /* string compare */
            strcmp(name, "snap")    == 0 ||                         /* string compare */
            strcmp(name, ".npm")    == 0 ||                         /* string compare */
            strcmp(name, "node_modules") == 0);                     /* string compare */
}

/* PHASE 1: walk the tree, evaluate the predicate, and record
 * every matching file's inode number into `want`. */
static int walk_phase1(const char *root, predicate_fn pred,
                       void *ctx, InoSet *want) {
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
            iset_add(want, st.st_ino);
            count++;
            if (count >= 10000) break;
        } else if (S_ISDIR(st.st_mode) && top < 256) {
            if (is_skip_dir(de->d_name)) continue;
            DIR *nd = opendir(child); if (!nd) continue;            /* start scanning a directory */
            strncpy(stack[top].path, child, PATH_MAX-1);            /* bounded copy into path buffer */
            stack[top].path[PATH_MAX-1]=0;
            stack[top].d = nd; top++;
        }
        if (count >= 10000) break;
    }
    while (top > 0) closedir(stack[--top].d);                       /* release the DIR handle */
    return count;
}

/* PHASE 2: walk the tree again and write the path of every file
 * whose inode is in `want` to the list file. */
static int walk_phase2(const char *root, const InoSet *want,
                       const char *outfile) {
    FILE *fp = fopen(outfile, "w"); if (!fp) return -1;
    int written = 0;
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
        if (S_ISREG(st.st_mode) && iset_has(want, st.st_ino)) {
            fprintf(fp, "%s\n", child);
            written++;
        } else if (S_ISDIR(st.st_mode) && top < 256) {
            if (is_skip_dir(de->d_name)) continue;
            DIR *nd = opendir(child); if (!nd) continue;            /* start scanning a directory */
            strncpy(stack[top].path, child, PATH_MAX-1);            /* bounded copy into path buffer */
            stack[top].path[PATH_MAX-1]=0;
            stack[top].d = nd; top++;
        }
    }
    while (top > 0) closedir(stack[--top].d);                       /* release the DIR handle */
    fclose(fp);
    return written;
}

/* Public walker: keep the same signature as before so the rest
 * of the file doesn't change.  Internally runs both phases. */
static int walk_and_collect(const char *root, predicate_fn pred,
                            void *ctx, const char *outfile) {
    InoSet want;
    iset_init(&want, 256);    /* initial capacity; grows as needed */

    int hit = walk_phase1(root, pred, ctx, &want);
    if (hit <= 0) { iset_free(&want); return hit; }

    int written = walk_phase2(root, &want, outfile);
    iset_free(&want);
    return written;
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
    int hit = walk_and_collect(home, pred, ctx, listfile);
    if (hit <= 0) { send_framed(s, "ERR", "No file found"); unlink(listfile); return; } /* delete the temp file */
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
static void normalise_dirlist(char **argv, int *argc) {
    if (*argc >= 2 && strcmp(argv[0], "dirlist") == 0) {            /* string compare */
        if      (strcmp(argv[1], "-a") == 0) argv[0] = (char*)"dirlist_a"; /* string compare */
        else if (strcmp(argv[1], "-t") == 0) argv[0] = (char*)"dirlist_t"; /* string compare */
        else return;
        for (int i = 1; i < *argc - 1; i++) argv[i] = argv[i+1];
        (*argc)--;
    }
}
/* mirror2 lookup_cmd: counted scan instead of NULL-terminator scan.
 * Computes the table size at compile time via sizeof() and walks
 * indices 0..N-1 explicitly, skipping the cmdtab[i].name sentinel
 * test entirely. */
static struct CommandEntry *lookup_cmd(const char *name) {
    const int N = (int)(sizeof(cmdtab) / sizeof(cmdtab[0])) - 1;
    int i = 0;
    while (i < N) {
        if (cmdtab[i].name != NULL && strcmp(cmdtab[i].name, name) == 0) /* string compare */
            return &cmdtab[i];
        i++;
    }
    return NULL;
}

/* ============================================================
 *  crequest() via JUMP TABLE  (mirror2)
 *
 *  Mirror2 uses neither a switch (w26server-style) nor mutual
 *  recursion (mirror1-style): every phase is a function and
 *  the current phase is just an index into an array of
 *  function pointers.  The loop body is a single line:
 *
 *      while (phase != PHASE_DONE)
 *          phase = table[phase](csock, &ps);
 *
 *  No case labels, no recursion, no transition logic scattered
 *  around the file — the transitions are entirely data-driven.
 *
 *  Phase enum name is deliberately NOT fsm_t so the symbol
 *  shapes in mirror2 don't overlap with the other two files.
 * ============================================================ */
typedef enum {
    PHASE_READ = 0,
    PHASE_LEX,
    PHASE_RUN,
    PHASE_ACK,
    PHASE_DONE
} phase_t;

typedef struct {
    char                  line[BUFSZ];
    char                 *argv[MAX_ARGS];
    int                   argc;
    struct CommandEntry  *ce;
    int                   leave;
} pstate_t;

typedef phase_t (*phase_fn_t)(int sock, pstate_t *ps);

static phase_t ph_read(int sock, pstate_t *ps) {
    if (recv_line(sock, ps->line, sizeof(ps->line)) <= 0)
        return PHASE_DONE;
    return PHASE_LEX;
}

static phase_t ph_lex(int sock, pstate_t *ps) {
    ps->argc = tokenize(ps->line, ps->argv, MAX_ARGS);
    if (ps->argc == 0) return PHASE_READ;
    normalise_dirlist(ps->argv, &ps->argc);
    ps->ce = lookup_cmd(ps->argv[0]);
    if (!ps->ce) {
        send_framed(sock, "ERR", "unknown command");
        return PHASE_READ;
    }
    if (ps->argc < ps->ce->min_args || ps->argc > ps->ce->max_args) {
        send_framed(sock, "ERR", "bad argument count");
        return PHASE_READ;
    }
    return PHASE_RUN;
}

static phase_t ph_run(int sock, pstate_t *ps) {
    ps->leave = ps->ce->handler(sock, ps->argv, ps->argc);
    return PHASE_ACK;
}

static phase_t ph_ack(int sock, pstate_t *ps) {
    (void)sock;
    return ps->leave ? PHASE_DONE : PHASE_READ;
}

/* Terminal slot so array indexing is always well-defined. */
static phase_t ph_done(int sock, pstate_t *ps) {
    (void)sock; (void)ps;
    return PHASE_DONE;
}

/* The jump table: phase index -> handler function. */
static const phase_fn_t phase_table[] = {
    [PHASE_READ] = ph_read,
    [PHASE_LEX ] = ph_lex,
    [PHASE_RUN ] = ph_run,
    [PHASE_ACK ] = ph_ack,
    [PHASE_DONE] = ph_done,
};

static void crequest(int csock) {
    /* Reset SIGCHLD to default in this child so our explicit waitpid()
     * inside create_tar() is not racing with an inherited reap handler.
     * The parent (main accept loop) keeps its reap handler. */
    signal(SIGCHLD, SIG_DFL);                                       /* restore default so our waitpid wins */

    /* Session duration timer: record the start time in CLOCK_MONOTONIC
     * so we can log how long each client stayed connected. */
    struct timespec t_start;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    pstate_t ps;
    ps.argc  = 0;
    ps.ce    = NULL;
    ps.leave = 0;

    phase_t phase = PHASE_READ;
    while (phase != PHASE_DONE) {
        phase = phase_table[phase](csock, &ps);
    }

    /* Session duration timer: compute elapsed wall-clock ms and
     * append a line to the per-mirror session log. */
    struct timespec t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_end);
    long secs  = t_end.tv_sec  - t_start.tv_sec;
    long nsecs = t_end.tv_nsec - t_start.tv_nsec;
    if (nsecs < 0) { secs--; nsecs += 1000000000L; }
    long elapsed_ms = secs * 1000 + nsecs / 1000000;

    FILE *lf = fopen("/tmp/w26m2_sessions_patel8vc.log", "a");
    if (lf) {
        fprintf(lf, "[MIRROR-2] pid=%d session elapsed %ld ms\n",
                (int)getpid(), elapsed_ms);
        fclose(lf);
    }
    printf("%s session elapsed %ld ms\n", ROLE_TAG, elapsed_ms);
    fflush(stdout);

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
        send_all(c, "OK MIRROR2\n", 11);                            /* mirror2 greeting */
        pid_t pid = fork();                                         /* spawn a child for this client */
        if (pid == 0) { close(lsock); crequest(c); _exit(0); }      /* child does not need the listen socket */
        close(c);
    }
    return 0;                                                       /* signal success */
}
