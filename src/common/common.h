#ifndef COMMON_H
#define COMMON_H

/* ── Allowed headers from OS labs ── */
#include <iostream>       /* cout, cerr, cin  (Lab style) */
#include <cstring>        /* strlen, strcpy, strcmp, strtok_r */
#include <cstdlib>        /* exit, atoi, atof, malloc, free */
#include <unistd.h>       /* read, write, close, fork, pipe, dup, dup2, getpid, getppid, sleep */
#include <fcntl.h>        /* open, O_RDONLY, O_WRONLY, O_CREAT, O_TRUNC */
#include <sys/types.h>    /* pid_t, ssize_t, size_t, key_t */
#include <sys/wait.h>     /* wait, waitpid */
#include <sys/stat.h>     /* mkfifo, stat */
#include <sys/shm.h>      /* shmget, shmat, shmdt, shmctl, IPC_CREAT, IPC_RMID */
#include <signal.h>       /* signal, kill, sigaction, sigemptyset, sigfillset, sigaddset */
                          /* sigdelset, sigprocmask, sigsuspend, SIG_IGN, SIG_DFL */
#include <pthread.h>      /* pthread_create, pthread_join, pthread_exit, pthread_self */
                          /* pthread_attr_t, pthread_mutex_t, pthread_cancel */
                          /* pthread_setcancelstate, pthread_setcanceltype */
#include <semaphore.h>    /* sem_t, sem_init, sem_wait, sem_post, sem_destroy */
                          /* sem_open, sem_close, sem_unlink (named semaphores) */
#include <errno.h>        /* errno, strerror */

using namespace std;

/* ══════════════════════════════════════════════════════
 *  IPC Constants
 * ══════════════════════════════════════════════════════ */
#define FIFO_DEFAULT        "/tmp/os_proj_fifo"
#define SEM_READY_NAME      "/os_proj_sem_ready"

/* SysV shared memory key (as taught in lab) */
#define SHM_KEY             ((key_t)5678)
#define SHM_SIZE            sizeof(SharedData)

/* ══════════════════════════════════════════════════════
 *  Retail Transactions — Aggregation Structures
 * ══════════════════════════════════════════════════════ */
#define MAX_CATEGORIES      128
#define CATEGORY_NAME_LEN   64
#define PRODUCT_NAME_LEN    128

struct CategoryRecord {
    char   category[CATEGORY_NAME_LEN];
    double total_revenue;
    long   transaction_count;
    char   top_product[PRODUCT_NAME_LEN];
    double top_product_revenue;
};

struct SharedData {
    int            num_categories;
    long           total_records;
    long           total_chunks;
    int            ready;           /* set to 1 by processor when done */
    CategoryRecord categories[MAX_CATEGORIES];
};

/* ══════════════════════════════════════════════════════
 *  Chunk header written into the FIFO
 * ══════════════════════════════════════════════════════ */
#define CHUNK_MAGIC         0xC001BEEF
#define CHUNK_EOF_FLAG      0x01
#define MAX_CHUNK_BYTES     (32 * 1024)   /* 32 KB payload */

struct ChunkHeader {
    unsigned int magic;
    unsigned int chunk_id;
    unsigned int source_file_id;
    unsigned int byte_count;
    unsigned int flags;
};

/* ══════════════════════════════════════════════════════
 *  Standard Exit Codes (from project spec)
 * ══════════════════════════════════════════════════════ */
#define EXIT_OK          0
#define EXIT_BAD_ARGS   10
#define EXIT_IPC_FAIL   20
#define EXIT_CHILD_DIED 30
#define EXIT_IO_ERROR   40
#define EXIT_SIGINT    130
#define EXIT_SIGTERM   143

/* ══════════════════════════════════════════════════════
 *  Logging macro — every line carries PID and PPID
 *  Uses cerr so it always goes to stderr even after
 *  stdout is redirected via dup2.
 * ══════════════════════════════════════════════════════ */
#define LOG(comp, msg) \
    cerr << "[" << comp << "][PID=" << getpid() \
         << "|PPID=" << getppid() << "] " << msg << endl

#endif /* COMMON_H */
