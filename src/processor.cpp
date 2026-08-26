/*
 * processor.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Reads chunks from the FIFO, feeds a bounded queue (producer-consumer),
 * runs N worker threads that parse CSV rows and update a shared aggregation
 * table (Retail variant: per-category revenue + top product).
 * Writes the final aggregated result into SysV shared memory and posts a
 * named POSIX semaphore to signal the reporter.
 *
 * Usage:
 *   ./processor <fifo_path> <num_threads> <queue_size>
 *
 * Synchronisation:
 *   • sem_empty (unnamed, init=Q)  — tracks free slots in the queue
 *   • sem_full  (unnamed, init=0)  — tracks filled slots in the queue
 *   • q_mutex                      — protects head/tail pointers
 *   • agg_mutex                    — protects the aggregation table
 *
 * Threads use explicit pthread_attr_t (4-step pattern from Lab 13).
 * Code style: C++ with "using namespace std;"
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include "common/common.h"

/* ════════════════════════════════════════════════════════════
 *  Aggregation table  (shared between worker threads)
 * ════════════════════════════════════════════════════════════ */
static pthread_mutex_t agg_mutex = PTHREAD_MUTEX_INITIALIZER;

static CategoryRecord g_agg[MAX_CATEGORIES];
static int            g_num_categories = 0;
static long           g_total_records  = 0;
static long           g_total_chunks   = 0;

/* ════════════════════════════════════════════════════════════
 *  Bounded queue
 * ════════════════════════════════════════════════════════════ */
struct QueueItem {
    char  *data;
    int    len;
    int    is_poison;
};

static QueueItem      *g_queue      = NULL;
static int             g_queue_cap  = 0;
static int             g_head       = 0;
static int             g_tail       = 0;

static pthread_mutex_t q_mutex    = PTHREAD_MUTEX_INITIALIZER;
static sem_t           sem_empty;   /* counts free slots  (init = Q) */
static sem_t           sem_full;    /* counts filled slots (init = 0) */

/* ════════════════════════════════════════════════════════════
 *  Signal state
 * ════════════════════════════════════════════════════════════ */
static volatile sig_atomic_t g_sigterm = 0;
static volatile sig_atomic_t g_sigusr1 = 0;

static void handler_sigterm(int sig)
{
    signal(SIGTERM, handler_sigterm);
    signal(SIGINT,  handler_sigterm);
    (void)sig;
    g_sigterm = 1;
}

static void handler_sigusr1(int sig)
{
    signal(SIGUSR1, handler_sigusr1);
    (void)sig;
    g_sigusr1 = 1;
}

/* ════════════════════════════════════════════════════════════
 *  Aggregation helpers
 * ════════════════════════════════════════════════════════════ */

/* Find an existing category or create a new one (call under agg_mutex) */
static CategoryRecord *get_or_create_category(const char *name)
{
    for (int i = 0; i < g_num_categories; i++) {
        if (strcmp(g_agg[i].category, name) == 0)
            return &g_agg[i];
    }
    if (g_num_categories >= MAX_CATEGORIES) return NULL;
    CategoryRecord *rec = &g_agg[g_num_categories++];
    memset(rec, 0, sizeof(CategoryRecord));
    strncpy(rec->category, name, CATEGORY_NAME_LEN - 1);
    return rec;
}

/*
 * Parse one CSV line (Retail format):
 *   category, product_name, price, quantity
 * Revenue = price * quantity
 */
static void aggregate_line(const char *line)
{
    /* Work on a local copy so strtok_r does not mangle shared data */
    char buf[512];
    strncpy(buf, line, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    /* Trim trailing CR/LF */
    int l = (int)strlen(buf);
    while (l > 0 && (buf[l-1] == '\n' || buf[l-1] == '\r'))
        buf[--l] = '\0';
    if (l == 0) return;

    /* Skip header row */
    if (strncmp(buf, "category", 8) == 0 ||
        strncmp(buf, "Category", 8) == 0) return;

    char *saveptr;
    char *tok;

    tok = strtok_r(buf, ",", &saveptr);
    if (!tok) return;
    char category[CATEGORY_NAME_LEN];
    strncpy(category, tok, CATEGORY_NAME_LEN - 1);
    category[CATEGORY_NAME_LEN - 1] = '\0';

    tok = strtok_r(NULL, ",", &saveptr);
    if (!tok) return;
    char product[PRODUCT_NAME_LEN];
    strncpy(product, tok, PRODUCT_NAME_LEN - 1);
    product[PRODUCT_NAME_LEN - 1] = '\0';

    tok = strtok_r(NULL, ",", &saveptr);
    if (!tok) return;
    double price = atof(tok);

    tok = strtok_r(NULL, ",", &saveptr);
    if (!tok) return;
    int quantity = atoi(tok);

    if (price <= 0.0 || quantity <= 0) return;

    double revenue = price * (double)quantity;

    /* Update aggregation table under mutex */
    pthread_mutex_lock(&agg_mutex);

    CategoryRecord *rec = get_or_create_category(category);
    if (rec) {
        rec->total_revenue     += revenue;
        rec->transaction_count += 1;
        if (revenue > rec->top_product_revenue) {
            rec->top_product_revenue = revenue;
            strncpy(rec->top_product, product, PRODUCT_NAME_LEN - 1);
            rec->top_product[PRODUCT_NAME_LEN - 1] = '\0';
        }
    }
    g_total_records++;

    pthread_mutex_unlock(&agg_mutex);
}

/* Split a chunk payload into lines and aggregate each one */
static void process_chunk_data(const char *data, int len)
{
    char line[1024];
    int  lpos = 0;

    for (int i = 0; i < len; i++) {
        char c = data[i];
        if (c == '\n') {
            line[lpos] = '\0';
            aggregate_line(line);
            lpos = 0;
        } else if (c != '\r') {
            if (lpos < (int)(sizeof(line) - 1))
                line[lpos++] = c;
        }
    }
    /* Flush any partial last line */
    if (lpos > 0) {
        line[lpos] = '\0';
        aggregate_line(line);
    }
}

/* ════════════════════════════════════════════════════════════
 *  Bounded queue — enqueue (called by FIFO reader thread)
 * ════════════════════════════════════════════════════════════ */
static void enqueue(QueueItem item)
{
    sem_wait(&sem_empty);          /* wait for a free slot */

    pthread_mutex_lock(&q_mutex);
    g_queue[g_tail] = item;
    g_tail = (g_tail + 1) % g_queue_cap;
    pthread_mutex_unlock(&q_mutex);

    sem_post(&sem_full);           /* signal a filled slot */
}

/* ════════════════════════════════════════════════════════════
 *  Worker thread function
 * ════════════════════════════════════════════════════════════ */
static void *worker_thread(void *arg)
{
    int id = *(int *)arg;
    LOG("processor", "Worker " << id << " started (TID="
        << (unsigned long)pthread_self() << ")");

    while (1) {
        sem_wait(&sem_full);       /* block until a slot is filled */

        pthread_mutex_lock(&q_mutex);
        QueueItem item = g_queue[g_head];
        g_head = (g_head + 1) % g_queue_cap;
        pthread_mutex_unlock(&q_mutex);

        sem_post(&sem_empty);      /* a slot is now free again */

        if (item.is_poison) {
            LOG("processor", "Worker " << id << " received poison pill — exiting.");
            if (item.data) free(item.data);
            break;
        }

        LOG("processor", "Worker " << id << " processing chunk ("
            << item.len << " bytes).");
        process_chunk_data(item.data, item.len);
        free(item.data);
    }

    LOG("processor", "Worker " << id << " done.");
    pthread_exit(NULL);
    return NULL;
}

/* ════════════════════════════════════════════════════════════
 *  main()
 * ════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    if (argc < 4) {
        cerr << "[processor] Usage: processor <fifo_path> <num_threads> <queue_size>"
             << endl;
        return EXIT_BAD_ARGS;
    }

    const char *fifo_path  = argv[1];
    int         num_threads = atoi(argv[2]);
    int         queue_size  = atoi(argv[3]);

    if (num_threads < 1 || queue_size < 1) {
        cerr << "[processor] num_threads and queue_size must be >= 1." << endl;
        return EXIT_BAD_ARGS;
    }

    LOG("processor", "Starting — fifo=" << fifo_path
        << " threads=" << num_threads
        << " queue="   << queue_size);

    /* Install signal handlers */
    signal(SIGTERM, handler_sigterm);
    signal(SIGINT,  handler_sigterm);
    signal(SIGUSR1, handler_sigusr1);

    /* ── Initialise bounded queue ── */
    g_queue_cap = queue_size;
    g_queue = (QueueItem *)malloc(queue_size * sizeof(QueueItem));
    if (!g_queue) {
        cerr << "[processor] malloc(queue) failed." << endl;
        return EXIT_IPC_FAIL;
    }
    memset(g_queue, 0, queue_size * sizeof(QueueItem));

    /* Initialise unnamed semaphores (pshared=0 → thread scope, as taught) */
    if (sem_init(&sem_empty, 0, (unsigned int)queue_size) < 0) {
        cerr << "[processor] sem_init(empty) failed: " << strerror(errno) << endl;
        free(g_queue);
        return EXIT_IPC_FAIL;
    }
    if (sem_init(&sem_full, 0, 0) < 0) {
        cerr << "[processor] sem_init(full) failed: " << strerror(errno) << endl;
        sem_destroy(&sem_empty);
        free(g_queue);
        return EXIT_IPC_FAIL;
    }

    /* ── Create thread pool using pthread_attr_t (4-step Lab 13 pattern) ── */
    pthread_attr_t attr;

    /* Step 1 — Declare + Step 2 — Initialise */
    pthread_attr_init(&attr);

    /* Step 3 — Modify: joinable state + stack size */
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_attr_setstacksize(&attr, 1024 * 1024);   /* 1 MB per worker */

    pthread_t *threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));
    int       *ids     = (int *)malloc(num_threads * sizeof(int));

    if (!threads || !ids) {
        cerr << "[processor] malloc(threads) failed." << endl;
        pthread_attr_destroy(&attr);
        sem_destroy(&sem_empty);
        sem_destroy(&sem_full);
        free(g_queue);
        return EXIT_IPC_FAIL;
    }

    for (int i = 0; i < num_threads; i++) {
        ids[i] = i;
        /* Step 4 — Create */
        int rc = pthread_create(&threads[i], &attr, worker_thread, &ids[i]);
        if (rc != 0) {
            cerr << "[processor] pthread_create(" << i << ") failed: "
                 << strerror(rc) << endl;
            /* Clean up already-created threads with poison pills */
            for (int j = 0; j < i; j++) {
                QueueItem poison = { NULL, 0, 1 };
                enqueue(poison);
            }
            for (int j = 0; j < i; j++) pthread_join(threads[j], NULL);
            pthread_attr_destroy(&attr);
            sem_destroy(&sem_empty);
            sem_destroy(&sem_full);
            free(g_queue); free(threads); free(ids);
            return EXIT_IPC_FAIL;
        }
    }
    /* Destroy attr immediately after all creates (Lab 13: "never before") */
    pthread_attr_destroy(&attr);

    /* ── Open FIFO for reading (blocks until ingester opens write end) ── */
    LOG("processor", "Opening FIFO " << fifo_path << " for reading...");
    int fifo_fd = open(fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        cerr << "[processor] open(FIFO) failed: " << strerror(errno) << endl;
        for (int i = 0; i < num_threads; i++) {
            QueueItem poison = { NULL, 0, 1 };
            enqueue(poison);
        }
        for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);
        free(g_queue); free(threads); free(ids);
        return EXIT_IPC_FAIL;
    }
    LOG("processor", "FIFO opened — reading chunks.");

    /* ── FIFO reader loop ── */
    while (!g_sigterm) {

        if (g_sigusr1) {
            g_sigusr1 = 0;
            pthread_mutex_lock(&agg_mutex);
            cerr << "[processor][PID=" << getpid() << "] SIGUSR1 stats:"
                 << " categories=" << g_num_categories
                 << " records="    << g_total_records
                 << " chunks="     << g_total_chunks << endl;
            pthread_mutex_unlock(&agg_mutex);
        }

        /* Read chunk header */
        ChunkHeader hdr;
        ssize_t r = read(fifo_fd, &hdr, sizeof(hdr));

        if (r == 0) {
            LOG("processor", "FIFO closed by ingester (EOF).");
            break;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            cerr << "[processor] read(header) error: " << strerror(errno) << endl;
            break;
        }
        if (r != (ssize_t)sizeof(hdr) || hdr.magic != CHUNK_MAGIC) {
            cerr << "[processor] Invalid chunk header (magic=0x" << hex
                 << hdr.magic << dec << ")." << endl;
            break;
        }

        /* EOF chunk from ingester → stop reading */
        if (hdr.flags & CHUNK_EOF_FLAG) {
            LOG("processor", "EOF chunk received from ingester.");
            break;
        }

        /* Read payload */
        char *payload = (char *)malloc(hdr.byte_count + 1);
        if (!payload) {
            cerr << "[processor] malloc(payload) OOM." << endl;
            break;
        }

        ssize_t total_read = 0;
        while (total_read < (ssize_t)hdr.byte_count) {
            ssize_t n = read(fifo_fd, payload + total_read,
                             hdr.byte_count - (unsigned int)total_read);
            if (n <= 0) {
                cerr << "[processor] Short read on payload." << endl;
                break;
            }
            total_read += n;
        }
        payload[hdr.byte_count] = '\0';

        g_total_chunks++;
        LOG("processor", "Received chunk " << hdr.chunk_id
            << " (file=" << hdr.source_file_id
            << ", " << hdr.byte_count << " bytes) — enqueuing.");

        QueueItem item;
        item.data      = payload;
        item.len       = (int)hdr.byte_count;
        item.is_poison = 0;
        enqueue(item);
    }

    close(fifo_fd);

    /* ── Send N poison pills to shut workers down cleanly ── */
    LOG("processor", "Sending " << num_threads << " poison pills.");
    for (int i = 0; i < num_threads; i++) {
        QueueItem poison = { NULL, 0, 1 };
        enqueue(poison);
    }

    /* ── Join all worker threads ── */
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    LOG("processor", "All workers joined.");

    /* ── Write aggregation table into SysV shared memory ── */
    /* Consumer uses shmget without IPC_CREAT to access the existing segment */
    int shmid = shmget(SHM_KEY, SHM_SIZE, 0666);
    if (shmid < 0) {
        cerr << "[processor] shmget failed: " << strerror(errno) << endl;
    } else {
        SharedData *shm = (SharedData *) shmat(shmid, NULL, 0);
        if ((void *)shm == (void *)-1) {
            cerr << "[processor] shmat failed: " << strerror(errno) << endl;
        } else {
            pthread_mutex_lock(&agg_mutex);
            shm->num_categories = g_num_categories;
            shm->total_records  = g_total_records;
            shm->total_chunks   = g_total_chunks;
            shm->ready          = 1;
            memcpy(shm->categories, g_agg,
                   g_num_categories * sizeof(CategoryRecord));
            pthread_mutex_unlock(&agg_mutex);

            shmdt(shm);
            LOG("processor", "Aggregation written to shared memory ("
                << g_num_categories << " categories, "
                << g_total_records  << " records).");
        }
    }

    /* ── Post named semaphore so reporter knows data is ready ── */
    sem_t *sem_ready = sem_open(SEM_READY_NAME, O_CREAT, 0666, 0);
    if (sem_ready == SEM_FAILED) {
        cerr << "[processor] sem_open(" << SEM_READY_NAME
             << ") failed: " << strerror(errno) << endl;
    } else {
        sem_post(sem_ready);
        sem_close(sem_ready);
        LOG("processor", "Named semaphore " << SEM_READY_NAME << " posted.");
    }

    /* ── Cleanup ── */
    sem_destroy(&sem_empty);
    sem_destroy(&sem_full);
    pthread_mutex_destroy(&agg_mutex);
    pthread_mutex_destroy(&q_mutex);
    free(g_queue);
    free(threads);
    free(ids);

    LOG("processor", "Exiting cleanly.");
    return EXIT_OK;
}
