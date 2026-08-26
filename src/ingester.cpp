/*
 * ingester.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Scans the input directory for *.csv files using low-level POSIX calls
 * (opendir/readdir are NOT in our allowed list, so we use a compile-time
 * workaround: accept a list of files via argv, or use a helper approach).
 *
 * Because <dirent.h> was NOT taught in the labs we instead receive the
 * input directory path and walk it using a pipe to the shell ls command
 * — OR — we accept a single CSV file per run, orchestrated from run.sh.
 *
 * Design chosen: run.sh passes every *.csv path as separate argv arguments.
 * This keeps ingester.cpp using only lab-approved headers.
 *
 * Usage:
 *   ./ingester <fifo_path> <csv_file1> [<csv_file2> ...]
 *
 * Allowed headers: exactly those taught in OS labs.
 * Code style     : C++ with "using namespace std;"
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include "common/common.h"

/* ── Signal state ── */
static volatile sig_atomic_t g_sigterm    = 0;
static volatile sig_atomic_t g_sigusr1    = 0;

static unsigned int  g_files_processed = 0;
static unsigned int  g_chunks_sent     = 0;
static unsigned long g_bytes_sent      = 0;

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

/* ── Write exactly n bytes to fd (handles partial writes) ── */
static int write_all(int fd, const void *buf, size_t n)
{
    const char *ptr = (const char *)buf;
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t w = write(fd, ptr, remaining);
        if (w < 0) {
            cerr << "[ingester] write error: " << strerror(errno) << endl;
            return -1;
        }
        ptr       += w;
        remaining -= w;
    }
    return 0;
}

/* ── Send one chunk (header + payload) into the FIFO ── */
static int send_chunk(int fifo_fd, unsigned int chunk_id,
                      unsigned int file_id, const char *buf,
                      unsigned int len, unsigned int flags)
{
    ChunkHeader hdr;
    hdr.magic          = CHUNK_MAGIC;
    hdr.chunk_id       = chunk_id;
    hdr.source_file_id = file_id;
    hdr.byte_count     = len;
    hdr.flags          = flags;

    if (write_all(fifo_fd, &hdr, sizeof(hdr)) < 0) return -1;
    if (len > 0 && write_all(fifo_fd, buf, len) < 0) return -1;

    g_chunks_sent++;
    g_bytes_sent += len;
    return 0;
}

/* ── Process one CSV file: read in chunks and forward to FIFO ── */
static int process_file(int fifo_fd, const char *filepath,
                        unsigned int file_id, unsigned int *chunk_id_ptr)
{
    LOG("ingester", "Opening file: " << filepath << " (file_id=" << file_id << ")");

    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        cerr << "[ingester] Cannot open " << filepath << ": "
             << strerror(errno) << endl;
        return EXIT_IO_ERROR;
    }

    char buf[MAX_CHUNK_BYTES];
    ssize_t n;

    while (!g_sigterm && (n = read(fd, buf, sizeof(buf))) > 0) {
        LOG("ingester", "Sending chunk " << *chunk_id_ptr
            << " (file_id=" << file_id << ", " << n << " bytes)");

        if (send_chunk(fifo_fd, *chunk_id_ptr, file_id,
                       buf, (unsigned int)n, 0) < 0) {
            close(fd);
            return EXIT_IO_ERROR;
        }
        (*chunk_id_ptr)++;
    }

    if (n < 0) {
        cerr << "[ingester] read error on " << filepath << ": "
             << strerror(errno) << endl;
        close(fd);
        return EXIT_IO_ERROR;
    }

    close(fd);
    g_files_processed++;
    LOG("ingester", "Finished file: " << filepath);
    return EXIT_OK;
}

int main(int argc, char *argv[])
{
    if (argc < 3) {
        cerr << "[ingester] Usage: ingester <fifo_path> <csv_file1> [csv_file2 ...]" << endl;
        return EXIT_BAD_ARGS;
    }

    const char *fifo_path = argv[1];

    LOG("ingester", "Starting — fifo=" << fifo_path
        << " files=" << (argc - 2));

    /* Install signal handlers (re-register inside handler, as per lab) */
    signal(SIGTERM, handler_sigterm);
    signal(SIGINT,  handler_sigterm);
    signal(SIGUSR1, handler_sigusr1);

    /* Open FIFO for writing — blocks until processor opens read end */
    LOG("ingester", "Opening FIFO " << fifo_path
        << " for writing (will block until reader opens)...");
    int fifo_fd = open(fifo_path, O_WRONLY);
    if (fifo_fd < 0) {
        cerr << "[ingester] open(FIFO) failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }
    LOG("ingester", "FIFO opened — starting to send chunks.");

    unsigned int chunk_id = 0;

    /* Process each CSV file passed as argument */
    for (int i = 2; i < argc && !g_sigterm; i++) {

        /* Print SIGUSR1 stats if requested */
        if (g_sigusr1) {
            g_sigusr1 = 0;
            cerr << "[ingester][PID=" << getpid() << "] SIGUSR1 stats:"
                 << " files_processed=" << g_files_processed
                 << " chunks_sent="     << g_chunks_sent
                 << " bytes_sent="      << g_bytes_sent << endl;
        }

        unsigned int file_id = (unsigned int)(i - 2);
        process_file(fifo_fd, argv[i], file_id, &chunk_id);
    }

    /* Send EOF chunk so processor knows we are done */
    LOG("ingester", "Sending EOF chunk.");
    send_chunk(fifo_fd, chunk_id, 0, NULL, 0, CHUNK_EOF_FLAG);

    close(fifo_fd);

    LOG("ingester", "Done — files=" << g_files_processed
        << " chunks=" << g_chunks_sent
        << " bytes="  << g_bytes_sent);

    return g_sigterm ? EXIT_SIGTERM : EXIT_OK;
}
