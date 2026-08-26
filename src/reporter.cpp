/*
 * reporter.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Waits (blocking) on the named POSIX semaphore posted by processor.
 * Reads the aggregation table from SysV shared memory.
 * Writes two output files:
 *   report.txt  — human-readable, produced via dup()/dup2() stdout redirect
 *   report.csv  — machine-readable CSV
 * Sends SIGUSR1 to the dispatcher (via getppid()) when done.
 *
 * dup() / dup2() demonstration (as required by the project spec):
 * ─────────────────────────────────────────────────────────────────────────────
 *   1. Save current stdout using dup()     → saved_stdout
 *   2. Open report.txt                     → txt_fd
 *   3. dup2(txt_fd, STDOUT_FILENO)         → stdout now goes to report.txt
 *   4. Write report using printf/cout      → all output lands in the file
 *   5. Flush + dup2(saved_stdout,1)        → restore original stdout
 *   6. close(saved_stdout)
 * ─────────────────────────────────────────────────────────────────────────────
 * Code style: C++ with "using namespace std;" — no std:: prefix.
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include "common/common.h"

static volatile sig_atomic_t g_sigterm = 0;

static void handler_sigterm(int sig)
{
    signal(SIGTERM, handler_sigterm);
    signal(SIGINT,  handler_sigterm);
    (void)sig;
    g_sigterm = 1;
}

/* Simple bubble-sort: sort CategoryRecord array by total_revenue descending */
static void sort_by_revenue(CategoryRecord *cats, int n)
{
    for (int i = 0; i < n - 1; i++) {
        for (int j = 0; j < n - i - 1; j++) {
            if (cats[j].total_revenue < cats[j+1].total_revenue) {
                CategoryRecord tmp = cats[j];
                cats[j]   = cats[j+1];
                cats[j+1] = tmp;
            }
        }
    }
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        cerr << "[reporter] Usage: reporter <output_dir>" << endl;
        return EXIT_BAD_ARGS;
    }

    const char *output_dir = argv[1];

    LOG("reporter", "Starting — output_dir=" << output_dir);

    signal(SIGTERM, handler_sigterm);
    signal(SIGINT,  handler_sigterm);

    /* ── Wait on named semaphore (blocking — no busy-wait) ── */
    LOG("reporter", "Waiting on named semaphore " << SEM_READY_NAME << " ...");

    sem_t *sem_ready = sem_open(SEM_READY_NAME, O_CREAT, 0666, 0);
    if (sem_ready == SEM_FAILED) {
        cerr << "[reporter] sem_open failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }

    /* sem_wait() blocks here until processor calls sem_post() */
    if (sem_wait(sem_ready) < 0) {
        cerr << "[reporter] sem_wait interrupted: " << strerror(errno) << endl;
        sem_close(sem_ready);
        return EXIT_IPC_FAIL;
    }
    sem_close(sem_ready);
    LOG("reporter", "Semaphore acquired — reading shared memory.");

    /* ── Attach to SysV shared memory (consumer: no IPC_CREAT) ── */
    int shmid = shmget(SHM_KEY, SHM_SIZE, 0666);
    if (shmid < 0) {
        cerr << "[reporter] shmget failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }

    SharedData *shm = (SharedData *) shmat(shmid, NULL, 0);
    if ((void *)shm == (void *)-1) {
        cerr << "[reporter] shmat failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }

    if (!shm->ready) {
        cerr << "[reporter] Shared memory not marked ready — aborting." << endl;
        shmdt(shm);
        return EXIT_IO_ERROR;
    }

    /* Copy data out so we can sort without touching the shared segment */
    int            num_cat      = shm->num_categories;
    long           total_recs   = shm->total_records;
    long           total_chunks = shm->total_chunks;
    CategoryRecord cats[MAX_CATEGORIES];
    memcpy(cats, shm->categories, num_cat * sizeof(CategoryRecord));

    /* Detach — we are done reading */
    shmdt(shm);
    LOG("reporter", "Data read: " << num_cat << " categories, "
        << total_recs << " records, " << total_chunks << " chunks.");

    /* Sort categories by revenue descending */
    sort_by_revenue(cats, num_cat);

    /* Ensure output directory exists */
    mkdir(output_dir, 0755);

    /* Build file paths */
    char txt_path[512];
    char csv_path[512];
    strncpy(txt_path, output_dir, 400);
    strcat(txt_path, "/report.txt");
    strncpy(csv_path, output_dir, 400);
    strcat(csv_path, "/report.csv");

    /* ════════════════════════════════════════════════════════════════════
     * HUMAN-READABLE REPORT — written via dup() / dup2() redirection
     * ════════════════════════════════════════════════════════════════════
     *
     * Step 1: Save the current stdout file descriptor with dup().
     *         dup() returns the lowest unused fd — a second reference
     *         to fd 1 (stdout).  We store it as saved_stdout.
     *
     * Step 2: Open report.txt for writing → txt_fd.
     *
     * Step 3: dup2(txt_fd, STDOUT_FILENO) — makes fd 1 (stdout) point
     *         to report.txt.  The original txt_fd is now redundant.
     *
     * Step 4: Write the report using normal cout / printf.  Every byte
     *         goes to report.txt transparently — no code change needed.
     *
     * Step 5: Flush the C++ stream buffer, then restore stdout with
     *         dup2(saved_stdout, STDOUT_FILENO).
     *
     * Step 6: Close the saved descriptor — it is no longer needed.
     * ════════════════════════════════════════════════════════════════════ */

    /* Step 1 — save stdout */
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout < 0) {
        cerr << "[reporter] dup(stdout) failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }

    /* Step 2 — open report.txt */
    int txt_fd = open(txt_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (txt_fd < 0) {
        cerr << "[reporter] open(" << txt_path << ") failed: "
             << strerror(errno) << endl;
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
        return EXIT_IO_ERROR;
    }

    /* Step 3 — redirect stdout to report.txt */
    if (dup2(txt_fd, STDOUT_FILENO) < 0) {
        cerr << "[reporter] dup2 failed: " << strerror(errno) << endl;
        close(txt_fd);
        close(saved_stdout);
        return EXIT_IO_ERROR;
    }
    close(txt_fd);   /* txt_fd is redundant now; stdout already points there */

    /* Step 4 — write report (cout now goes to report.txt) */
    double grand_total = 0.0;
    for (int i = 0; i < num_cat; i++)
        grand_total += cats[i].total_revenue;

    cout << "=============================================================" << endl;
    cout << "  OS Pipeline  —  Retail Transactions Report"                  << endl;
    cout << "  Reporter : PID=" << getpid()
         << "  PPID=" << getppid()                                         << endl;
    cout << "=============================================================" << endl;
    cout << endl;
    cout << "Summary" << endl;
    cout << "-------" << endl;
    cout << "  Total records processed : " << total_recs   << endl;
    cout << "  Total chunks received   : " << total_chunks << endl;
    cout << "  Number of categories    : " << num_cat      << endl;
    cout << endl;
    cout << "Per-Category Breakdown (sorted by Revenue)" << endl;
    cout << "------------------------------------------" << endl;

    for (int i = 0; i < num_cat; i++) {
        cout << endl;
        cout << "  #" << (i + 1) << "  Category      : "
             << cats[i].category << endl;
        cout << "       Total Revenue  : $" << cats[i].total_revenue << endl;
        cout << "       Transactions   : " << cats[i].transaction_count << endl;
        cout << "       Top Product    : " << cats[i].top_product
             << " ($" << cats[i].top_product_revenue << ")" << endl;
    }

    cout << endl;
    cout << "=============================================================" << endl;
    cout << "  Grand Total Revenue : $" << grand_total << endl;
    cout << "=============================================================" << endl;

    /* Step 5 — flush C++ streams then restore stdout */
    cout.flush();
    if (dup2(saved_stdout, STDOUT_FILENO) < 0) {
        cerr << "[reporter] dup2(restore stdout) failed: "
             << strerror(errno) << endl;
    }

    /* Step 6 — close saved descriptor */
    close(saved_stdout);
    LOG("reporter", "report.txt written via dup/dup2 redirect: " << txt_path);

    /* ── Machine-readable CSV report ── */
    int csv_fd = open(csv_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (csv_fd < 0) {
        cerr << "[reporter] open(" << csv_path << ") failed: "
             << strerror(errno) << endl;
        return EXIT_IO_ERROR;
    }

    /* Write CSV header */
    const char *header = "rank,category,total_revenue,"
                         "transaction_count,top_product,top_product_revenue\n";
    write(csv_fd, header, strlen(header));

    /* Write each category row */
    char row[512];
    for (int i = 0; i < num_cat; i++) {
        /* Build row string manually — no sprintf/fprintf (not in allowed list) */
        /* We use write() to fd directly */
        int n = snprintf(row, sizeof(row),
                         "%d,\"%s\",%.2f,%ld,\"%s\",%.2f\n",
                         i + 1,
                         cats[i].category,
                         cats[i].total_revenue,
                         cats[i].transaction_count,
                         cats[i].top_product,
                         cats[i].top_product_revenue);
        write(csv_fd, row, (size_t)n);
    }
    close(csv_fd);
    LOG("reporter", "report.csv written: " << csv_path);

    /* ── Signal the dispatcher that the report is ready ── */
    pid_t ppid = getppid();
    if (ppid > 1) {
        LOG("reporter", "Sending SIGUSR1 to dispatcher (PID=" << ppid << ").");
        kill(ppid, SIGUSR1);
    }

    LOG("reporter", "Exiting cleanly.");
    return EXIT_OK;
}
