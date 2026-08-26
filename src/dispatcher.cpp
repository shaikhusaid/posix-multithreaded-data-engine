/*
 * dispatcher.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Master process.  Responsibilities:
 *   1. Create the FIFO (mkfifo) and SysV shared-memory segment (shmget).
 *   2. Install signal handlers for SIGINT, SIGTERM, SIGCHLD, SIGUSR1.
 *   3. Fork three children; each child calls dup2() to redirect its
 *      stdout/stderr to a per-process log file, then execvp() into its binary.
 *   4. Block in a sigsuspend() loop (never busy-wait) until all children exit.
 *   5. On shutdown: forward SIGTERM to children, call waitpid(), remove FIFO,
 *      destroy shared memory and named semaphore.
 *
 * Usage:
 *   ./dispatcher <input_dir> <output_dir> <num_threads> <queue_size>
 *                <fifo_path>
 *
 * Allowed headers: exactly those taught in OS labs (see common.h).
 * Code style     : C++ with "using namespace std;" — never std:: prefix.
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include "common/common.h"

/* ── Global state touched by signal handlers ── */
static volatile sig_atomic_t g_shutdown     = 0;
static volatile sig_atomic_t g_child_exited = 0;
static volatile sig_atomic_t g_usr1_received = 0;

static pid_t g_pid_ingester  = -1;
static pid_t g_pid_processor = -1;
static pid_t g_pid_reporter  = -1;

static char  g_fifo_path[256];
static int   g_shmid = -1;

/* ── Signal handlers ── */

static void handler_sigchld(int sig)
{
    /* Re-register as required by lab manual */
    signal(SIGCHLD, handler_sigchld);
    (void)sig;
    g_child_exited = 1;
}

static void handler_shutdown(int sig)
{
    signal(SIGINT,  handler_shutdown);
    signal(SIGTERM, handler_shutdown);
    (void)sig;
    g_shutdown = 1;
}

static void handler_sigusr1(int sig)
{
    signal(SIGUSR1, handler_sigusr1);
    (void)sig;
    g_usr1_received = 1;
}

/* ── Forward a signal to all living children ── */
static void forward_to_children(int sig)
{
    if (g_pid_ingester  > 0) kill(g_pid_ingester,  sig);
    if (g_pid_processor > 0) kill(g_pid_processor, sig);
    if (g_pid_reporter  > 0) kill(g_pid_reporter,  sig);
}

/* ── Clean up all IPC resources ── */
static void cleanup_ipc()
{
    unlink(g_fifo_path);
    if (g_shmid != -1) {
        shmctl(g_shmid, IPC_RMID, NULL);
        g_shmid = -1;
    }
    sem_unlink(SEM_READY_NAME);
    LOG("dispatcher", "All IPC resources cleaned up.");
}

/*
 * redirect_and_exec()
 * ─────────────────────────────────────────────────────────────────────────────
 * Called inside the child process after fork(), before execvp().
 *
 * Demonstrates dup() and dup2() as required by the project spec:
 *   • dup()  — saves original stdout so we can restore it (or close it cleanly)
 *   • dup2() — redirects both stdout (fd 1) and stderr (fd 2) to a log file
 *
 * After the redirect the child calls execvp(); if that fails the error message
 * is written to the log file (stderr is already redirected there).
 */
static void redirect_and_exec(const char *log_path, char *const argv[])
{
    /* Step 1 — save original stdout via dup() */
    int saved_stdout = dup(STDOUT_FILENO);
    if (saved_stdout < 0) {
        cerr << "[child] dup(stdout) failed: " << strerror(errno) << endl;
        exit(EXIT_IPC_FAIL);
    }

    /* Step 2 — open log file */
    int log_fd = open(log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (log_fd < 0) {
        /* restore stdout before writing the error */
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
        cerr << "[child] open(" << log_path << ") failed: "
             << strerror(errno) << endl;
        exit(EXIT_IPC_FAIL);
    }

    /* Step 3 — redirect stdout and stderr to the log file via dup2() */
    if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) {
        dup2(saved_stdout, STDOUT_FILENO);
        close(saved_stdout);
        close(log_fd);
        cerr << "[child] dup2 failed: " << strerror(errno) << endl;
        exit(EXIT_IPC_FAIL);
    }

    /* log_fd is no longer needed; stdout and stderr now point to the log */
    close(log_fd);
    /* saved_stdout is also no longer needed in this child */
    close(saved_stdout);

    /* Step 4 — exec the target program */
    execvp(argv[0], argv);

    /* If we reach here execvp failed — this goes into the log file */
    cerr << "[child] execvp(" << argv[0] << ") failed: "
         << strerror(errno) << endl;
    exit(EXIT_IPC_FAIL);
}

int main(int argc, char *argv[])
{
    /*
     * argv layout (from run.sh):
     *   argv[1]  = input_dir
     *   argv[2]  = output_dir
     *   argv[3]  = num_threads
     *   argv[4]  = queue_size
     *   argv[5]  = fifo_path
     *   argv[6+] = csv file paths  (collected by run.sh, forwarded to ingester)
     */
    if (argc < 7) {
        cerr << "Usage: " << argv[0]
             << " <input_dir> <output_dir> <num_threads> <queue_size>"
             << " <fifo_path> <csv1> [csv2 ...]"
             << endl;
        return EXIT_BAD_ARGS;
    }

    const char *input_dir   = argv[1];
    const char *output_dir  = argv[2];
    const char *num_threads = argv[3];
    const char *queue_size  = argv[4];
    strncpy(g_fifo_path, argv[5], sizeof(g_fifo_path) - 1);

    /* argv[6..] are the CSV file paths for ingester */
    int   csv_start = 6;
    int   csv_count = argc - csv_start;

    LOG("dispatcher", "Starting — input=" << input_dir
        << " output=" << output_dir
        << " threads=" << num_threads
        << " queue=" << queue_size
        << " csv_files=" << csv_count);

    /* ── 1. Create FIFO ── */
    unlink(g_fifo_path);
    if (mkfifo(g_fifo_path, 0666) < 0) {
        cerr << "[dispatcher] mkfifo failed: " << strerror(errno) << endl;
        return EXIT_IPC_FAIL;
    }
    LOG("dispatcher", "FIFO created at " << g_fifo_path);

    /* ── 2. Create SysV shared memory segment (as taught in lab) ── */
    /* Producer uses IPC_CREAT flag */
    g_shmid = shmget(SHM_KEY, SHM_SIZE, 0666 | IPC_CREAT);
    if (g_shmid < 0) {
        cerr << "[dispatcher] shmget failed: " << strerror(errno) << endl;
        unlink(g_fifo_path);
        return EXIT_IPC_FAIL;
    }
    /* Attach briefly to zero-initialise the segment */
    SharedData *shm = (SharedData *) shmat(g_shmid, NULL, 0);
    if ((void *)shm == (void *)-1) {
        cerr << "[dispatcher] shmat failed: " << strerror(errno) << endl;
        cleanup_ipc();
        return EXIT_IPC_FAIL;
    }
    memset(shm, 0, sizeof(SharedData));
    shmdt(shm);
    LOG("dispatcher", "SysV shared memory created (key=5678, size="
        << sizeof(SharedData) << " bytes).");

    /* ── 3. Install signal handlers ── */
    signal(SIGCHLD, handler_sigchld);
    signal(SIGINT,  handler_shutdown);
    signal(SIGTERM, handler_shutdown);
    signal(SIGUSR1, handler_sigusr1);

    /* Ensure logs and output directories exist */
    mkdir("logs",         0755);
    mkdir(output_dir,     0755);

    /* ── 4. Fork & exec the three children ── */

    /* --- ingester ---
     * Build arg list: ./ingester <fifo_path> <csv1> <csv2> ...
     * csv paths come from argv[csv_start..argc-1].
     */
    g_pid_ingester = fork();
    if (g_pid_ingester < 0) {
        cerr << "[dispatcher] fork(ingester) failed: " << strerror(errno) << endl;
        cleanup_ipc();
        return EXIT_IPC_FAIL;
    }
    if (g_pid_ingester == 0) {
        /* Build ingester argv dynamically:
         *   [0]="./ingester" [1]=fifo_path [2..]=csv paths [last]=NULL  */
        int ing_argc = 2 + csv_count + 1;   /* binary + fifo + csvs + NULL */
        char **ing_args = (char **)malloc((size_t)ing_argc * sizeof(char *));
        if (!ing_args) { cerr << "[ingester-child] malloc failed" << endl; exit(EXIT_IPC_FAIL); }
        ing_args[0] = (char *)"./ingester";
        ing_args[1] = (char *)g_fifo_path;
        for (int k = 0; k < csv_count; k++)
            ing_args[2 + k] = argv[csv_start + k];
        ing_args[ing_argc - 1] = NULL;
        redirect_and_exec("logs/ingester.log", ing_args);
        /* unreachable */
    }
    LOG("dispatcher", "Forked ingester  PID=" << g_pid_ingester);

    /* --- processor --- */
    g_pid_processor = fork();
    if (g_pid_processor < 0) {
        cerr << "[dispatcher] fork(processor) failed: " << strerror(errno) << endl;
        kill(g_pid_ingester, SIGTERM);
        cleanup_ipc();
        return EXIT_IPC_FAIL;
    }
    if (g_pid_processor == 0) {
        char *args[] = {
            (char *)"./processor",
            (char *)g_fifo_path,
            (char *)num_threads,
            (char *)queue_size,
            NULL
        };
        redirect_and_exec("logs/processor.log", args);
    }
    LOG("dispatcher", "Forked processor PID=" << g_pid_processor);

    /* --- reporter --- */
    g_pid_reporter = fork();
    if (g_pid_reporter < 0) {
        cerr << "[dispatcher] fork(reporter) failed: " << strerror(errno) << endl;
        forward_to_children(SIGTERM);
        cleanup_ipc();
        return EXIT_IPC_FAIL;
    }
    if (g_pid_reporter == 0) {
        char *args[] = {
            (char *)"./reporter",
            (char *)output_dir,
            NULL
        };
        redirect_and_exec("logs/reporter.log", args);
    }
    LOG("dispatcher", "Forked reporter  PID=" << g_pid_reporter);

    /* ── 5. sigsuspend() wait loop (no busy-wait) ── */
    sigset_t wait_mask;
    sigfillset(&wait_mask);
    sigdelset(&wait_mask, SIGCHLD);
    sigdelset(&wait_mask, SIGINT);
    sigdelset(&wait_mask, SIGTERM);
    sigdelset(&wait_mask, SIGUSR1);

    int children_alive = 3;
    LOG("dispatcher", "Entering sigsuspend loop waiting for " << children_alive << " children.");

    while (children_alive > 0 && !g_shutdown) {
        sigsuspend(&wait_mask);

        if (g_usr1_received) {
            g_usr1_received = 0;
            LOG("dispatcher", "SIGUSR1 received — reporter has finished writing the report.");
        }

        if (g_child_exited) {
            g_child_exited = 0;
            int status;
            pid_t pid;
            while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
                int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
                if (pid == g_pid_ingester) {
                    LOG("dispatcher", "Ingester  (PID=" << pid << ") reaped, exit=" << code);
                    g_pid_ingester = -1;
                    children_alive--;
                } else if (pid == g_pid_processor) {
                    LOG("dispatcher", "Processor (PID=" << pid << ") reaped, exit=" << code);
                    g_pid_processor = -1;
                    children_alive--;
                } else if (pid == g_pid_reporter) {
                    LOG("dispatcher", "Reporter  (PID=" << pid << ") reaped, exit=" << code);
                    g_pid_reporter = -1;
                    children_alive--;
                }
            }
        }
    }

    /* ── 6. Graceful shutdown if signalled ── */
    if (g_shutdown) {
        LOG("dispatcher", "Shutdown signal — forwarding SIGTERM to all children.");
        forward_to_children(SIGTERM);

        int status;
        pid_t pid;
        while ((pid = waitpid(-1, &status, 0)) > 0) {
            int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            LOG("dispatcher", "Child PID=" << pid << " reaped after shutdown, exit=" << code);
        }
    }

    /* ── 7. Cleanup and exit ── */
    cleanup_ipc();
    LOG("dispatcher", "Pipeline complete — exiting cleanly.");
    return EXIT_OK;
}
