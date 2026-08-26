```markdown
# CS-2006: Operating Systems — Final Term Project[cite: 1]
## Parallel CSV Data Processing Pipeline (Retail Transactions)[cite: 1, 2]

**Team Members:** Usaid Raza, Haseeb Ullah, M. Hamza Khan[cite: 2]

---

### What We Built
We engineered a multi-process and multi-threaded data processing engine in C++ to ingest, parse, and aggregate retail transaction records from CSV datasets[cite: 1, 2, 8]. Instead of relying on high-level frameworks, the system interacts directly with the Linux kernel using pure POSIX system calls to calculate total revenues and identify top-selling products per category[cite: 1, 2, 8].

---

### How We Built It (System Architecture)
The pipeline is split into four distinct programs, each compiled into an independent executable and orchestrated via a master Bash script (`run.sh`)[cite: 1, 4, 5]:

1. **The Dispatcher (`dispatcher.cpp`) — Master Process:**
   * Initializes Inter-Process Communication (IPC) resources by creating a named pipe (FIFO) via `mkfifo()` and a System V shared memory segment[cite: 1, 6].
   * Spawns child processes using `fork()` and replaces their memory images with the target executables using `execvp()`[cite: 1, 6].
   * Redirects standard output and error streams to per-process log files inside `logs/` using `dup2()`[cite: 1, 6].
   * Monitors child lifecycles efficiently using `sigsuspend()` to avoid CPU-heavy busy-waiting, and cleans up zombie processes using `waitpid()`[cite: 1, 6].

2. **The Ingester (`ingester.cpp`) — Data Producer:**
   * Scans and opens input CSV files sequentially[cite: 1, 7].
   * Reads data in memory chunks, appends a binary header containing a magic number, chunk ID, file ID, and byte count, and writes them into the named FIFO pipe[cite: 1, 7, 10].

3. **The Processor (`processor.cpp`) — Multi-Threaded Engine:**
   * A dedicated reader thread pulls chunks from the FIFO and populates a bounded producer-consumer queue[cite: 1, 8].
   * Creates a configurable pool of worker threads using `pthread_create`, explicitly configured via `pthread_attr_t` for a 1-MB stack size and joinable state[cite: 1, 8].
   * Coordinates queue access using a mutex (`pthread_mutex_t`) and unnamed semaphores (`sem_empty` and `sem_full`)[cite: 1, 8].
   * Parses CSV rows in-place using `strtok_r` and safely updates an aggregation table guarded by a mutex[cite: 1, 8, 10].
   * Serializes the final data into the System V shared memory segment and posts a named POSIX semaphore to notify the reporter[cite: 1, 8].

4. **The Reporter (`reporter.cpp`) — Consumer & Summary Generator:**
   * Blocks cleanly on `sem_wait()` using the named POSIX semaphore, entirely avoiding busy-wait polling[cite: 1, 9].
   * Attaches to the shared memory segment to extract the final aggregated metrics[cite: 1, 9].
   * Demonstrates file descriptor manipulation by saving the original stdout via `dup()`, redirecting stdout to `report.txt` via `dup2()` to write the text report with standard `cout`, and then restoring the original stdout descriptor[cite: 1, 9].
   * Outputs a machine-readable CSV summary to `report.csv` and signals the dispatcher via `SIGUSR1` upon completion[cite: 1, 9].

5. **The Orchestrator (`run.sh`) — Build & Control Script:**
   * Uses `getopts` for dynamic flag parsing, loops for file discovery, case statements, and arithmetic expansions[cite: 1, 5].
   * Installs an exit/interrupt trap that handles cascading termination signals, kills background tasks, and guarantees zero leaked IPC resources[cite: 1, 5].

---

### How to Build & Run

**Build:**
```bash
make

```

*(Produces four binaries: `dispatcher`, `ingester`, `processor`, `reporter`)*

**Run via Orchestrator Script:**

```bash
chmod +x run.sh
./run.sh -i data -o output -n 4 -q 16

```

| Option | Meaning | Default |
| --- | --- | --- |
| `-i DIR` | Input directory with `*.csv` files | *(Required)*<br> |
| `-o DIR` | Output directory for reports | `output`<br> |
| `-n INT` | Number of worker threads | `4`<br> |
| `-q INT` | Bounded queue size | `16`<br> |
| `-c` | Clean build artifacts and exit | —

 |
| `-h` | Help message | —

 |

**Manual Run (Dispatcher directly):**

```bash
./dispatcher data output 4 16 /tmp/os_proj_fifo data/sample.csv

```

---

### Input & Output Formats

* **Input CSV Format (Retail Transactions):**
```csv
category,product_name,price,quantity
Electronics,Laptop Pro,1299.99,2

```


*(Revenue per row = price × quantity)*

* **Output Artifacts:**
* `output/report.txt` (Human-readable text summary via stdout redirection)


* `output/report.csv` (Machine-readable CSV summary)


* `logs/*.log` (Isolated per-process logs)





---

### OS Concepts & Technical Summary

* **Process Management:** `fork()`, `execvp()`, `waitpid()`, `exit()`

* **Inter-Process Communication (IPC):** Named pipes via `mkfifo()`, System V Shared Memory (`shmget`, `shmat`, `shmdt`, `shmctl`)


* **Concurrency & Synchronization:** POSIX threads (`pthread_create`, `pthread_join`, `pthread_attr_t`), mutexes (`pthread_mutex_t`), unnamed semaphores (`sem_empty`, `sem_full`), named semaphores (`sem_open`, `sem_wait`, `sem_post`)


* **Signaling & Control Flow:** POSIX signals (`SIGINT`, `SIGTERM`, `SIGCHLD`, `SIGUSR1`), signal masking, and `sigsuspend()`

* **File Descriptors & I/O:** `open()`, `read()`, `write()`, `close()`, `dup()`, `dup2()`


---

### Shutdown & Resource Cleanup

Pressing `Ctrl-C` triggers `SIGINT`, prompting the dispatcher to cascade `SIGTERM` to all child processes. Files are closed, semaphores and mutexes are destroyed, shared memory segments are unmapped and unlinked, and the FIFO is removed via `unlink()` prior to program exit.

To verify zero resource leaks after execution, check that the following commands return nothing:

```bash
ipcs -m
ls /tmp/os_proj_fifo

```

---

### Approved Header Files Used

`<iostream>`, `<cstring>`, `<cstdlib>`, `<unistd.h>`, `<fcntl.h>`, `<sys/types.h>`, `<sys/wait.h>`, `<sys/stat.h>`, `<sys/shm.h>`, `<signal.h>`, `<pthread.h>`, `<semaphore.h>`, `<errno.h>`

```

```
