```markdown
# CS-2006: Operating Systems — Final Term Project[cite: 1]
## Parallel CSV Data Processing Engine (Retail Transactions)[cite: 1, 2]

**Team Members:** Usaid Raza, Haseeb Ullah, M. Hamza Khan[cite: 2]

---

### Abstract
This project implements a multi-process, multi-threaded data processing pipeline built from scratch in C++[cite: 1, 2]. The system ingests raw CSV datasets, distributes the workload across a dynamically sized thread pool, and aggregates retail metrics (revenue and top products)[cite: 1, 8]. Designed to bypass high-level abstractions, the architecture relies exclusively on low-level POSIX system calls to interact directly with the Linux kernel, demonstrating advanced process management, inter-process communication (IPC), and thread synchronization[cite: 1, 10].

---

### System Architecture & OS Concepts

The pipeline is orchestrated by a master Bash script (`run.sh`) and is divided into four distinct executables[cite: 1, 5].

*   **Process Lifecycle Management:** The master `dispatcher` process dynamically spawns the `ingester`, `processor`, and `reporter` binaries using `fork()` and replaces their memory images via `execvp()`[cite: 1, 6]. To prevent zombie processes, the dispatcher reaps its children using `waitpid()`[cite: 1, 6].
*   **Inter-Process Communication (IPC):** Data flows sequentially across isolated processes[cite: 1]. The `ingester` streams chunked raw data to the `processor` through a named pipe, created via `mkfifo()`[cite: 1, 6, 7]. Once processed, the aggregated data is serialized into System V Shared Memory (`shmget`, `shmat`, `shmdt`, `shmctl`) for the `reporter` to safely access[cite: 1, 2, 8].
*   **Concurrency & The Producer-Consumer Model:** The `processor` maintains a bounded queue to balance I/O reads with computational work[cite: 1, 8]. A pool of worker threads is spun up using `pthread_create`, strictly configured using `pthread_attr_t` to enforce a 1MB stack size and a joinable state[cite: 1, 8]. 
*   **Synchronization Primitives:** Thread-safe queue access is coordinated using unnamed POSIX semaphores (`sem_empty`, `sem_full`)[cite: 1, 8]. A POSIX mutex (`pthread_mutex_t`) guarantees atomic updates to the shared aggregation table, preventing race conditions[cite: 1, 8]. A named POSIX semaphore (`sem_open`, `sem_wait`, `sem_post`) signals the `reporter` when the shared memory segment is populated[cite: 1, 2, 8].
*   **Asynchronous Event Handling (Signals):** The architecture relies heavily on POSIX signals (`SIGINT`, `SIGTERM`, `SIGCHLD`, `SIGUSR1`) for control flow[cite: 1, 2]. The `dispatcher` yields the CPU efficiently using `sigsuspend()` rather than busy-waiting[cite: 1, 6]. Upon receiving a termination signal, the dispatcher cascades a graceful shutdown to all child processes[cite: 1, 6].
*   **File Descriptor Manipulation & I/O:** Standard POSIX I/O (`open`, `read`, `write`, `close`) replaces C++ `<fstream>` for lower-level disk access[cite: 7, 9, 10]. Process output is redirected to log files by saving the original streams with `dup()` and overwriting them with `dup2()` before executing the child binaries[cite: 1, 6, 9]. 

---

### Standard Libraries Used

The implementation strictly restricts itself to the following lab-approved POSIX and C/C++ libraries[cite: 1, 10]:
*   `<unistd.h>`, `<fcntl.h>`, `<sys/types.h>`, `<sys/stat.h>`, `<sys/wait.h>` (Process control, descriptors, I/O)[cite: 10]
*   `<pthread.h>`, `<semaphore.h>` (Threading and synchronization)[cite: 10]
*   `<sys/shm.h>` (System V Shared Memory)[cite: 10]
*   `<signal.h>` (Signal dispositions and masks)[cite: 10]
*   `<iostream>`, `<cstring>`, `<cstdlib>`, `<errno.h>` (Standard utilities, error handling, in-place string parsing via `strtok_r`)[cite: 10]

---

### Build & Execution Instructions

**Compilation:**
```bash
make

```

*Compiles the `dispatcher`, `ingester`, `processor`, and `reporter` binaries using `g++`.*

**Execution (Recommended):**
The pipeline is managed by an orchestrator script that parses arguments, validates dependencies, and traps exit signals for robust cleanup.

```bash
chmod +x run.sh
./run.sh -i data -o output -n 4 -q 16

```

| Flag | Description | Default |
| --- | --- | --- |
| `-i` | Input directory containing `*.csv` files (required) | N/A

 |
| `-o` | Output directory for generated reports | `output`<br> |
| `-n` | Thread pool size | `4`<br> |
| `-q` | Bounded queue capacity | `16`<br> |
| `-c` | Clean build artifacts and exit | N/A

 |

**Data Format (Retail Transactions):**
The engine expects standard comma-separated values calculating revenue as `price × quantity`:
`category,product_name,price,quantity`

---

### Deliverables & Cleanup

Upon completion, the pipeline produces:

1. `output/report.txt`: A human-readable text file generated via standard `cout` redirected by `dup2()`.


2. `output/report.csv`: A machine-readable breakdown of revenue and top products by category.


3. `logs/*.log`: Per-process `stderr`/`stdout` streams isolated for debugging.



**Graceful Termination:**
Interrupting the process (`Ctrl+C`) triggers a structured shutdown. The master script and dispatcher trap `SIGINT`/`SIGTERM`, ensuring all children exit cleanly, FIFOs are unlinked, and shared memory segments are explicitly destroyed.

*Verification:* Running `ipcs -m` and `ls /tmp/os_proj_fifo` after termination will confirm zero leaked resources.

---

### Learning Outcomes

1. **Systems-Level Resource Management:** We learned the absolute necessity of structured teardowns. Failing to trap signals results in orphaned IPC artifacts (stale FIFOs, memory leaks in the kernel), emphasizing the importance of explicit resource deallocation (`shmctl`, `unlink`, `sem_unlink`).


2. **Concurrency & Deadlock Prevention:** By engineering a producer-consumer bounded queue from scratch, we practically applied semaphore ordering and mutex locking, learning firsthand how improper synchronization leads to race conditions or system deadlocks.


3. **Process Orchestration:** We successfully bridged multi-processing (scaling across isolated binaries via pipes/memory) and multi-threading (scaling within a single binary via shared address space), unifying them into a single, cohesive architecture.



```

```