```markdown
# CS-2006: Operating Systems — Final Term Project[cite: 1]
## Parallel CSV Data Processing Pipeline (Retail Transactions)[cite: 1, 2]

**Team Members:** Usaid Raza, Haseeb Ullah, M. Hamza Khan[cite: 2]

---

### Overview
A multi-process and multi-threaded data processing pipeline built in C++ using core POSIX system calls[cite: 1, 2]. The program ingests CSV datasets, streams them via a named pipe, processes them through a thread pool using a bounded queue, aggregates retail metrics, and outputs reports via shared memory[cite: 1, 2, 8].

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

### OS Concepts & Architecture

* **Process Lifecycle:** The `dispatcher` spawns the `ingester`, `processor`, and `reporter` using `fork()` and replaces their process images via `execvp()`. It reaps children with `waitpid()` to prevent zombie processes.


* **Inter-Process Communication (IPC):**
* **Named Pipe (FIFO):** Created via `mkfifo()` to stream raw chunks from `ingester` to `processor`.


* **System V Shared Memory:** `shmget`, `shmat`, `shmdt`, and `shmctl` are used to pass the final aggregated table from `processor` to `reporter`.




* **Multi-Threading & Synchronization:**
* **Thread Pool:** Created using `pthread_create` with a custom `pthread_attr_t` (joinable state, 1MB stack size).


* **Bounded Queue:** A producer-consumer queue managed with a mutex (`pthread_mutex_t`) and unnamed semaphores (`sem_empty`, `sem_full`).


* **Data Protection:** Table updates are guarded by a mutex to prevent race conditions.


* **Inter-Process Synchronization:** A named POSIX semaphore (`sem_open`, `sem_wait`, `sem_post`) allows the `reporter` to wait for the `processor` without busy-waiting.




* **Signals & Control Flow:** Uses `SIGINT`, `SIGTERM`, `SIGCHLD`, and `SIGUSR1` for clean shutdowns and status dumps. The dispatcher uses `sigsuspend()` instead of busy-waiting.


* **File Descriptors & I/O:** Uses `dup()` and `dup2()` to redirect standard output/error to per-process log files. The `reporter` also uses `dup()` and `dup2()` to temporarily redirect stdout to `report.txt` before restoring it. Low-level POSIX I/O (`open`, `read`, `write`, `close`) is used throughout.



---

### Input & Output Formats

* **Input CSV Format:**
```csv
category,product_name,price,quantity
Electronics,Laptop Pro,1299.99,2

```


*(Revenue = price × quantity)*

* **Output Files:**
* `output/report.txt` (Human-readable text summary via stdout redirection)


* `output/report.csv` (Machine-readable CSV summary)


* `logs/*.log` (Isolated per-process logs)





---

### Shutdown & Cleanup

Pressing `Ctrl-C` sends `SIGINT`, prompting the dispatcher to forward `SIGTERM` to all children. Files close, semaphores and mutexes destroy, shared memory unmaps and unlinks, and the FIFO unlinks via `unlink()` before exit.

Verify zero resource leaks after running:

```bash
ipcs -m
ls /tmp/os_proj_fifo

```

---

### Approved Header Files Used

`<iostream>`, `<cstring>`, `<cstdlib>`, `<unistd.h>`, `<fcntl.h>`, `<sys/types.h>`, `<sys/wait.h>`, `<sys/stat.h>`, `<sys/shm.h>`, `<signal.h>`, `<pthread.h>`, `<semaphore.h>`, `<errno.h>`

```

```
