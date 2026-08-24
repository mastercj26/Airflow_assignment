<img width="1888" height="687" alt="image" src="https://github.com/user-attachments/assets/ffa38d62-d14b-49e1-bf41-d3899380c2e1" />


# 🧟 Zombie Survival DAG — README
**Apache Airflow Assignment**
**Author:** Chetan Jain
**File:** `zombie_survival_dag.py`

---

## 1. Overview

This DAG automates a twice-daily survival routine for a small group of bunker survivors during a zombie apocalypse. Instead of humans manually running an 8-step checklist at dawn and dusk, the pipeline checks the perimeter, takes a headcount, decides whether to fight or hide, verifies supplies, and produces a shift report — automatically, every single shift.

---

## 2. Task Flow & Design Rationale

The DAG contains exactly **6 tasks** in the following order. `scan_perimeter` and `count_survivors` run in **parallel** first (neither depends on the other). Everything else is sequential.

| Task ID | Operator | Purpose |
|---|---|---|
| `scan_perimeter` | PythonOperator | Simulates a perimeter sensor; generates a `threat_level` (0–100) and pushes it via XCom. |
| `count_survivors` | PythonOperator | Simulates a radio roll-call; records `survivor_count` and pushes it via XCom. |
| `decide_strategy` | BranchPythonOperator | Reads `threat_level` from XCom and routes to either `engage_zombies` or `hide_and_wait`. |
| `engage_zombies` | PythonOperator | Active defense protocol. Runs only when `threat_level ≥ 50`. Skipped otherwise. |
| `hide_and_wait` | BashOperator | Stealth protocol (bash echo + sleep). Runs only when `threat_level < 50`. Skipped otherwise. |
| `check_supplies` | PythonOperator | Counts supply units; logs a CRITICAL alert if below threshold. Runs after whichever branch executed. |
| `generate_daily_report` | PythonOperator | Pulls all XCom values and logs a complete end-of-shift summary. |

### Why this design?
- **Parallel start (tasks 1 & 2):** the perimeter scan and headcount are independent — running them simultaneously saves time when every second matters.
- **Branch (task 3):** a real survival situation has two modes — fight or hide. `BranchPythonOperator` models this naturally and guarantees exactly one path executes.
- **BashOperator for `hide_and_wait`:** the "hide" action is a simple shell command (lock doors, kill lights) — BashOperator is the cleanest fit for this.
- **`trigger_rule=ONE_SUCCESS` on tasks 5 & 6:** these must run regardless of which branch was taken, so the default `ALL_SUCCESS` would incorrectly block them after a skip.

---

## 3. XCom Data Passed Between Tasks

| XCom Key | Pushed by → Consumed by | Why |
|---|---|---|
| `threat_level` | `scan_perimeter` → `decide_strategy` + `generate_daily_report` | The branch operator needs this to decide which path to take. The report also logs it for audit purposes. |
| `survivor_count` | `count_survivors` → `generate_daily_report` | Headcount is a critical end-of-shift metric; if it drops below 4, the report highlights the warning. |
| `supply_count` | `check_supplies` → `generate_daily_report` | Supplies are checked mid-run but surfaced in the consolidated report, which is the right place to raise a critical shortage alert. |

XCom was chosen over global variables or filesystem writes because it is native to Airflow, visible in the UI, safely retried, and scoped to a single DAG run.

---

## 4. Skip Condition

The branching task (`decide_strategy`) reads `threat_level` from XCom and returns exactly one downstream `task_id`:

- If `threat_level ≥ 50` → **`engage_zombies` runs**; `hide_and_wait` is **skipped** (shown pink/grey in the Graph view).
- If `threat_level < 50` → **`hide_and_wait` runs**; `engage_zombies` is **skipped**.

This is implemented with a `BranchPythonOperator`, which is Airflow's built-in mechanism for conditional task execution. Skipped tasks are logged with an explanation so a reviewer reading the logs later knows exactly why a branch was not taken.

---

## 5. Schedule Choice

```
schedule_interval = "0 6,18 * * *"
```

The cron expression triggers the DAG **twice a day — at 06:00 (dawn patrol) and 18:00 (dusk lockdown)**. These are the two highest-risk transition points in a zombie scenario:
- **Dawn** — visibility returns; the group must decide whether to scavenge or defend.
- **Dusk** — the group must lock down before visibility drops.

A generic `@daily` (midnight) would run at the least useful time for a survival routine. Dawn and dusk align directly with the story and make the pipeline's purpose self-evident to anyone reading the schedule.

---

## 6. How to Trigger via the Airflow REST API

As required, the DAG is triggered via the Airflow REST API (not the UI button):

```bash
# Using curl
curl -X POST http://localhost:8080/api/v1/dags/zombie_survival_dag/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"conf": {}}'
```

**Using Postman:**
- Method: `POST`
- URL: `http://localhost:8080/api/v1/dags/zombie_survival_dag/dagRuns`
- Authorization: Basic Auth → `admin` / `admin`
- Body → raw → JSON: `{"conf": {}}`

The response contains a `dag_run_id` and `state: "queued"`, confirming the run was accepted.

---

## 7. Deliverables Checklist

- [x] `zombie_survival_dag.py` — well-commented, PEP8-compliant DAG file
- [x] Screenshot: DAG Graph view showing a completed run with the skipped task visibly marked in pink/grey
- [x] Screenshot: Airflow REST API trigger request and its JSON response showing `dag_run_id` and `state: queued`
- [x] This README

---


