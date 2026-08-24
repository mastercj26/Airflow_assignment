from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

logger = logging.getLogger(__name__)

THREAT_THRESHOLD = 5
HIGH_THREAT = 7
MODERATE_THREAT = 4
FOOD_PER_SURVIVOR = 2
WATER_PER_SURVIVOR = 3
MINIMUM_FOOD = 15
MINIMUM_WATER = 20


def check_perimeter(**context) -> None:
    ti = context["ti"]
    threat_level = random.randint(0, 10)
    if threat_level >= HIGH_THREAT:
        logger.warning("HIGH threat detected. Threat level=%s.", threat_level)
    elif threat_level >= MODERATE_THREAT:
        logger.warning("MODERATE threat detected. Threat level=%s.", threat_level)
    else:
        logger.info("Perimeter appears quiet. Threat level=%s.", threat_level)
    ti.xcom_push(key="threat_level", value=threat_level)


def headcount_survivors(**context) -> None:
    ti = context["ti"]
    survivor_count = random.randint(3, 10)
    logger.info("Current survivors=%s.", survivor_count)
    ti.xcom_push(key="survivor_count", value=survivor_count)


def decide_threat_response(**context) -> str:
    ti = context["ti"]
    threat_level = ti.xcom_pull(task_ids="check_perimeter", key="threat_level")
    if threat_level >= THREAT_THRESHOLD:
        return "defend_bunker"
    return "all_clear_log"


defensive_command = (
    'echo "[DEFENSE] Threat detected near bunker." && '
    'echo "[DEFENSE] Reinforcing barricades." && '
    'echo "[DEFENSE] Defense team moving to perimeter."'
)


def all_clear_log(**context) -> None:
    ti = context["ti"]
    threat_level = ti.xcom_pull(task_ids="check_perimeter", key="threat_level")
    logger.info("Perimeter is currently considered safe. Threat level=%s.", threat_level)


def check_supplies(**context) -> None:
    ti = context["ti"]
    survivor_count = ti.xcom_pull(task_ids="headcount_survivors", key="survivor_count")
    food_available = random.randint(15, 40)
    water_available = random.randint(20, 50)
    if food_available < MINIMUM_FOOD:
        logger.warning("Food supplies are critically low: %s units.", food_available)
    else:
        logger.info("Food supplies are currently sufficient.")
    if water_available < MINIMUM_WATER:
        logger.warning("Water supplies are critically low: %s units.", water_available)
    else:
        logger.info("Water supplies are currently sufficient.")
    ti.xcom_push(key="food_available", value=food_available)
    ti.xcom_push(key="water_available", value=water_available)


def make_survival_decision(**context) -> None:
    ti = context["ti"]
    threat_level = ti.xcom_pull(task_ids="check_perimeter", key="threat_level")
    food_available = ti.xcom_pull(task_ids="check_supplies", key="food_available")
    water_available = ti.xcom_pull(task_ids="check_supplies", key="water_available")
    if threat_level >= HIGH_THREAT:
        logger.critical("FINAL DECISION: DEFEND. High threat detected.")
    elif food_available < MINIMUM_FOOD or water_available < MINIMUM_WATER:
        logger.warning("FINAL DECISION: SCAVENGE. Essential supplies are running low.")
    else:
        logger.info("FINAL DECISION: HIDE. Current conditions do not require exposure.")


default_args = {
    "owner": "bunker_ops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="zombie_survival_dag",
    description="Daily zombie apocalypse survival routine.",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    t_check_perimeter = PythonOperator(
        task_id="check_perimeter",
        python_callable=check_perimeter,
    )

    t_headcount = PythonOperator(
        task_id="headcount_survivors",
        python_callable=headcount_survivors,
    )

    t_branch = BranchPythonOperator(
        task_id="branch_on_threat",
        python_callable=decide_threat_response,
    )

    t_defend_bunker = BashOperator(
        task_id="defend_bunker",
        bash_command=defensive_command,
    )

    t_all_clear = PythonOperator(
        task_id="all_clear_log",
        python_callable=all_clear_log,
    )

    t_check_supplies = PythonOperator(
        task_id="check_supplies",
        python_callable=check_supplies,
        trigger_rule="none_failed_min_one_success",
    )

    t_survival_decision = PythonOperator(
        task_id="survival_decision",
        python_callable=make_survival_decision,
    )

    t_mission_report = BashOperator(
        task_id="mission_report",
        bash_command=(
            'echo "[REPORT] Zombie survival routine completed." && '
            'echo "[REPORT] Perimeter inspection completed." && '
            'echo "[REPORT] Survivor headcount completed." && '
            'echo "[REPORT] Supply assessment completed." && '
            'echo "[REPORT] Final survival decision completed."'
        ),
    )

    t_check_perimeter >> t_branch >> [t_defend_bunker, t_all_clear]
    t_headcount >> t_check_supplies
    [t_defend_bunker, t_all_clear] >> t_check_supplies
    t_check_supplies >> t_survival_decision
    t_survival_decision >> t_mission_report