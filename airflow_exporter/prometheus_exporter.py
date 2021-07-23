import typing
from typing import List, Tuple, Optional, Generator, NamedTuple, Dict
import datetime

from dataclasses import dataclass
import itertools

from sqlalchemy import func
from sqlalchemy import text

from flask import Response, current_app
from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun, Pool
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.samples import Sample

import itertools

POOL_SLOT_TYPES = ['total', 'running', 'queued', 'open']

@dataclass
class PoolSlots:
    pool: str
    cnt: int

def get_pool_stats() -> Dict[str, List[PoolSlots]]:
    pool_stats = Pool.slots_stats()
    return {stat_name: _build_pool_slots(pool_stats, stat_name) for stat_name in POOL_SLOT_TYPES}

def _build_pool_slots(pool_stats: dict, stat_name: str) -> List[PoolSlots]:
    return [PoolSlots(pool=k, cnt=v[stat_name]) for k, v in pool_stats.items()]

@dataclass
class DagScheduleInterval:
    dag_id: str
    cnt: int

def get_dag_schedule_interval() -> List[DagScheduleInterval]:
    assert(Session is not None)

    sql_res = Session.query( # pylint: disable=no-member
        DagModel.dag_id,
        DagModel.schedule_interval
    ).group_by(
        DagModel.dag_id
    ).all()

    filtered_schedule_intervals = select_datetime_schedule_intervals(sql_res)

    res = [
        DagScheduleInterval(
            dag_id = i.dag_id,
            cnt = i.schedule_interval.total_seconds()
        )
        for i in filtered_schedule_intervals
    ]

    return res

def select_datetime_schedule_intervals(schedule_intervals) -> List[datetime.timedelta]:
    return [i  for i in schedule_intervals if isinstance(i.schedule_interval, datetime.timedelta)]

@dataclass
class DagStatusInfo:
    dag_id: str
    status: str
    cnt: int
    owner: str

def get_dag_status_info() -> List[DagStatusInfo]:
    '''get dag info
    :return dag_info
    '''
    assert(Session is not None)

    dag_status_query = Session.query( # pylint: disable=no-member
        DagRun.dag_id,
        DagRun.state,
        func.count(DagRun.state).label('cnt')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    sql_res = Session.query( # pylint: disable=no-member
        dag_status_query.c.dag_id,
        dag_status_query.c.state,
        dag_status_query.c.cnt,
        DagModel.owners,
    ).join(
        DagModel,
        DagModel.dag_id == dag_status_query.c.dag_id
    ).all()

    res = [
        DagStatusInfo(
            dag_id = i.dag_id,
            status = i.state,
            cnt = i.cnt,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res


def get_last_dagrun_info() -> List[DagStatusInfo]:
    '''get last_dagrun info
    :return last_dagrun_info
    '''
    assert(Session is not None)

    last_dagrun_query = Session.query(
        DagRun.dag_id, DagRun.state,
        func.row_number().over(partition_by=DagRun.dag_id,
                               order_by=DagRun.execution_date.desc()).label('row_number')
    ).subquery()

    sql_res = Session.query(
        last_dagrun_query.c.dag_id, last_dagrun_query.c.state, last_dagrun_query.c.row_number,
        DagModel.owners
    ).filter(last_dagrun_query.c.row_number == 1).join(DagModel, DagModel.dag_id == last_dagrun_query.c.dag_id).all()

    res = [
        DagStatusInfo(
            dag_id = i.dag_id,
            status = i.state,
            cnt = 1,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res

@dataclass
class TaskStatusInfo:
    dag_id: str
    task_id: str
    status: str
    cnt: int
    owner: str

def get_task_status_info() -> List[TaskStatusInfo]:
    '''get task info
    :return task_info
    '''
    assert(Session is not None)

    task_status_query = Session.query( # pylint: disable=no-member
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('cnt')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    sql_res = Session.query( # pylint: disable=no-member
        task_status_query.c.dag_id, task_status_query.c.task_id,
        task_status_query.c.state, task_status_query.c.cnt, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()

    res = [
        TaskStatusInfo(
            dag_id = i.dag_id,
            task_id = i.task_id,
            status = i.state or 'none',
            cnt = i.cnt,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res

@dataclass
class DagDurationInfo:
    dag_id: str
    duration: float

def get_dag_duration_info() -> List[DagDurationInfo]:
    '''get duration of currently running DagRuns
    :return dag_info
    '''
    assert(Session is not None)

    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'mysqlconnector':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default':  func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    sql_res = Session.query( # pylint: disable=no-member
        DagRun.dag_id,
        func.max(duration).label('duration')
    ).group_by(
        DagRun.dag_id
    ).filter(
        DagRun.state == State.RUNNING
    ).all()

    res = []

    for i in sql_res:
        if driver in ('mysqldb', 'mysqlconnector', 'pysqlite'):
            dag_duration = i.duration
        else:
            dag_duration = i.duration.total_seconds()

        res.append(DagDurationInfo(
            dag_id = i.dag_id,
            duration = dag_duration
        ))

    return res


def get_dag_elapsed_info() -> List[DagDurationInfo]:
    '''get elapsed time from the last successful DagRuns
    :return dag_info
    '''
    assert(Session is not None)

    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'mysqlconnector':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default':  func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    sql_res = Session.query( # pylint: disable=no-member
        DagRun.dag_id,
        func.min(duration).label('duration')
    ).group_by(
        DagRun.dag_id
    ).filter(
        DagRun.state == State.SUCCESS
    ).all()

    res = []

    for i in sql_res:
        if driver in ('mysqldb', 'mysqlconnector', 'pysqlite'):
            dag_duration = i.duration
        else:
            dag_duration = i.duration.total_seconds()

        res.append(DagDurationInfo(
            dag_id = i.dag_id,
            duration = dag_duration
        ))

    return res


def get_dag_labels(dag_id: str) -> Dict[str, str]:
    # reuse airflow webserver dagbag
    dag = current_app.dag_bag.get_dag(dag_id)

    if dag is None:
        return dict()

    labels = dag.params.get('labels', {})
    labels = labels.get('__var', {})

    return labels


def _add_gauge_metric(metric, labels, value):
    metric.samples.append(Sample(
        metric.name,
        labels,
        value,
        None
    ))


def _build_pool_stat_gauge(pool_stats: List[PoolSlots], stat_name: str) -> Generator[Metric, None, None]:
    pool_stat_metric = GaugeMetricFamily(
        f'airflow_{stat_name}_pool_slots',
        f'Shows the number of {stat_name} pool slots',
        labels=['pool']
    )

    for pool_stat in pool_stats:
        _add_gauge_metric(pool_stat_metric, {'pool': pool_stat.pool}, pool_stat.cnt)

    return pool_stat_metric

class MetricsCollector(object):
    '''collection of metrics for prometheus'''

    def describe(self):
        return []

    def collect(self) -> Generator[Metric, None, None]:
        '''collect metrics'''

        # Pool stats metrics
        pool_stats = get_pool_stats()

        for stat_name in pool_stats.keys():
            yield _build_pool_stat_gauge(pool_stats[stat_name], stat_name)


        # Dag Metrics and collect all labels
        dag_info = get_dag_status_info()

        dag_status_metric = GaugeMetricFamily(
            'airflow_dag_status',
            'Shows the number of dag starts with this status',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in dag_info:
            labels = get_dag_labels(dag.dag_id)

            _add_gauge_metric(
                dag_status_metric,
                {
                    'dag_id': dag.dag_id,
                    'owner': dag.owner,
                    'status': dag.status,
                    **labels
                },
                dag.cnt,
            )

        yield dag_status_metric


        dag_schedule_interval = get_dag_schedule_interval()

        dag_schedule_interval_metric = GaugeMetricFamily(
            'airflow_dag_schedule_interval',
            'Shows the DAG schedule interval in seconds',
            labels=['dag_id', 'schedule_interval']
        )

        for dag in dag_schedule_interval:
            labels = get_dag_labels(dag.dag_id)

            _add_gauge_metric(
                dag_schedule_interval_metric,
                {
                    'dag_id': dag.dag_id,
                    **labels
                },
                dag.cnt,
            )

        yield dag_schedule_interval_metric

        # Last DagRun Metrics
        last_dagrun_info = get_last_dagrun_info()

        dag_last_status_metric = GaugeMetricFamily(
            'airflow_dag_last_status',
            'Shows the status of last dagrun',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in last_dagrun_info:
            labels = get_dag_labels(dag.dag_id)

            for status in State.dag_states:
                _add_gauge_metric(
                    dag_last_status_metric,
                    {
                        'dag_id': dag.dag_id,
                        'owner': dag.owner,
                        'status': status,
                        **labels
                    },
                    int(dag.status == status)
                )

        yield dag_last_status_metric

        # DagRun metrics - airflow_dag_run_duration
        dag_duration_metric = GaugeMetricFamily(
            'airflow_dag_run_duration',
            'Maximum duration of currently running dag_runs for each DAG in seconds',
            labels=['dag_id']
        )
        for dag_duration in get_dag_duration_info():
            labels = get_dag_labels(dag_duration.dag_id)

            _add_gauge_metric(
                dag_duration_metric,
                {
                    'dag_id': dag_duration.dag_id,
                    **labels
                },
                dag_duration.duration
            )

        yield dag_duration_metric

        # DagRun metric - airflow_dag_elapsed_time
        dag_elapsed_metric = GaugeMetricFamily(
            'airflow_dag_elapsed_time',
            'Time from the last successful dag runs for each DAG in seconds (now - start_date)',
            labels=['dag_id']
        )
        for dag_elapsed_info in get_dag_elapsed_info():
            labels = get_dag_labels(dag_elapsed_info.dag_id)

            _add_gauge_metric(
                dag_elapsed_metric,
                {
                    'dag_id': dag_elapsed_info.dag_id,
                    **labels
                },
                dag_elapsed_info.duration
            )

        yield dag_elapsed_metric

        # Task metrics
        task_status_metric = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )

        for dag_id, tasks in itertools.groupby(get_task_status_info(), lambda x: x.dag_id):
            labels = get_dag_labels(dag_id)

            for task in tasks:
                _add_gauge_metric(
                    task_status_metric,
                    {
                        'dag_id': task.dag_id,
                        'task_id': task.task_id,
                        'owner': task.owner,
                        'status': task.status,
                        **labels
                    },
                    task.cnt
                )

        yield task_status_metric


REGISTRY.register(MetricsCollector())

class RBACMetrics(FABBaseView):
    route_base = "/admin/metrics/"
    @FABexpose('/')
    def list(self):
        return Response(generate_latest(), mimetype='text')


# Metrics View for Flask app builder used in airflow with rbac enabled
RBACmetricsView = {
    "view": RBACMetrics(),
    "name": "Metrics",
    "category": "Admin"
}


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = [] # type: ignore
    hooks = [] # type: ignore
    executors = [] # type: ignore
    macros = [] # type: ignore
    admin_views = [] # type: ignore
    flask_blueprints = [] # type: ignore
    menu_links = [] # type: ignore
    appbuilder_views = [RBACmetricsView]
    appbuilder_menu_items = [] # type: ignore
