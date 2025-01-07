# include/notifications.py
from airflow.models import Variable
from datetime import timedelta
import requests

airflow_ip = Variable.get('airflow_ip')

def notify_teams(context) -> None:
    """
    Sends a notification to Microsoft Teams.

    Args:
        context (dict): The Airflow task context.
    """
    print("Sending Teams notification")

    failed_tasks = []
    dag_run = context["dag_run"]
    for task in dag_run.get_task_instances(state='failed'):
        task_id = task.task_id
        task_start_date = (task.start_date- timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
        task_end_date = (task.end_date- timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
        failed_tasks.append((f"task_id: {task_id}", f"start_date: {task_start_date}", f"end_date: {task_end_date}"))
        last_error_log = f'http://{airflow_ip}:8080/' + str(task.log_url).split('/')[-1]
        
    dag_run_date = context['execution_date']
    dt_local = (dag_run_date - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "title": "Airflow Dag Run Failed",
        "summary": f"Task {context['task_instance'].task_id}",
        "themeColor": "0078D7",
        "sections": [
            {
                "activityTitle": "",
                "activitySubtitle": "",
                "facts": [
                    {"name": "Dag Run Start Date:", "value": dt_local},
                    {"name": "Dag:", "value": context['dag'].dag_id},
                    {"name": "Reason:", "value": context['reason']},
                    {"name": "Failed Tasks:", "value": str(failed_tasks)},
                    {"name": "Log URL (last task):", "value": last_error_log},
                ]
            }
        ],
        "potentialAction": [{
            "@type": "OpenUri",
            "name": "View Logs (last task)",
            "targets": [{
                "os": "default",
                "uri": last_error_log
            }]
        }]
    }

    headers = {"content-type": "application/json"}
    requests.post(Variable.get('stack_teams_webhook_secret'), json=payload, headers=headers)
    requests.post(Variable.get('alloha_teams_webhook_secret'), json=payload, headers=headers)
    print("Teams notification sent")
