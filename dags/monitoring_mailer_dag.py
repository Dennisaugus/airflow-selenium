"""
    DAG for reporting access to the application by e-mail.

    The mail in sent using the default configured SMTP credentials, present in the
    environment variables.

    For setting up the receivers of the e-mail, modify the airflow Variable
    MONITORING_MAIL_TO, which should be a list os strings with double quotes.

    There's also a second Variable MONITORING_MAIL_TO_STG, to use in str running.
"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow import DAG

from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import jinja2
import os


MAIL_SUBJECT = "PAT - Weekly usage report"
TEMPLATE = """
    <html>
    <head>
    </head>
    <body>
        <h3>{{ header_text }}</h3>
        <div style="content-align: center;">
            <img  src="{{ access_chart }}"/>
        </div>

        <table style="width: 80%; border: 1px solid black; border-collapse: collapse;">
            <tr>
                {% for date in table.header %}
                <td style="border: 1px solid black; text-align: center;">{{ date }}</td>
                {% endfor %}
            </tr>
            {% for row in table.rows %}
            <tr>
                {% for col in row %}
                {% if col %}
                    <td style="border: 1px solid black; text-align: center;">{{ col }}</td>
                {% else %}
                    <td style="border: 1px solid black; text-align: center;"></td>
                {% endif %}
                {% endfor %}
            </tr>
            {% endfor %}
        </table>

        <small>{{ footer_text }}</small>
    </body>
    </html>
"""


def create_server_connection(conn_id: str = "sys_tactics_prd_airflow"):
    """Connect to the Database"""
    config = BaseHook.get_connection(conn_id)

    try:
        connection = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.schema,
            user=config.login,
            password=config.password,
        )
        print("Database connection successful")

    except Exception as error:
        print(f"Error connecting to database: {error}")
        raise error

    return connection


def get_last_week_days(format: str = "%Y-%m-%d") -> list:
    """Return list of the days for the last week in YYYY-MM-DD format"""
    week_ago_day = datetime.now() - timedelta(days=7)
    delta_days = 1 + week_ago_day.weekday()
    last_week_sunday = week_ago_day - timedelta(days=delta_days)

    last_week_days = [
        (last_week_sunday + timedelta(days=i)).strftime(format) for i in range(7)
    ]

    return last_week_days


def get_last_week_data() -> list:
    """Get current week access data, from sunday to saturday"""
    last_week_days = get_last_week_days()
    connection = create_server_connection()

    sql_last_week_days = "("
    for idx, day in enumerate(last_week_days):
        if idx == len(last_week_days) - 1:
            sql_last_week_days = sql_last_week_days + "'" + day + "')"
            break

        sql_last_week_days = sql_last_week_days + "'" + day + "', "

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT user_email, TO_CHAR(log_date, 'YYYY-MM-DD') as date
                FROM user_logs
                WHERE TO_CHAR(log_date, 'YYYY-MM-DD') in {sql_last_week_days}
                GROUP BY log_date, user_email
                """
            )
            result = cursor.fetchall()

        return result

    except Exception as error:
        print("get_last_week_data ['user_logs']")
        raise error

    finally:
        cursor.close()


def generate_message(data: list, chart_cid: str) -> str:
    """Generate the message body"""
    template = jinja2.Environment().from_string(TEMPLATE)

    return template.render(get_template_context(data, chart_cid))


def get_template_context(data: list, chart_cid: str) -> dict:
    """Generate the message content

    Parameters
    ----------
    file : _TemporaryFileWrapper
        File to write the chart

    Returns
    -------
    dict
    """
    last_week_days = get_last_week_days()
    table_header = ["Users"] + [day for day in last_week_days]

    unique_users = list(set([day[0] for day in data]))
    unique_users.sort()

    table_rows = []
    for user in unique_users:
        days_column = list()

        for day in last_week_days:
            tam = len(days_column)

            for input in data:
                if user == input[0] and day == input[1]:
                    days_column.append("X")

            if len(days_column) == tam:
                days_column.append("")

        table_rows.append([user] + days_column)

    return {
        "header_text": "Weekly access report",
        "access_chart": f"cid:{chart_cid}",
        "table": {"header": table_header, "rows": table_rows},
        "footer_text": "To stop receiving this or any other problem,"
        + " please contact the system admin",
    }


def gen_chart(data: list, file: _TemporaryFileWrapper):
    """Save chart to temporary rfile

    Parameters
    ----------
    data : list
    file : _TemporaryFileWrapper
    """
    last_week_days = get_last_week_days()

    count_of_days = list()
    for day in last_week_days:
        count = 0

        for input in data:
            if day == input[1]:
                count += 1

        count_of_days.append(count)

    chart_data = {
        "day": last_week_days,
        "count": count_of_days,
    }

    g = sns.lineplot(data=chart_data, x="day", y="count")
    g.set(yticks=range(0, 1 + max(chart_data["count"])))
    g.set_title("Access count by day")
    plt.xticks(rotation=45)
    fig = plt.gcf()

    fig.savefig(file, format="png", bbox_inches="tight")


def mount_and_send_mail(**context):
    """Mount email and push as XCOM for the mail operator send"""
    with NamedTemporaryFile("wb+", suffix=".png") as file:
        data = get_last_week_data()

        gen_chart(data, file)
        content = generate_message(data, chart_cid=os.path.basename(file.name))

        email_op = EmailOperator(
            task_id="send_email",
            to=Variable.get("MONITORING_MAIL_TO"),
            subject=MAIL_SUBJECT,
            html_content=content,
            files=[file.name],
        )
        email_op.execute(context)


"""
DAG config
"""
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 11, 7, 8, 0, 0),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="monitoring_mailer_dag",
    schedule_interval="0 08 * * MON",
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    catchup=False,
)


"""
Tasks
"""
mailer_task = PythonOperator(
    task_id="mailer_task",
    python_callable=mount_and_send_mail,
    provide_context=True,
    dag=dag,
)
