import requests
from bs4 import BeautifulSoup
import pandas as pd
import ibm_db_dbi as db
# import ibm_db_sa
# from airflow import DAG
# from airflow.sensors.http_sensor import HttpSensor
# import airflow
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# from sqlalchemy import create_engine

# default_args = {
#     "owner": "User04",
#     "start_date": airflow.utils.dates.days_ago(1),
#     "depends_on_past": False,
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "email": "youremail@host.com",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5)
# }

dsn_driver = "DB2 LUW"
dsn_database = "IBA_EDU"
dsn_hostname = "3d-edu-db.icdc.io"
dsn_port = "8163"
dsn_protocol = "TCPIP"
dsn_uid = "stud04"
dsn_pwd = "12345"

url = 'https://www.bbc.com/sport/football/premier-league/table'

sql_truncate_temp = """TRUNCATE table TEMP_TAB1
 REUSE STORAGE
      IGNORE DELETE TRIGGERS
      IMMEDIATE;
"""

sql_update = """update "EPL_TABLE" as pd
set pd."Team"            = tt."Team_other",
    pd."Drawn"           = tt."Drawn",
    pd."Goal_difference" = tt."Goal_difference",
    pd."Lost"            = tt."Lost",
    pd."For"             = tt."For",
    pd."Against"         = tt."Against",
    pd."Played"          = tt."Played_other",
    pd."Points"          = tt."Points",
    pd."Won"             = tt."Won"
from TEMP_TAB1 as tt
where pd."index" = tt."index";
"""

team_name = []
column_list = []
games_num = []
won_games = []
lost_games = []
drown_g = []
team_score = []
team_miss = []
gd = []
points = []
results = []


def read_from_source():
    response = requests.get('https://www.bbc.com/sport/football/premier-league/table')
    soup = BeautifulSoup(response.text, "html.parser")
    names = soup.findAll('abbr', class_='sp-u-abbr-on sp-u-abbr-off@m')
    for name in names:
        team_name.append(name.get('title'))
    teams = team_name[7:]  # Take only team names

    points = soup.findAll('td',
                          class_='gs-o-table__cell gs-o-table__cell gs-o-table__cell--right gs-o-table__cell--bold '
                                 'gs-u-pr+@l')
    some = soup.findAll('td',
                        class_='gs-o-table__cell gs-o-table__cell gs-o-table__cell--right')  # all points for this class

    for_against_stat = soup.findAll('td',
                                    class_='gs-o-table__cell gs-o-table__cell '
                                           'gs-o-table__cell--right gs-u-display-none gs-u-display-table-cell@m')
    # Parse points for exact list
    for counter in range(0, 2):
        for t in range(counter, len(for_against_stat), 2):
            if counter == 0:
                team_score.append(for_against_stat[t].text)
            if counter == 1:
                team_miss.append(for_against_stat[t].text)

    for i in range(0, 5):
        for g in range(i, len(some), 5):
            if i == 0:
                games_num.append(some[g].text)
            if i == 1:
                won_games.append(some[g].text)
            if i == 2:
                drown_g.append(some[g].text)
            if i == 3:
                lost_games.append(some[g].text)
            if i == 4:
                gd.append(some[g].text)
    point_list = []
    for p in points:
        point_list.append(p.text)

    return pd.DataFrame(
        list(zip(teams, games_num, won_games, drown_g, lost_games, team_score, team_miss, gd, point_list)),
        columns=['Team', 'Played', 'Won', 'Drawn', 'Lost', 'For', 'Against', 'Goal_difference', 'Points'])


# Function to create main table and write data for the first time
# def write_to_db(df):
#     df.to_sql('Epl_Table', engine)


# Read data to df
def read_from_db():
    conn = db.connect('DATABASE=IBA_EDU;'
                      'HOSTNAME=3d-edu-db.icdc.io;'
                      'PORT=8163;'
                      'PROTOCOL=TCPIP;'
                      'UID=stud04;'
                      'PWD=12345;', '', '')
    cur = conn.cursor()

    df = pd.read_sql_query("""SELECT "Team","Played"  FROM "EPL_TABLE" """, conn)
    conn.commit()
    cur.close()
    conn.close()

    return df


# Mid_delta - delta to detect update columns with team which played a match
def __calculate_mid_delta(source_df, db_df):
    return source_df[db_df.ne(source_df).any(axis=1)]


# True_delta - delta which presents data which only update in df
def calculate_true_delta(src_data, db_data):
    mid_delta = __calculate_mid_delta(src_data[['Team', 'Played']], db_data)
    true_delta = mid_delta.join(src_data, lsuffix='_caller', rsuffix='_other')
    del true_delta["Team_caller"], true_delta["Played_caller"]
    return true_delta


# Create temp table to contain delta data. Delta has to be new every time we add it, so we need to truncate temp table
# then update data rows with the same index at db for the reason to update the main table
def update_db(delta):
    conn = db.connect('DATABASE=IBA_EDU;'
                      'HOSTNAME=3d-edu-db.icdc.io;'
                      'PORT=8163;'
                      'PROTOCOL=TCPIP;'
                      'UID=stud04;'
                      'PWD=12345;', '', '')
    cur = conn.cursor()

    cur.execute(sql_truncate_temp)
    delta.to_sql('temp_tab1', conn, if_exists='append')
    cur.execute(sql_update)
    conn.commit()
    cur.close()
    conn.close()


def check(response):
    if response == 200:
        print("Returning True")
        return True
    else:
        print("Returning False")
        return False


if __name__ == '__main__':
    update_db(calculate_true_delta(read_from_source(), read_from_db()))
    # update_db(calculate_true_delta(read_from_source(),read_from_db()))

# update_db(calculate_true_delta(read_from_source(), read_from_db()))
# with DAG(dag_id="u04_champ", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
#     is_champ_available = HttpSensor(
#         task_id="is_champ_available",
#         method="GET",
#         http_conn_id="u04_champ",
#         endpoint="",
#         response_check=lambda response: check(response.status_code),
#         poke_interval=5,
#         timeout=20
#     )
#
#     read_source_data = PythonOperator(
#         task_id="read_source_data",
#         provide_context=True,
#         python_callable=read_from_source
#
#     )
#     read_db_data = PythonOperator(
#         task_id="read_db_data",
#         provide_context=True,
#         python_callable=read_from_db
#     )
#     update_db_data = PythonOperator(
#         task_id="to_db",
#         provide_context=True,
#         python_callable=update_db,
#         op_kwargs={'delta': calculate_true_delta(read_from_source(), read_from_db())}
#     )
#     is_champ_available >> read_source_data >> read_db_data >> update_db_data
