import oracledb
import datetime
from datetime import datetime, timedelta
import os


ORACLE_CONFIG = {
    "user": "username",
    "password": "password",
    "dsn": "host_url/service_name"}

def get_oracle_connection(ORACLE_CONFIG):
    """
    Creates a connection wiht the Oracle database using the ORACLE_CONFIG.
    
    Parameters:
       Oracle config as a dictionary, that contains the user, password, and dsn.
    Returns:
        Connection object.
    """
    
    print("--------------------------------------------")
    print("CONNECTION REQUEST INITIATED")
    connection = oracledb.connect(**ORACLE_CONFIG)
    print("CONNECTION HAS BEEN ESTABLISHED")
    print("--------------------------------------------")
    return connection

def get_archived_logs(cursor,offset,poll_interval_mins):
    """
    Fetches redo log files where FIRST_TIME is greater than the given offset.
    
    Parameters:
        cursor (oracledb.Cursor): Database cursor for executing queries.
        offset (str): The offset in the format 'YYYY-MM-DD HH24:MI:SS' to filter logs by FIRST_TIME.
    
    Returns:
        List[Tuple[str, datetime, datetime]]: List of tuples containing log file name, FIRST_TIME, and NEXT_TIME.
    """
    print("--------------------------------------------")
    print("FETCH ARCHIVED LOGS FUNCTION INITIATED")
    print("executing the below SQL:")
    
    lower_boundary = offset
    upper_boundary = datetime.strptime(offset, '%Y-%m-%d %H:%M:%S')
    upper_boundary = (upper_boundary + timedelta(minutes=poll_interval_mins)).strftime('%Y-%m-%d %H:%M:%S')
    sql = """
    SELECT NAME, FIRST_TIME, NEXT_TIME
    FROM V$ARCHIVED_LOG
    WHERE STATUS = 'A' AND FIRST_TIME > TO_DATE(:1, 'YYYY-MM-DD HH24:MI:SS') AND FIRST_TIME < TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS')
    ORDER BY FIRST_TIME ASC"""

    print(sql)
    print("lower boundary used: " + lower_boundary)
    print("upper boundary used: " + upper_boundary)
    cursor.execute(sql, [lower_boundary, upper_boundary])

    results = cursor.fetchall()
    # for row in results:
    #     print("log file = {} and FIRST_TIME = {} and NEXT_TIME = {}".format(row[0],row[1],row[2]))

    print("A total of " + str(len(results))+ " log files have been identitfied")
    if len(results) != 0:
        max_offset = results[-1][1].strftime('%Y-%m-%d %H:%M:%S')
        print("max time value of the log files:  " + str(max_offset) + " storing this offset in the file.")
        update_last_scn(max_offset,"archived_logs_offset_file.txt")

    print("--------------------------------------------")
    return [row[0] for row in results]

def add_logs_to_logminer(cursor, logs):
    """
    Adds the list of logs recived to the logminer. 
    
    Parameters:
        cursor (oracledb.Cursor): Database cursor for executing queries.
        logs (list): List of log files to be added to the log miner.
   
    """
    for i, log in enumerate(logs):
        options = "DBMS_LOGMNR.NEW" if i == 0 else "DBMS_LOGMNR.ADDFILE"
        print("--------------------------------------------")
        print(f"Iteration {i} - Executing below:")

        plsql_block = f"""
        BEGIN
            DBMS_LOGMNR.ADD_LOGFILE('{log}', {options});
        END;
        """
        if i == 3:
            print("skipping the files from here on")
            return
        
        print(plsql_block)
        try:
            cursor.execute(plsql_block)
        except:
            print("error while adding the file, skipping this file")
            
        
        print(f"Iteration {i} has ended")
        print("--------------------------------------------\n")

def read_offset(file):
    if os.path.exists(file):
        with open(file, "r") as f:
            content = f.read().strip()
            return content if content else 0
    return 0

def update_last_scn(last_scn, file):
    with open(file, "w") as file:
        file.write(str(last_scn))

def start_logminer(cursor, start_scn: int):
    sql = """
        BEGIN 
            DBMS_LOGMNR.START_LOGMNR(
                STARTSCN => {},
                OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG
            ); 
        END;""".format(start_scn)
    
    print("--------------------------------------------")
    print("log_miner_Started at scn: " + str(start_scn))
    print(f"Executing below:")
    print(sql)
    cursor.execute(sql)
    print("started sucesfully")
    print("--------------------------------------------")

def stop_logminer(cursor):
    cursor.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;")
    print("--------------------------------------------")
    print("logminer has been stopped. ")
    print("--------------------------------------------")

def fetch_changes(cursor, batch_size: int = 100000):
    print("--------------------------------------------\n")
    print("fetching changes from V$LOGMNR_CONTENTS")
    print("Ececuting below")

    sql = """
        SELECT SCN, OPERATION, SEG_NAME, ROW_ID, REDO_VALUE, SQL_REDO
        FROM V$LOGMNR_CONTENTS
        WHERE OPERATION IN ('INSERT', 'UPDATE', 'DELETE') AND TABLE_SPACE IN ('USERS') AND ROLLBACK != 1
        FETCH FIRST {} ROWS ONLY""".format(batch_size)

    print(sql)
    cursor.execute(sql)
    print("executed succesfully")
    results = cursor.fetchall()
    print("A total of ",len(results)," rows have been fetched")
    print("--------------------------------------------\n")
    return results

def generate_hevo_record(line):
    print("--------------------------------------------")
    print("GENERATE HEVO RECORD FUNCTION INITIATED")
    operation = line[1]
    table = line[2]
    sql_redo = line[5]

    start = sql_redo.lower().find("values (") + len("values (")
    end = sql_redo.rfind(")")
    values = sql_redo[start:end]
    values = values.replace("'", "").split(",")
    start_cols = sql_redo.find("(") + 1
    end_cols = sql_redo.find(")")
    columns = sql_redo[start_cols:end_cols].replace('"', "").split(",")
    data = dict(zip(columns, values))
    json_result = {
            "table": table.lower(),
            "operation": operation.lower(),
            "data": data}
    print("JSON RESULT: " + str(json_result))
    print("--------------------------------------------")
    return json_result

connection = get_oracle_connection()
cursor = connection.cursor()
offset = "2021-07-29 00:00:00"
poll_interval_mins = 60
archived_logs = get_archived_logs(cursor,offset,poll_interval_mins)
add_logs_to_logminer(cursor,archived_logs)
start_logminer(cursor, 0)
changes = fetch_changes(cursor)
a =[]
for change in changes:
    a.append(generate_hevo_record(change))
#stop_logminer(cursor)

