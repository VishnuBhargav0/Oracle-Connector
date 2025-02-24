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

connection = get_oracle_connection()
cursor = connection.cursor()
offset = "2021-07-29 00:00:00"
poll_interval_mins = 60
archived_logs = get_archived_logs(cursor,offset,poll_interval_mins)

add_logs_to_logminer(cursor,archived_logs)