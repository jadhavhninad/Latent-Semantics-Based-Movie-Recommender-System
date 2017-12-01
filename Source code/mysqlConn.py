import MySQLdb

# open a database connection
# be sure to change the host IP address, username, password and database name to match your own

class DbConnect():
    def __init__(self):
        #Initail tests done here
        self.conn = MySQLdb.connect(host='localhost', user='root', passwd='abc123', db='mwd_phase3')

        #For testing Phase2 - MODEL-1
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='abc123', db='mwd_phase2_test1')

        #For testing Phase2 - MODEL-2
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='abc123', db='mwd_phase2_test2')

        # For testing Phase2 - MODEL-3
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='abc123', db='mwd_phase2_test3')

        #Testing with testData
        #self.conn = MySQLdb.connect(host='localhost', user='root', passwd='abc123', db='mwd_phase2_sub1')

        self.cur = self.conn.cursor()

    def get_connection(self):
        return self.conn

    def db_query(self):
        query = "show tables"
        self.cur.execute(query)
        self.result = self.cur.fetchone()
        return self.result

    def db_exit(self):
        self.conn.close()

if __name__ == "__main__":
    db_conn = DbConnect()
    value = db_conn.db_query()
    print(value)
    db_conn.db_exit()





