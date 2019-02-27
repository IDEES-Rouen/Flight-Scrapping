import psycopg2
import json

# from stackoverflow
# https://stackoverflow.com/questions/32812463/setting-schema-for-all-queries-of-a-connection-in-psycopg2-getting-race-conditi

class DatabaseCursor(object):

    def __init__(self, conn_config_file):
        with open(conn_config_file) as config_file:
            self.conn_config = json.load(config_file)

    def __enter__(self):
        self.conn = psycopg2.connect(
            "dbname='" + self.conn_config['dbname'] + "' " +
            "user='" + self.conn_config['user'] + "' " +
            "host='" + self.conn_config['host'] + "' " +
            "password='" + self.conn_config['password'] + "' " +
            "port=" + self.conn_config['port'] + " "
        )
        self.cur = self.conn.cursor()

        return self.cur

    def __exit__(self, exc_type, exc_val, exc_tb):
        # some logic to commit/rollback
        self.conn.close()