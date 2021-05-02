# coding: utf8
import cx_Oracle
import pymysql
import redis

class RedisConnection:

    def __init__(self, cfg):
        redis_url = cfg.get("REDIS_URL")
        self.db_conn(redis_url)

    def db_conn(self, redis_url):
        self.conn = redis.ConnectionPool.from_url(redis_url)
        self.db = redis.StrictRedis(connection_pool=self.conn)

    def db_rpush(self, key_name, value):
        self.db.rpush(key_name, value)


class OracleConnection:

    def __init__(self, logs, cfg):
        self.logs = logs
        self.db_conn(cfg)

    def db_conn(self, cfg):
        host = cfg.get("HOST")
        port = cfg.get("PORT")
        dbName = cfg.get("DB")
        username = cfg.get("USERNAME")
        password = cfg.get("PASSWD")
        dsn = cx_Oracle.makedsn(host, port, dbName)

        self.conn = cx_Oracle.connect(username, password, dsn)
        self.db = cx_Oracle.Cursor(self.conn)

    def insert_data(self, item_info, table_name):

        keys = ', '.join(list(item_info.keys()))
        values = ', '.join([f":{i}" for i in range(1, len(item_info) + 1)])
        insert_sql = "INSERT INTO {}({})VALUES({})".format(table_name, keys, values)
        try:
            self.db.execute(insert_sql, tuple(item_info.values()))
            self.conn.commit()
            self.logs.info("【数据存储成功】-{}".format(item_info))
        except Exception as e:
            self.logs.error("【数据存储失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def select_data(self, table_name, search_keys, cond_item=None):

        SELECT_SQL = "SELECT DISTINCT {} FROM {}".format(", ".join(search_keys), table_name)
        if cond_item:
            cond_str_list = []
            for key, value in cond_item.items():
                cond_str_list.append(f"{key}='{value}'")

            cond_str = " AND ".join(cond_str_list)
            SELECT_SQL += f" WHERE {cond_str}"

        self.db.execute(SELECT_SQL)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False

    def update_data(self, item_info, condition_item, table_name):
        VALUE_STR = ", ".join([f"{key}='{value}'" for key, value in item_info.items()])
        string_list = []
        for i in condition_item.keys():
            string = "%s='%s'" % (i, condition_item.get(i))
            string_list.append(string)
        CON_STR = ' AND '.join(string_list)
        update_sql = "UPDATE {} SET {} WHERE {}".format(table_name, VALUE_STR, CON_STR)
        try:
            self.db.execute(update_sql)
            self.conn.commit()
            self.logs.info("【数据已存在，更新成功-{}】".format(item_info))
        except Exception as e:
            self.logs.error("【数据已存在，数据更新失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def select_category(self):
        select_sql = "SELECT SITE_ID, LINK FROM {} WHERE CATID IS NOT NULL".format(self.oracle_table_name_category)
        self.db.execute(select_sql)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False


class MySqlConnection:

    def __init__(self, logs, cfg):
        self.sql_conn(cfg)
        self.logs = logs

    def sql_conn(self, cfg):
        host = cfg.get("HOST")
        port = cfg.get("PORT")
        dbName = cfg.get("DB")
        username = cfg.get("USERNAME")
        password = cfg.get("PASSWD")
        self.conn = pymysql.connect(host=host, port=int(port), db=dbName, user=username, password=password)
        self.db = self.conn.cursor()

    def select_data(self, table_name, search_keys=None, cond_item=None):
        if not search_keys:
            search_keys = ["*"]
        SELECT_SQL = "SELECT DISTINCT {} FROM {}".format(", ".join(search_keys), table_name)
        if cond_item:
            cond_str_list = []
            for key, value in cond_item.items():
                cond_str_list.append(f"{key}='{value}'")

            cond_str = " AND ".join(cond_str_list)
            SELECT_SQL += f" WHERE {cond_str}"

        self.db.execute(SELECT_SQL)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False

    def insert_data(self, item_info, table_name):
        keys = ', '.join(list(item_info.keys()))
        values = ', '.join([f"%s" for i in range(len(item_info))])
        insert_sql = "INSERT INTO {}({})VALUES({})".format(table_name, keys, values)
        try:
            self.db.execute(insert_sql, tuple(item_info.values()))
            self.conn.commit()
            self.logs.info("【数据存储成功】-{}".format(item_info))
        except Exception as e:
            self.logs.error("【数据存储失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def update_data(self, item_info, condition_item, table_name):
        VALUE_STR = ", ".join([f"{key}='{value}'" for key, value in item_info.items()])
        string_list = []
        for i in condition_item.keys():
            string = "%s='%s'" % (i, condition_item.get(i))
            string_list.append(string)
        CON_STR = ' AND '.join(string_list)
        update_sql = "UPDATE {} SET {} WHERE {}".format(table_name, VALUE_STR, CON_STR)
        try:
            self.db.execute(update_sql)
            self.conn.commit()
            self.logs.info("【数据已存在，更新成功-{}】".format(item_info))
        except Exception as e:
            self.logs.error("【数据已存在，数据更新失败】-{}-{}".format(e, item_info))
            self.conn.rollback()

    def select_data_mqd(self, table_name):

        select_sql = "select platform , store_name ,product_link from {} where platform != '淘宝天猫' group by platform ,store_name".format(
            table_name)
        self.db.execute(select_sql)
        res = self.db.fetchall()
        if res:
            return res
        else:
            return False
