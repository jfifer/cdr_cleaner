#!/usr/bin/python

import sys, getopt
import mysql.connector

class Cleaner(object):
   def __init__(self, username, password, operation="addEvents", host="localhost", db="information_schema"):
      self.host = host
      self.username = username
      self.password = password
      self.db = db
      self.operation = operation
      self.connect()

   def connect(self):
      try:
         #print "Connectiong %s:%s to %s@%s\n" % (self.username, self.password, self.db, self.host)
         self.conn = mysql.connector.connect(user=self.username, password=self.password, host=self.host, database=self.db)
      except mysql.connector.Error as err:
         print("Connection Error: {}".format(err))
         sys.exit(2)
      self.cursor = self.conn.cursor()
      if self.db == "information_schema":
	 self.getCDR()
         self.conn.close()
      else:
         if self.operation == "removeEvents":
            self.removeProc()
            self.removeEvents()
         elif self.operation == "addEvents":
            self.addProc()
            self.addEvent()
         self.conn.close()

   def getCDR(self):
      query = "SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME REGEXP '^gw[0-9]*cdr'";
      self.cursor.execute(query)
      for (SCHEMA_NAME) in self.cursor:
         cleaner = Cleaner(self.username, self.password, self.operation, self.host, SCHEMA_NAME[0])

   def addProc(self):
      query = ("CREATE DEFINER=`root`@`localhost` PROCEDURE `clean_local_cdr`()"
        " BEGIN"
	" DECLARE v_intvl_date, v_now DATETIME;"
        " SET v_now = (select current_timestamp());"
        " SET v_intvl_date = (select date_sub(v_now, interval 2 month));"
        " IF EXISTS ("
   	   " select table_name"
   	   " from information_schema.tables"
   	      " where table_schema = (select schema())"
   	      " and table_name = 'cdr'"
   	" ) THEN"
        " DROP TABLE IF EXISTS local_cdr;"
        " CREATE TABLE local_cdr ("
           " id int(11),"
           " calldate datetime,"
           " clid varchar(80),"
           " src varchar(80),"
           " dst varchar(80),"
           " dcontext varchar(80),"
           " channel varchar(80),"
           " dstchannel varchar(80),"
           " lastapp varchar(80),"
           " lastdata varchar(80),"
           " duration int(11),"
           " billsec  int(11),"
           " disposition varchar(45),"
           " amaflags int(11),"
           " accountcode varchar(20),"
           " uniqueid varchar(32),"
           " userfield varchar(255),"
           " cd_accountcode varchar(255),"
           " cd_branchId int(11),"
           " cd_pathcount int(11),"
           " KEY `calldate` (`calldate`)"
         ") ENGINE = MYISAM"
         " AS "
         " SELECT *"
           " FROM cdr" 
          " WHERE calldate BETWEEN v_intvl_date AND v_now;"         
	" END IF;"
        " END")
      try:
         self.cursor.execute(query)
      except mysql.connector.Error as err:
         print("Query Error: {}".format(err))

   def removeProc(self):
      query = "DROP PROCEDURE IF EXISTS clean_local_cdr";
      self.cursor.execute(query)

   def addEvent(self):
      query = "CREATE EVENT `clean_local_cdr` ON SCHEDULE EVERY 1 DAY STARTS '2017-01-31 02:00:00' ON COMPLETION PRESERVE ENABLE DO CALL clean_local_cdr()";
      self.cursor.execute(query)

   def removeEvents(self):
      query = "DROP EVENT IF EXISTS clean_local_cdr"
      self.cursor.execute(query)

def main(argv):
   username = ''
   password = ''
   host = ''
   operation = ''
   try:
      opts, args = getopt.getopt(argv,"u:p:h:o:",["username=","password=","host=","operation="])
   except getopt.GetoptError:
      print 'cleaner.py -u <username> -p <password> -h <host> -o <operation>'
      sys.exit(2)
   for opt, arg in opts:
      if opt in ("-h", "--host"):
         host = arg
      elif opt in ("-u", "--user"):
         username = arg
      elif opt in ("-p", "--password"):
         password = arg
      elif opt in ("-o", "--operation"):
         operation = arg
   cleaner = Cleaner(username, password, operation, host)

if __name__ == "__main__":
   main(sys.argv[1:])
