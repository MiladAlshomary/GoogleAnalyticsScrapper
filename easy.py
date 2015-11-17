import argparse
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
import json
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep
import MySQLdb as mdb
import logging


logging.basicConfig()

def get_service(api_name, api_version, scope, key_file_location,
                service_account_email):
  """Get a service that communicates to a Google API.

  Args:
    api_name: The name of the api to connect to.
    api_version: The api version to connect to.
    scope: A list auth scopes to authorize for the application.
    key_file_location: The path to a valid service account p12 key file.
    service_account_email: The service account email address.

  Returns:
    A service that is connected to the specified API.
  """

  f = open(key_file_location, 'rb')
  key = f.read()
  f.close()

  credentials = SignedJwtAssertionCredentials(service_account_email, key,
    scope=scope)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  service = build(api_name, api_version, http=http)

  return service


def get_first_profile_id(service):
  # Use the Analytics service object to get the first profile id.

  # Get a list of all Google Analytics accounts for this user
  accounts = service.management().accounts().list().execute()

  if accounts.get('items'):
    # Get the first Google Analytics account.
    account = accounts.get('items')[0].get('id')

    # Get a list of all the properties for the first account.
    properties = service.management().webproperties().list(
        accountId=account).execute()

    if properties.get('items'):
      # Get the first property id.
      property = properties.get('items')[0].get('id')

      # Get a list of all views (profiles) for the first property.
      profiles = service.management().profiles().list(
          accountId=account,
          webPropertyId=property).execute()

      if profiles.get('items'):
        # return the first view (profile) id.
        return profiles.get('items')[0].get('id')

  return None


def get_results(service, profile_id, start, end, metrics, dimensions, segments):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the number of sessions within the past seven days.
  if not segments :
    return service.data().ga().get(
        ids='ga:' + profile_id,
        start_date=start,#'1daysAgo',
        end_date=end,#'today',
        metrics= metrics,
        dimensions=dimensions).execute()
  else :
    return service.data().ga().get(
        ids='ga:' + profile_id,
        start_date=start,#'1daysAgo',
        end_date=end,#'today',
        metrics= metrics,
        segment= 'dynamic::' + segments,
        dimensions=dimensions).execute()


def save_results(results, conf, db_conn, seg_value='all') :
  #table name
  table_name  = conf["db_conf"]['table_name']
  column_map  = conf["db_conf"]["column_map"]

  columns = map(lambda clm :  column_map[clm['name']], results['columnHeaders'])
  columns.append('segment')
  columns_str = ','.join(columns)
  for row in results['rows'] :
    cur = db_conn.cursor()
    row  = map(lambda item : '\'' + item + '\'' , row)
    row.append('\'' + seg_value + '\'')
    values = ','.join(row)
    sql    = "INSERT INTO " + table_name + "(" + columns_str + ") VALUES (" + values + ")"
    cur.execute(sql)

  db_conn.commit()


def executeJob(service, config, db_conn):
  print "Am working..."
  ga_info = config["ga_info"]

  #number of daysago to scrap
  days = str(config["interval"])

  #prepare semgents
  segments = ga_info['segments']
  if not segments :
    #get the results
    results = get_results(service, config['profile'], days + 'daysAgo' , 'today', ','.join(ga_info['metrics']), ','.join(ga_info['dimensions']), '')
    save_results(results, config, db_conn)
  else :
    for segment in segments :
      s_name, s_val = segment.split("::")
      results = get_results(service, config['profile'], days + 'daysAgo' , 'today', ','.join(ga_info['metrics']), ','.join(ga_info['dimensions']), s_val)
      save_results(results, config, db_conn, s_name)


def main():
  # Load configuration file
  with open('./conf.json') as data_file:
    config = json.load(data_file)

  service_email     = config['service_email']
  key_file_location = config['key_file_location']
  profile_name      = config['profile']
  interval          = config['interval']
  db_conf           = config['db_conf']


  # Define the auth scopes to request.
  scope = ['https://www.googleapis.com/auth/analytics.readonly']
  # Authenticate and construct service.
  service = get_service('analytics', 'v3', scope, key_file_location, service_email)

  #connection to db
  con = mdb.connect(db_conf['db_server'], db_conf['db_user'], db_conf['db_password'], db_conf['db_name']);

  scheduler = BackgroundScheduler()
  scheduler.add_job(executeJob, 'interval',  kwargs={'service' : service, 'config' : config, 'db_conn' : con }, days = interval)
  scheduler.start()

if __name__ == '__main__':
  main()
  while True:
    sleep(2)
    print('.')