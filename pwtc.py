import psycopg2
import boto3
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

 

try:
   #boto3
   sqs = boto3.resource('sqs',aws_access_key_id =  'AKIAWNKTUM4AZYHDYFKQ',
                        aws_secret_access_key = '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')
   queue = sqs.create_queue(QueueName='pwtc-project', Attributes={'DelaySeconds': '5'})

   #connecting to postgis
   connection = psycopg2.connect(user="postgres",
                                  password="6162",
                                  host="127.0.0.1")
   connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
   cursor = connection.cursor()

   #create databse
   cursor.execute("drop database if exists pwtc;")

   create_database = """create database pwtc; """
   cursor.execute(create_database)
   connection.commit()

   #create extension postgis
   create_extension_query = """create extension if not exists postgis;"""
   cursor.execute(create_extension_query)
   connection.commit()

   #create tables and indexes
   create_tables_landmarks = """  CREATE TABLE if not exists landmarks
(
  gid serial NOT NULL,
  name character varying(50),
  address character varying(50),
  date_built character varying(10),
  architect character varying(50),
  landmark character varying(10),
  latitude double precision,
  longitude double precision,
  the_geom geometry,
  CONSTRAINT landmarks_pkey PRIMARY KEY (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'POINT'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4326)
)""" 
   cursor.execute(create_tables_landmarks)
   connection.commit()
   create_index_landmarks = """ CREATE INDEX if not exists landmarks_the_geom_gist ON landmarks USING gist (the_geom )"""
   cursor.execute(create_index_landmarks)
   connection.commit()
   
   #insertion of data
   insert_data = """ copy landmarks(name,address,date_built,architect,landmark,latitude,longitude) FROM 'Individual_Landmarks.csv' DELIMITERS ',' CSV HEADER """
   cursor.execute(insert_data)
   connection.commit()

   #sending insertion info to queue
   response = queue.send_message(MessageBody='Landmarks',MessageAttributes={
      'Insertion':{
         'StringValue':'Data Uploaded Successfully!!!',
         'DataType':'String'
         }})
   

   queue = sqs.get_queue_by_name(QueueName='pwtc-project')
   
   #updation of table for POINT
   update_table = """UPDATE landmarks SET the_geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326) """
   cursor.execute(update_table)
   connection.commit()

   #Display near locations
   select_statement = """SELECT distinct
ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), landmarks.the_geom) AS planar_degrees,
name,
architect, latitude, longitude
FROM landmarks
ORDER BY planar_degrees ASC
LIMIT 5 """
   count = 1
   cursor.execute(select_statement)
   connection.commit()
   location_details=[]
   records = cursor.fetchall()
   print("5 closest landmarks to -87.6348345 41.8786207")
   print("*******************")
   for row in records:
       print("Location-" + str(count))
       print("----------")
       print("Planar_Degrees - " + str(row[0]))
       print("Name - " + str(row[1]))
       print("Architect - " + str(row[2]))
       print("Latitude - "+ str(row[3]))
       print("Longitude - "+ str(row[4]))
       print("*******************")
       count +=1
       location_details.append(str(row[0]))
       location_details.append(str(row[1]))
       location_details.append(str(row[2]))
       location_details.append(str(row[3]))
       location_details.append(str(row[4]))

   #sending location data to the queue    
   response = queue.send_message(MessageBody='Landmarks',MessageAttributes={
      'Locations':{
         'StringValue':",".join(location_details),
         'DataType':'String'
         }})
   connection.commit()
   
   
except (Exception, psycopg2.Error) as error :
    if(connection):
        print(error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
