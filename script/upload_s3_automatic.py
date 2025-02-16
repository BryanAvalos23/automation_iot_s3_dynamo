import os
import boto3
import time
from botocore.exceptions import NoCredentialsError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

LOCAL_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data")
BUCKET_NAME = "bryanparcial4"
S3_FOLDER = "winds_metrics/"

s3 = boto3.client('s3')

def exist_file_in_s3(bucket, s3_key):
  try:
    s3.head_object(Bucket=bucket, Key=s3_key)
    return True
  except:
    return False

def upload_file_s3(file_path, bucket, s3_key):
  if not exist_file_in_s3(bucket, s3_key):
    try:
      s3.upload_file(file_path, bucket, s3_key)
      print(f"Archivo subido: {file_path}")
    except NoCredentialsError:
      print("Error no se encontraron credenciales de AWS")
  else:
    print(f"El archivo ya existe en s3")


class FileHandler(FileSystemEventHandler):
  
  def on_created(self, event):
    if not event.is_directory:
      file_path = event.src_path
      file_name = os.path.basename(file_path)
      s3_key = S3_FOLDER + file_name

      print(f"Nuevo archivo detectado")
      upload_file_s3(file_path, BUCKET_NAME, s3_key)

def start_monitoring():
  observer = Observer()
  event_handler = FileHandler()

  observer.schedule(event_handler, LOCAL_FOLDER, recursive=False)
  observer.start()

 

  print(f"Monitoreo iniciado en {LOCAL_FOLDER}")
  
  for file in os.listdir(LOCAL_FOLDER):
    print(f"🔍 Encontrado: {file}")
    file_path = os.path.join(LOCAL_FOLDER, file)
    upload_file_s3(file_path, BUCKET_NAME, S3_FOLDER + file)

  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
    print("Monitoreo detenido")

  observer.join()


if __name__ == "__main__":
  start_monitoring()