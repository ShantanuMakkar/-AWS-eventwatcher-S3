import boto3
import os
from boto3 import resource, client
import botocore
from datetime import datetime


def lambda_handler(event, context):
    
        
    def initialize_objects_and_varibales():
        global SOURCE_BUCKET_NAME
        global FILE_NAME
        global FILE_NAME_WITH_DIRECTORY
        global dt
        global File_PREFIX_DATE
        
        dt = datetime.now()
        File_PREFIX_DATE = dt.strftime('%Y%m%d')
        FILE_PREFIX_DIRECTORY = os.getenv("bucket_sub_directory")
        FILE_SUFFIX = os.getenv("file_suffix")
        SOURCE_BUCKET_NAME = os.getenv("bucket_name")
        FILE_TYPE = os.getenv('fileType')
        
        if FILE_PREFIX_DIRECTORY == 'False':
            
            if FILE_TYPE == 'daily':
                FILE_NAME = File_PREFIX_DATE+FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_NAME
            else:
                FILE_NAME = FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_NAME   
        else:
            if FILE_TYPE == 'daily':
                FILE_NAME = File_PREFIX_DATE+FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_PREFIX_DIRECTORY+FILE_NAME    
            else:
                FILE_NAME = FILE_SUFFIX
                FILE_NAME_WITH_DIRECTORY = FILE_PREFIX_DIRECTORY+FILE_NAME
                

    def check_file_existance():
        
        s3 = resource('s3')
        
        try:
            s3.Object(SOURCE_BUCKET_NAME, FILE_NAME_WITH_DIRECTORY).load()
            print("[SUCCESS]", dt, "File Exists with name as",FILE_NAME)
            

        except botocore.exceptions.ClientError as errorStdOut:
            
            if errorStdOut.response['Error']['Code'] >= "401":
                print("[ERROR]", dt, "File does not exist. :", FILE_NAME)
                
            else:
                print("[ERROR]", dt, "Something went wrong")
                
    initialize_objects_and_varibales()
    check_file_existance() 
