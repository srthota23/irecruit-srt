from __future__ import print_function

import os
import urllib
import boto3
from cStringIO import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage


print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event,context):
    pages=None
    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)
        
    output = StringIO()
    manager = PDFResourceManager()
    converter = TextConverter(manager, output, laparams=LAParams())
    interpreter = PDFPageInterpreter(manager, converter)
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
    out_key = key.split('.')[0] + '.txt'
    out_bucket = os.environ['dest_s3_bucket']
    
    
    if key.split('.')[-1] == "pdf":
        try:
            s3.download_file(bucket, key, '/tmp/file.pdf')
            pdf_file = open('/tmp/file.pdf', 'rb')
            for page in PDFPage.get_pages(pdf_file, pagenums):
                interpreter.process_page(page)
            #pdf_file.close()
            converter.close()
            text = output.getvalue()
            print(text)
            # textFile = open('/tmp/file.txt', 'w') #make text file
            # textFile.write(text.encode('utf-8') + '\n') #write text to text file
            with open('/tmp/file.txt', 'w') as outfile:
                page_content = text
                outfile.write(page_content.encode('utf-8') + '\n')
            
            s3.upload_file('/tmp/file.txt', out_bucket, out_key)
            
            os.unlink('/tmp/file.pdf')
            os.unlink('/tmp/file.txt')
            return 'TXT file was uploaded to S3'
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}.')
            raise(e)
    return
