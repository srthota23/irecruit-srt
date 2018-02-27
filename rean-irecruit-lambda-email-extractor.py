from __future__ import print_function

import boto3
import urllib
import string
import email
import os
import shutil
import gzip
import zipfile

print('Loading function')

s3 = boto3.client('s3')
s3r = boto3.resource('s3')
outdir = "/tmp/output/"

outputBucket = "rean-irecruit-01-raw"  # Set here for a seperate bucket otherwise it is set to the events bucket
outputPrefix = "/"  # Should end with /


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')

    try:
        # Set outputBucket if required
        if not outputBucket:
            global outputBucket
            outputBucket = bucket

        # Use waiter to ensure the file is persisted
        waiter = s3.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key=key)

        response = s3r.Bucket(bucket).Object(key)

        # Read the raw text file into a Email Object
        msg = email.message_from_string(response.get()["Body"].read())

        if len(msg.get_payload()) > 1:

            # Create directory for XML files (makes debugging easier)
            if os.path.isdir(outdir) == False:
                os.mkdir(outdir)
                print(len(msg.get_payload()))
                for c in range (1,len(msg.get_payload())):
                    print(c)
                    # The first attachment
                    attachment = msg.get_payload()[c]
                    # Extract the attachment and upload to S3
                    extract_attachment(attachment)
                    # Upload the XML files to S3
                upload_resulting_files_to_s3()
                shutil.rmtree('/tmp/output')
        else:
            print("Could not see file/attachment.")
        #return "Boom"
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist '
            'and your bucket is in the same region as this '
            'function.'.format(key, bucket))
        raise e

def extract_attachment(attachment):
    # Process .gzip attachments
    if "gzip" in attachment.get_content_type():
        contentdisp = string.split(attachment.get('Content-Disposition'), '=')
        fname = contentdisp[1].replace('\"', '')
        # new = fname.split(';')
        # fname = new[0]
        open('/tmp/' + fname, 'wb').write(attachment.get_payload(decode=True))
        outname = fname[:-3]
        open(outdir + outname, 'wb').write(gzip.open('/tmp/' + fname, 'rb').read())

    # Process .zip attachments
    elif "zip" in attachment.get_content_type():
        open('/tmp/attachment.zip', 'wb').write(attachment.get_payload(decode=True))
        with zipfile.ZipFile('/tmp/attachment.zip', "r") as z:
            z.extractall(outdir)

    # Process other files - txt, rtf, doc, docx, pdf
    else:
        contentdisp = string.split(attachment.get('Content-Disposition'), '=')
        outname = contentdisp[1].replace('\"', '')
        #print(outname)
        new = outname.split(';')
        outname = new[0]
        open(outdir + outname, 'wb').write(attachment.get_payload(decode=True))
        print(contentdisp)
        return(contentdisp)


def upload_resulting_files_to_s3():
    # Put all files back into S3 (Covers non-compliant cases if a ZIP contains multiple files)
    for fileName in os.listdir(outdir):
        print("Uploading: " + fileName)  # File name to upload
        s3r.meta.client.upload_file(outdir+'/'+fileName, outputBucket, fileName)