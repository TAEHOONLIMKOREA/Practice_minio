from minio import Minio
import urllib3

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    minioClient = Minio('keties.iptime.org:59000',  # ip:port  or URL
                        access_key='keti_root',
                        secret_key='madcoder',
                        secure=False)

    found = minioClient.bucket_exists("my-bucket")
    if not found:
        minioClient.make_bucket("my-bucket")
    else:
        print("Bucket 'my_bucket' already exists")

    # Upload '/home/user/Photos/asiaphotos.zip' as object name
    # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    minioClient.fput_object("my-bucket", "test.zip", "./asset/test.zip", )
    print("'./asset/test.zip' is successfully uploaded as "
          "object 'test.zip' to bucket 'my_bucket'.")


