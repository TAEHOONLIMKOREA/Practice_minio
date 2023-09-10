from minio import Minio
import os

minioClient = Minio('keties.iptime.org:59000',  # ip:port  or URL
                    access_key='keti_root',
                    secret_key='madcoder',
                    secure=False)

# 로컬 디렉토리 경로
local_directory = "/path/to/local/directory"

# MinIO 버킷 이름
minio_bucket = "your-minio-bucket"

def upload_directory_to_minio(local_path, minio_bucket):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, local_path)
            try:
                # 파일을 MinIO 버킷에 업로드
                minioClient.fput_object(minio_bucket, object_name, file_path)
                print(f"Uploaded: {object_name}")
            except Exception as e:
                print(f"Error uploading {object_name}: {e}")

if __name__ == "__main__":
    upload_directory_to_minio(local_directory, minio_bucket)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':


    found = minioClient.bucket_exists("my-bucket")
    if not found:
        minioClient.make_bucket("my-bucket")
    else:
        print("Bucket 'my_bucket' already exists")

    # Upload '/home/user/Photos/asiaphotos.zip' as object name
    # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    # minioClient.fput_object("my-bucket", "test.zip", "./asset/test.zip", )
    # print("'./asset/test.zip' is successfully uploaded as "
    #       "object 'test.zip' to bucket 'my_bucket'.")

    minioClient.fget_object("my-bucket", "test.zip", "./asset/my-filename.zip")


