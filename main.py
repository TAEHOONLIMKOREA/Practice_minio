from minio import Minio
import concurrent.futures
import time
import os

minio_client = Minio('keties.iptime.org:59000',  # ip:port  or URL
                    access_key='keti_root',
                    secret_key='madcoder',
                    secure=False)

# 로컬 디렉토리 경로
local_directory = "./(2023-04-28) 3DP DX 발표자료"

# MinIO 버킷 이름
minio_bucket = "your-minio-bucket"

def upload_file_to_minio(file_path, minio_bucket):
    file_name = os.path.relpath(file_path, local_directory)
    object_name = "/" + file_name.replace("\\", "/")
    try:
        # 파일을 MinIO 버킷에 멀티파트 업로드 (조각 크기 : 100MB)
        minio_client.fput_object(minio_bucket, object_name, file_path, part_size=100 * 1024 * 1024)
        print(f"Uploaded: {object_name}")
    except Exception as err:
        print(f"Error uploading {object_name}: {err}")

def upload_directory_to_minio(local_path, minio_bucket):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                file_path = os.path.join(root, file)
                executor.submit(upload_file_to_minio, file_path, minio_bucket)


# def upload_directory_to_minio(local_path, minio_bucket):
#     for root, dirs, files in os.walk(local_path):
#         for file in files:
#             file_path = os.path.join(root, file)
#             file_name = os.path.relpath(file_path, local_path)
#             object_name = "/" + file_name.replace("\\", "/")
#             try:
#                 # 파일을 MinIO 버킷에 업로드
#                 minio_client.fput_object(minio_bucket, object_name, file_path)
#                 print(f"Uploaded: {object_name}")
#             except Exception as e:
#                 print(f"Error uploading {object_name}: {e}")

if __name__ == "__main__":
    found = minio_client.bucket_exists(minio_bucket)
    if not found:
        minio_client.make_bucket(minio_bucket)
    else:
        print("Bucket " + minio_bucket + " already exists")
    start_time = time.time()
    upload_directory_to_minio(local_directory, minio_bucket)
    print("---{}s seconds---".format(time.time()-start_time))

