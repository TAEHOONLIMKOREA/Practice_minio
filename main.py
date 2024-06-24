from minio import Minio
from minio.commonconfig import CopySource
from tqdm import tqdm
import concurrent.futures
import time
import os
import re

minio_client = Minio('bigsoft.iptime.org:55420',  # ip:port  or URL
                    access_key='keti_root',
                    secret_key='madcoder',
                    secure=False)

# --------------------------------basic code test--------------------------------------------
def upload_file_to_minio(client, bucket, file_path):
    file_name = os.path.relpath(file_path, local_directory)
    object_name = "/" + file_name.replace("\\", "/")
    try:
        # 파일을 MinIO 버킷에 멀티파트 업로드 (조각 크기 : 100MB)
        client.fput_object(bucket, object_name, file_path, part_size=100 * 1024 * 1024)
        print(f"Uploaded: {object_name}")
    except Exception as err:
        print(f"Error uploading {object_name}: {err}")

def upload_directory_to_minio(local_path, bucket):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for root, dirs, files in os.walk(local_path):
            for file in files:
                file_path = os.path.join(root, file)
                executor.submit(upload_file_to_minio, file_path, bucket)
                

# --------------------------------change object names--------------------------------------------
def get_object_list_with_prefix(client, bucket_name, object_prefix):
    object_prefix = object_prefix + "/"
    # [1] 원본 폴더 안의 객체 리스트 가져오기
    objects = client.list_objects(
        bucket_name,
        prefix=object_prefix,
        recursive=True
    )
    result_list = []
    for obj in objects:
        # [2-1] 객체의 이름 설정
        object_name = obj.object_name
        object_name = object_name.replace(object_prefix,"",1)
        # object_name = object_name.lstrip(object_prefix)
        result_list.append(object_name)
    
    return result_list


# 객체 이동 (개발자용)
def move_object(client, bucket_name, old_object_name, new_object_name):                
    try:
        # 객체 복사
        client.copy_object(bucket_name, new_object_name, CopySource(bucket_name, old_object_name))
        # 이전 객체 삭제
        client.remove_object(bucket_name, old_object_name)
        print("Object moved successfully")
    except Exception as e:
        print(f"Error: {e}")
        
        
def move_objects(client, bucket_name, old_object_prefix, new_object_prefix):
    # [1] 원본 폴더 안의 객체 리스트 가져오기
    objects = client.list_objects(
        bucket_name,
        prefix=old_object_prefix,
        recursive=True
    )
    # [2] 객체들을 대상 폴더로 이동
    for obj in objects:
        # [2-1] 객체의 이름 설정
        source_object_name = obj.object_name
        destination_object_name = source_object_name.replace(
            old_object_prefix,
            new_object_prefix,
            1  # 한 번만 치환
        )
        # [2-2] 객체 이동
        client.copy_object(
            bucket_name,
            destination_object_name,
            CopySource(bucket_name, source_object_name)
        )
        # [2-3] 원본 객체 삭제
        client.remove_object(bucket_name, source_object_name)

    
# 버킷 삭제 (개발자용)
def delete_bucket(client, bucket_name):
    if client.bucket_exists(bucket_name):
        # 버킷 내의 모든 객체 및 폴더 목록 가져오기
        objects = client.list_objects(bucket_name, recursive=True, include_user_meta=True, include_version=True)
        # 객체와 폴더 삭제
        for obj in objects:
            client.remove_object(bucket_name, obj.object_name)        
        client.remove_bucket(bucket_name=bucket_name)        
        print(f"Bucket '{bucket_name}' deleted successfully")
    else:
        raise BucketNameError(bucket_name)
    

def change_Vision_data_name(client, bucket_name):
    # bp_id 가 242 이상이면 제끼면 됨
    for bp_id in tqdm(range(242), desc="Outer Loop"):
        prefix = "Build/Build Process/2/"+ str(bp_id) +"/InSitu/Vision"
        objects = get_object_list_with_prefix(minio_client, "hbnu", prefix)
        
        for obj in tqdm(objects, desc="Inner Loop", leave=False):
            splits = obj.split('/')
            file_name = splits[-1]  # 마지막 파트
            image_type = splits[-2] # 마지막에서 두 번째 파트
            # 정규 표현식을 사용하여 정수 추출
            match = re.search(r'\d+', file_name)
            if match:
                layer_num =  int(match.group())
            else:
                continue
            source_object_name = prefix + "/" + obj
            destination_object_name = prefix + "/" + image_type + "/" + str(layer_num) + ".jpg"
            if source_object_name == destination_object_name:
                continue
            # [2-2] 객체 이동
            client.copy_object(
                bucket_name,
                destination_object_name,
                CopySource(bucket_name, source_object_name)
            )
            # [2-3] 원본 객체 삭제
            client.remove_object(bucket_name, source_object_name)

if __name__ == "__main__":
    # found = client.bucket_exists(bucket)
    # if not found:
    #     client.make_bucket(bucket)
    # else:
    #     print("Bucket " + bucket + " already exists")
    # start_time = time.time()
    # upload_directory_to_minio(local_directory, bucket)
    # print("---{}s seconds---".format(time.time()-start_time))
    
    change_Vision_data_name(minio_client, "hbnu")
    
    
        
        
    
    

