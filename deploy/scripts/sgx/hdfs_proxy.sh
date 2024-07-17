#!/bin/bash

# Copyright 2020 The FedLearner Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TEMP_PROXY_BASE_PATH="/data/$APPLICATION_ID"
TEMP_PROXY_DATA_PATH="$TEMP_PROXY_BASE_PATH/data"
TEMP_PROXY_LOAD_PATH="$TEMP_PROXY_BASE_PATH/load"
TEMP_PROXY_CKPT_PATH="$TEMP_PROXY_BASE_PATH/ckpt"
TEMP_PROXY_MODEL_PATH="$TEMP_PROXY_BASE_PATH/export_model"

# 将文件大小转换为自适应单位的函数
function human_readable_size() {
    local bytes=$1
    local kilobyte=1024
    local format=""
    local result=0

    if (( bytes < kilobyte )); then
        result="$bytes B"
    elif (( bytes < kilobyte**2 )); then
        result=$(awk "BEGIN {printf \"%.2f\", $bytes/$kilobyte}")" KB"
    elif (( bytes < kilobyte**3 )); then
        result=$(awk "BEGIN {printf \"%.2f\", $bytes/($kilobyte^2)}")" MB"
    else
        result=$(awk "BEGIN {printf \"%.2f\", $bytes/($kilobyte^3)}")" GB"
    fi

    echo $result
}

# 重试下载
function download_with_retries() {
    local src="$1"
    local dest="$2"
    local max_retries=3
    local attempt=1

    while (( attempt <= max_retries )); do
        hdfs dfs -copyToLocal "$src" "$dest"
        if [ $? -eq 0 ]; then
            return 0
        fi
        echo "[HDFS_PROXY] Download failed, retrying ($attempt/$max_retries)..."
        attempt=$((attempt + 1))
        sleep 2  # 等待2秒再重试
    done

    echo "[HDFS_PROXY] Failed to download $src after $max_retries attempts"
    return 1
}

# hdfs读取
function hdfs_download(){
    local hdfs_path="$1"
    local local_dir="$2"
    # local data_path_wildcard="$3"

    # 检查hdfs路径是否存在
    hdfs dfs -test -e "$hdfs_path"
    if [ $? -ne 0 ]; then
        echo "[HDFS_PROXY] $hdfs_path no exists, hdfs_download failed"
        return 1
    fi

    echo "[HDFS_PROXY] Downloading files from HDFS path: $hdfs_path to local directory: $local_dir"
   # 遍历 HDFS 目录，找到所有文件
    hdfs dfs -ls -R "$hdfs_path" | grep "^-" | while read -r line; do
        # 从 ls 输出中获取文件大小和路径
        file_size_bytes=$(echo "$line" | awk '{print $5}')
        file=$(echo "$line" | awk '{print $8}')
        #         file=$(echo "$all_files" | python3 -c "
        # import sys
        # from fnmatch import fnmatch
        # pattern = '$data_path_wildcard'
        # if fnmatch('$file_path', pattern):
        #     print('$file_path')
        #         ")
        # if [ -z "$file" ];then
        #     continue
        # fi

        # 获取相对路径
        relative_path="${file#$hdfs_path/}"

        # 创建本地目录结构
        mkdir -p "$local_dir/$(dirname "$relative_path")"

        # 记录开始时间
        start_time=$(date +%s)

        # 下载文件到本地目录
        download_with_retries "$file" "$local_dir/$relative_path"
        if [ $? -ne 0 ]; then
            echo "[HDFS_PROXY] Failed to download $file after multiple attempts"
            return 1
        fi

        # 记录结束时间
        end_time=$(date +%s)

        # 计算耗时
        elapsed_time=$(($end_time - $start_time))

        # 转换文件大小
        file_size_human=$(human_readable_size "$file_size_bytes")

        # 打印文件下载耗时和大小
        echo "[HDFS_PROXY] Downloaded $file to $local_dir/$relative_path in $elapsed_time seconds, size: $file_size_human"
    done
    echo "[HDFS_PROXY] hdfs_download succeed"

    return 0
}

# 重试上传
function upload_with_retries() {
    local src="$1"
    local dest="$2"
    local max_retries=3
    local attempt=1

    while (( attempt <= max_retries )); do
        hdfs dfs -copyFromLocal "$src" "$dest"
        if [ $? -eq 0 ]; then
            return 0
        fi
        echo "[HDFS_PROXY] Upload failed, retrying ($attempt/$max_retries)..."
        attempt=$((attempt + 1))
        sleep 2  # 等待2秒再重试
    done

    echo "[HDFS_PROXY] Failed to upload $src after $max_retries attempts"
    return 1
}

# hdfs写
function hdfs_upload(){
    local local_dir="$1"
    local hdfs_dir="$2"

    # 检查是否存在文件
    file_count=$(find "$local_dir" -type f | wc -l)
    if [ "$file_count" == 0 ]; then
        echo "[HDFS_PROXY] No file found in $local_dir"
        return 0
    fi

    echo "[HDFS_PROXY] Uploading files from loacl path: $local_dir to HDFS directory: $hdfs_path"
    # 遍历目录，找到所有文件
    find "$local_dir" -type f | while read -r file; do
        # 获取相对路径
        relative_path="${file#$local_dir/}"

        # 创建 HDFS 目录结构
        hdfs dfs -mkdir -p "$hdfs_dir/$(dirname "$relative_path")"

        # 记录开始时间
        start_time=$(date +%s)

        # 上传文件到HDFS
        upload_with_retries "$file" "$hdfs_dir/$relative_path"
        if [ $? -ne 0 ]; then
            echo "[HDFS_PROXY] Failed to upload $file after multiple attempts"
            return 1
        fi

        # 记录结束时间
        end_time=$(date +%s)

        # 计算耗时
        elapsed_time=$(($end_time - $start_time))

        file_size=$(stat -c%s "$file")

        file_size_human=$(human_readable_size "$file_size")

        # 打印文件上传耗时和大小
        echo "[HDFS_PROXY] Uploaded $file to $hdfs_dir/$relative_path in $elapsed_time seconds,size: $file_size_human"
    done
    echo "[HDFS_PROXY] Upload files from loacl path: $local_dir to HDFS directory: $hdfs_path succeed"

}

function upload_after_train(){
    if [ -n "$HDFS_PROXY" ];then
        echo "[HDFS_PROXY] Start upload"
        source /app/deploy/scripts/hdfs_common.sh || true
        export HADOOP_HOME=${HADOOP_HOME:-/opt/tiger/yarn_deploy/hadoop_current}
        export PATH=$PATH:${HADOOP_HOME}/bin
        if [ -z "$CHECKPOINT_PATH" ]; then
            CHECKPOINT_PATH="$OUTPUT_BASE_DIR/checkpoints"
        fi

        if [[ -z "$EXPORT_PATH" ]]; then
            EXPORT_PATH="$OUTPUT_BASE_DIR/exported_models"
        fi

        if [[ $CHECKPOINT_PATH == hdfs* ]];then
            hdfs_upload $TEMP_PROXY_CKPT_PATH $CHECKPOINT_PATH
            if [ $? -ne 0 ]; then
                echo "[HDFS_PROXY] Checkpoint upload failed"
                exit 1
            fi
        fi

        if [[ $EXPORT_PATH == hdfs* ]];then
            hdfs_upload $TEMP_PROXY_MODEL_PATH $EXPORT_PATH
            if [ $? -ne 0 ]; then
                echo "[HDFS_PROXY] Model upload failed"
                exit 1
            fi
        fi

        echo "[HDFS_PROXY] clean $TEMP_PROXY_BASE_PATH"
        rm -rf $TEMP_PROXY_BASE_PATH
    else
        echo "[HDFS_PROXY] No nned to upload_after_train"
    fi
}