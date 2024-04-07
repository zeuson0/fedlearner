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

EXEC_DIR=/app/exec_dir
SGX_CONFIG_PATH="$GRPC_PATH/examples/dynamic_config.json"
TEMPLATE_PATH="/gramine/CI-Examples/generate-token/python.manifest.template"

# 更新sgx的认证策略
update_sgx_dynamic_config() {
  local mr_enclave="$1"
  local mr_signer="$2"
  local isv_prod_id="$3"
  local isv_svn="$4"
  local json_data=$(cat "$SGX_CONFIG_PATH")
  
  # 创建json体
  local new_mrs=$(jq -n --arg mr_enclave "$mr_enclave" \
                         --arg mr_signer "$mr_signer" \
                         --arg isv_prod_id "$isv_prod_id" \
                         --arg isv_svn "$isv_svn" \
                         '{
                             mr_enclave: $mr_enclave,
                             mr_signer: $mr_signer,
                             isv_prod_id: $isv_prod_id,
                             isv_svn: $isv_svn
                         }')
  
  # 检查sgx_mrs数组中是否存在相同的条目
  local exists=$(echo "$json_data" | jq --argjson check_mrs "$new_mrs" \
                    '.sgx_mrs[] | select(.mr_enclave == $check_mrs.mr_enclave and .mr_signer == $check_mrs.mr_signer and .isv_prod_id == $check_mrs.isv_prod_id and .isv_svn == $check_mrs.isv_svn) | . != null')

  # 不重复添加
  if [[ -z "$exists" ]]; then
    json_data=$(echo "$json_data" | jq --argjson new_mrs "$new_mrs" '.sgx_mrs += [$new_mrs]')
    echo "$json_data" > "$SGX_CONFIG_PATH"
  fi
}

# 从sig中获取度量值hex
function get_env() {
    gramine-sgx-get-token -s python.sig -o /dev/null | grep $1 | awk -F ":" '{print $2}' | xargs
}

# 设置自定义环境
function make_custom_env() {
    cd $EXEC_DIR

    export DEBUG=0
    export CUDA_VISIBLE_DEVICES=""
    export DNNL_VERBOSE=0
    export GRPC_VERBOSITY=ERROR
    export GRPC_POLL_STRATEGY=epoll1
    export TF_CPP_MIN_LOG_LEVEL=1
    export TF_GRPC_SGX_RA_TLS_ENABLE=on
    export FL_GRPC_SGX_RA_TLS_ENABLE=on
    export TF_DISABLE_MKL=0
    export TF_ENABLE_MKL_NATIVE_FORMAT=1
    export parallel_num_threads=$1
    export INTRA_OP_PARALLELISM_THREADS=$parallel_num_threads
    export INTER_OP_PARALLELISM_THREADS=$parallel_num_threads
    export GRPC_SERVER_CHANNEL_THREADS=4
    export KMP_SETTINGS=1
    export KMP_BLOCKTIME=0
    export HADOOP_HOME=${HADOOP_HOME:-/opt/tiger/yarn_deploy/hadoop_current}
    export PATH=$PATH:${HADOOP_HOME}/bin
    export JAVA_HOME=/opt/tiger/jdk/openjdk-1.8.0_265
    export LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native:${JAVA_HOME}/jre/lib/amd64/server:${LD_LIBRARY_PATH}
    export CLASSPATH=.:$CLASSPATH:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar:$($HADOOP_HOME/bin/hadoop classpath --glob)
    export RA_TLS_ALLOW_OUTDATED_TCB_INSECURE=1

    if [ -z "$PEER_MR_SIGNER" ]; then
        export PEER_MR_SIGNER=`get_env mr_signer`
    fi

    if [ -z "$PEER_MR_ENCLAVE" ]; then
        export PEER_MR_ENCLAVE=`get_env mr_enclave`
    fi

    update_sgx_dynamic_config $PEER_MR_ENCLAVE $PEER_MR_SIGNER 0 0
    cp $SGX_CONFIG_PATH $EXEC_DIR
    cd -
}

# 生成enclave和token
function generate_token() {
    cd /gramine/CI-Examples/generate-token/
    ./generate.sh
    update_sgx_dynamic_config `get_env mr_enclave` `get_env mr_signer` 0 0
    mkdir -p $EXEC_DIR
    cp /app/sgx/gramine/CI-Examples/tensorflow_io.py $EXEC_DIR
    cp python.sig $EXEC_DIR
    cp python.manifest.sgx $EXEC_DIR
    cp python.token $EXEC_DIR
    cp python.manifest $EXEC_DIR
    cd -
}

# 根据enclave_size调整enclave
function build_enclave(){
    local enclave_size="$1"
    local need_clean="$2"
    sed -i "/sgx.enclave_size/ s/\"[^\"]*\"/\"$enclave_size\"/" "$TEMPLATE_PATH"
    if [ $? -eq 0 ]; then
        echo "Enclave size changed to $enclave_size in $TEMPLATE_PATH"
    else
        echo "Failed to change enclave size in $TEMPLATE_PATH"
    fi
    generate_token
    if [ -n "$need_clean" ]; then
        rm -rf $EXEC_DIR
    fi
}

function build_enclave_all(){
    local enclave_size="8G"
    if [ -n "$1" ] && [ $1 == "ps" ]; then
        # build worker/master
        if [ -n "$GRAMINE_ENCLAVE_SIZE" ]; then
            enclave_size=$GRAMINE_ENCLAVE_SIZE
        fi
        build_enclave $enclave_size 1

        # build ps
        if [ -n "$COSTOM_PS_SIZE" ]; then
            enclave_size=$COSTOM_PS_SIZE
        else
            enclave_size="16G"
        fi
        build_enclave $enclave_size
    else
        # build ps
        if [ -n "$COSTOM_PS_SIZE" ]; then
            enclave_size=$COSTOM_PS_SIZE
        else
            enclave_size="16G"
        fi
        build_enclave $enclave_size 1

        # build worker/master
        if [ -n "$GRAMINE_ENCLAVE_SIZE" ]; then
            enclave_size=$GRAMINE_ENCLAVE_SIZE
        else
            enclave_size="8G"
        fi
        build_enclave $enclave_size
    fi
}

if [ -n "$PCCS_IP" ]; then
        sed -i "s|PCCS_URL=https://[^ ]*|PCCS_URL=https://pccs_url:8081/sgx/certification/v3/|" /etc/sgx_default_qcnl.conf
        echo >> /etc/hosts
        echo "$PCCS_IP   pccs_url" | tee -a /etc/hosts
elif [ -n "$PCCS_URL" ]; then
        sed -i "s|PCCS_URL=[^ ]*|PCCS_URL=$PCCS_URL|" /etc/sgx_default_qcnl.conf
fi
sed -i 's/USE_SECURE_CERT=TRUE/USE_SECURE_CERT=FALSE/' /etc/sgx_default_qcnl.conf

if [ -n "$GRAMINE_LOG_LEVEL" ]; then
        sed -i "/loader.log_level/ s/\"[^\"]*\"/\"$GRAMINE_LOG_LEVEL\"/" "$TEMPLATE_PATH"
        if [ $? -eq 0 ]; then
            echo "Log level changed to $GRAMINE_LOG_LEVEL in $TEMPLATE_PATH"
        else
            echo "Failed to change log level in $TEMPLATE_PATH"
        fi
fi

if [ -n "$GRAMINE_THREAD_NUM" ]; then
    sed -i "s/sgx.thread_num = [0-9]\+/sgx.thread_num = $GRAMINE_THREAD_NUM/" "$TEMPLATE_PATH"
    if [ $? -eq 0 ]; then
        echo "Thread number changed to $GRAMINE_THREAD_NUM in $TEMPLATE_PATH"
    else
        echo "Failed to change thread number in $TEMPLATE_PATH"
    fi
fi

if [ -n "$GRAMINE_STACK_SIZE" ]; then
    sed -i "/sys.stack.size/ s/\"[^\"]*\"/\"$GRAMINE_STACK_SIZE\"/" "$TEMPLATE_PATH"
    if [ $? -eq 0 ]; then
        echo "Stack size changed to $GRAMINE_STACK_SIZE in $TEMPLATE_PATH"
    else
        echo "Failed to change stack size in $TEMPLATE_PATH"
    fi
fi

mkdir -p /data
build_enclave_all $1