############################# Input parameters ############################
DATA_PROC="${2:-0}"
BUCKET_DATAHUB="$3"
ENV="$4"
# ------------------------- Optional parameters ------------------------- #
KNARR_VERSION="${5:-1.0.2}"
SPARK_MASTER="${6:-"yarn"}"
SPARK_DEPLOY_MODE="${7:-"cluster"}"
SPARK_DRIVER_MEMORY="${8:-"6g"}"
SPARK_EXECUTOR_MEMORY="${9:-"8g"}"
SPARK_NUM_EXECUTORS="${10:-22}"
SPARK_MIN_EXECUTORS="${11:-22}"
SPARK_MAX_EXECUTORS="${12:-22}"
SPARK_CORE_EXECUTORS="${13-1}"
SPARK_MAX_RESULT_SIZE="${14-"2g"}"
###########################################################################

############################ Find Date ####################################

DATA_EXECUCAO=$(date +%Y%m%d)

###########################################################################

emr_id=$(cat /mnt/var/lib/info/job-flow.json | jq -r ".jobFlowId")

echo "====================================================================="
echo "ENV:" ${ENV}
echo "EMR_ID:" ${emr_id}
echo "DATASET=" ${DATASET}
echo "DATA_PROC=" ${DATA_PROC}
echo "BUCKET_DATAHUB=" ${BUCKET_DATAHUB}
echo "KNARR_VERSION=" ${KNARR_VERSION}
echo "SPARK_MASTER=" ${SPARK_MASTER}
echo "SPARK_DEPLOY_MODE=" ${SPARK_DEPLOY_MODE}
echo "SPARK_DRIVER_MEMORY=" ${SPARK_DRIVER_MEMORY}
echo "SPARK_EXECUTOR_MEMORY=" ${SPARK_EXECUTOR_MEMORY}
echo "SPARK_NUM_EXECUTORS=" ${SPARK_NUM_EXECUTORS}
echo "SPARK_MIN_EXECUTORS=" ${SPARK_MIN_EXECUTORS}
echo "SPARK_MAX_EXECUTORS=" ${SPARK_MAX_EXECUTORS}
echo "SPARK_CORE_EXECUTORS=" ${SPARK_CORE_EXECUTORS}
echo "SPARK_MAX_RESULT_SIZE=" ${SPARK_MAX_RESULT_SIZE}
echo "====================================================================="

###########################################################################
# ---------------------------- Composite values ------------------------- #

if [ ${ENV,,} == "dev" ];then
    SQS_QUEUE_ERRORS="https://sqs.sa-east-1.amazonaws.com/530914589075/datahub_alert_teams_dev"    
    BUCKET_ODIN="serasaexperian-odin-data-mesh-dev-multiprovider"
    GLUE_CATALOG_ID_ODIN="294463638235"
    USING_LAKE_FORMATION=false    
elif [ ${ENV,,} == "uat" ];then
    SQS_QUEUE_ERRORS="https://sqs.sa-east-1.amazonaws.com/146737708860/datahub_alert_teams_uat"  
    BUCKET_ODIN="serasaexperian-odin-data-mesh-uat-multiprovider"
    GLUE_CATALOG_ID_ODIN="201085490967"
    USING_LAKE_FORMATION=false        
elif [ ${ENV,,} == "prod" ];then
    SQS_QUEUE_ERRORS="https://sqs.sa-east-1.amazonaws.com/662860092544/datahub_alert_teams_prod"    
    BUCKET_ODIN="serasaexperian-odin-data-mesh-prod-multiprovider"
    GLUE_CATALOG_ID_ODIN="833589082975"
    USING_LAKE_FORMATION=false    
fi
DAG="datahub_comportamental_multiprovider_cerc_controlador_AP005_dag"

###########################################################################
RC=0

JARS_AWS=$(find / -name "*iceberg-spark-runtime*" -type f 2> /dev/null | paste -sd ",")
KNARR_JAR_NAME="experian-datahub-knarr-$KNARR_VERSION.jar"

###########################################################################

########################### Function declarations #########################
function download_knarr {
  _download_link="http://10.96.170.203:8081/nexus/content/repositories/releases/br/com/experian/experian-datahub-knarr/$KNARR_VERSION/$KNARR_JAR_NAME"

  if [ -e "$KNARR_JAR_NAME" ] ; then
    echo "Knarr jar v$KNARR_VERSION already exists. Skipping..."
  else
    echo "Downloading Knarr v$KNARR_VERSION..."
    wget "$_download_link"
  fi
}

mkdir -p /tmp/projetos/multiprovider/cerc/count/AP004/saida/{$DATA_EXECUCAO}/
mkdir -p /tmp/projetos/multiprovider/cerc/count/AP004/entrada/{$DATA_EXECUCAO}/
mkdir -p /tmp/projetos/multiprovider/cerc/count/dados/stage/agenda/{$DATA_EXECUCAO}/
mkdir -p /tmp/projetos/multiprovider/cerc/count/dados/stage/agenda_erro/{$DATA_EXECUCAO}/

############################ Find Last Path Date S3 ####################################

PATH_S3="s3://${BUCKET_DATAHUB}/projetos/multiprovider/cerc/dados/original-data/AP004/saida/"

LAST_DATE=$(aws s3 ls "$PATH_S3" | awk '{print $2}' | sed 's#/##' | grep -E '^[0-9]{8}$' | sort | awk -v today="$DATA_EXECUCAO" '$0 < today' | tail -n 1)

if [ -n "$LAST_DATE" ]; then
  echo "Última data de envio antes de hoje: $LAST_DATE"
fi

###########################################################################



function Knarr_Count_Ap004 {
local TABLE_SUBJECT="$1"
echo "============================================="
echo "  ____  __                                   "
echo " |    |/ _|  ____  _____   _______  _______  "
echo " |      <   /    \ \__  \  \_  __ \ \_  __ \ "
echo " |    |  \ |   |  \ / __ \_ |  | \/  |  | \/ "
echo " |____|__ \|___|  /(____  / |__|     |__|    "
echo "         \/     \/      \/                   "
echo "                             KNARR_VERSION: "${KNARR_VERSION}
echo "============================================="
echo "                   COUNT AP004 "${ARQUIVO_CONF} ${DATA_EXECUCAO}
echo "============================================="    
echo ${FLOW}
echo ${RESULT_COUNT} 
echo $PATH_COUNT
echo "s3://${BUCKET_DATAHUB}/projetos/multiprovider/cerc/dados/original-data/AP004/${FLOW}/${LAST_DATE}/"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 2g \
--num-executors 180 \
--executor-memory ${SPARK_EXECUTOR_MEMORY} \
--executor-cores ${SPARK_CORE_EXECUTORS} \
--conf spark.driver.maxResultSize=2g \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
--conf "spark.sql.parquet.int96RebaseModeInWrite=LEGACY" \
--files "s3://${BUCKET_DATAHUB}/projetos/multiprovider/cerc/json/cerc_count_${ARQUIVO_CONF}.json" \
--jars "$JARS_AWS" \
--conf "spark.yarn.maxAppAttempts=1" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.hadoop.fs.s3a.bucket.${BUCKET_DATAHUB}.endpoint.region=sa-east-1" \
--class "br.com.experian.Application" "${KNARR_JAR_NAME}" \
--input_path "${$PATH_COUNT}"  \
--output_path "${RESULT_COUNT}"
}

function Knarr_Count_Ap005 {
local TABLE_SUBJECT="$1"
echo "============================================="
echo "  ____  __                                   "
echo " |    |/ _|  ____  _____   _______  _______  "
echo " |      <   /    \ \__  \  \_  __ \ \_  __ \ "
echo " |    |  \ |   |  \ / __ \_ |  | \/  |  | \/ "
echo " |____|__ \|___|  /(____  / |__|     |__|    "
echo "         \/     \/      \/                   "
echo "                             KNARR_VERSION: "${KNARR_VERSION}
echo "============================================="
echo "                   COUNT AP005    "${ARQUIVO_CONF} ${DATA_EXECUCAO}
echo "============================================="
echo ${AGENDA}
echo ${RESULT_COUNT}     
echo $PATH_COUNT
echo "s3://datahub-comportamental-input-dev/projetos/multiprovider/cerc/dados/stage/${AGENDA}/${DATA_EXECUCAO}/"
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 2g \
--num-executors 180 \
--executor-memory ${SPARK_EXECUTOR_MEMORY} \
--executor-cores ${SPARK_CORE_EXECUTORS} \
--conf spark.driver.maxResultSize=2g \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
--conf "spark.sql.parquet.int96RebaseModeInWrite=LEGACY" \
--files "s3://${BUCKET_DATAHUB}/projetos/multiprovider/cerc/json/cerc_count_${ARQUIVO_CONF}.json" \
--jars "$JARS_AWS" \
--conf "spark.yarn.maxAppAttempts=1" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.hadoop.fs.s3a.bucket.${BUCKET_DATAHUB}.endpoint.region=sa-east-1" \
--class "br.com.experian.Application" "${KNARR_JAR_NAME}" \
--input_path "${$PATH_COUNT}"  \
--output_path "${RESULT_COUNT}"
}

function Knarr_Count_Validate {
local TABLE_SUBJECT="$1"
echo "============================================="
echo "  ____  __                                   "
echo " |    |/ _|  ____  _____   _______  _______  "
echo " |      <   /    \ \__  \  \_  __ \ \_  __ \ "
echo " |    |  \ |   |  \ / __ \_ |  | \/  |  | \/ "
echo " |____|__ \|___|  /(____  / |__|     |__|    "
echo "         \/     \/      \/                   "
echo "                             KNARR_VERSION: "${KNARR_VERSION}
echo "============================================="
echo "                   COUNT VALIDATE    "${ARQUIVO_CONF} ${DATA_EXECUCAO}
echo "============================================="
spark-submit \
--master yarn \
--deploy-mode cluster \
--driver-memory 2g \
--num-executors 180 \
--executor-memory ${SPARK_EXECUTOR_MEMORY} \
--executor-cores ${SPARK_CORE_EXECUTORS} \
--conf spark.driver.maxResultSize=2g \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.sql.legacy.timeParserPolicy=LEGACY" \
--conf "spark.sql.parquet.int96RebaseModeInWrite=LEGACY" \
--files "s3://${BUCKET_DATAHUB}/projetos/multiprovider/cerc/json/cerc_validate_count.json" \
--jars "$JARS_AWS" \
--conf "spark.yarn.maxAppAttempts=1" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.knarr.monitoring.json.path=${MONITORIA}" \
--conf "spark.hadoop.fs.s3a.bucket.${BUCKET_DATAHUB}.endpoint.region=sa-east-1" \
--class "br.com.experian.Application" "${KNARR_JAR_NAME}" \
}

###########################################################################

########################### Knarr download ################################
echo " Retrieving Knarr..."
download_knarr

_return_code=$?
if [[ $_return_code -ne 0 ]]; then
  echo ""
  echo "Unable to download Knarr. Exiting..."
  exit 2
fi

if [ $RC -eq 0 ]; then
    ARQUIVO_CONF="ap004_out"
    FLOW="saida"
    RESULT_COUNT="/tmp/projetos/multiprovider/cerc/count/AP004/saida/{$DATA_EXECUCAO}/"
    [ $RC -eq 0 ] && Knarr_Count_Ap004   
fi

if [ $RC -eq 0 ]; then
    ARQUIVO_CONF="ap004_ret"
    FLOW="entrada"
    RESULT_COUNT="/tmp/projetos/multiprovider/cerc/count/AP004/entrada/{$DATA_EXECUCAO}/"
    [ $RC -eq 0 ] && Knarr_Count_Ap004    
fi

if [ $RC -eq 0 ]; then
    ARQUIVO_CONF="ap005_agenda"
    AGENDA="agenda"
    RESULT_COUNT="/tmp/projetos/multiprovider/cerc/count/dados/stage/agenda/{$DATA_EXECUCAO}/"
    [ $RC -eq 0 ] && Knarr_Count_Ap005    
fi

if [ $RC -eq 0 ]; then
    ARQUIVO_CONF="ap005_agenda_erro"
    AGENDA="agenda_erro"
    RESULT_COUNT="/tmp/projetos/multiprovider/cerc/count/dados/stage/agenda/{$DATA_EXECUCAO}/"
    [ $RC -eq 0 ] && Knarr_Count_Ap005    
fi


if [ $RC -eq 0 ]; then
    [ $RC -eq 0 ] && Knarr_Count_Validate 
    RC=$(($RC + $?))
    if [ $RC -gt 0 ];then
        aws sqs send-message --queue-url "${SQS_QUEUE_ERRORS}" --message-body '{"timestamp": "'$(date +%Y-%m-%dT%H:%M:%S)'", "co_erro": "997", "no_arquivo": "'${TABLE_SUBJECT}'", "no_canal": "multiprovider", "co_dag" : "'${DAG}'", "no_fluxo": "cerc_rechaco_AP005", "link_s3": "'${MONITORIA}'"}' 
        echo "=========== SQS Erro enviado "
        RC=$(($RC + $?))
        exit 1    
fi

