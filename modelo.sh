echo "Instalando dependencias"

pip3 install --user pysftp
ENV="${5}"
BUCKET_NAME=$3
sqsqueuename="${11}"
entradacerc=$4
DT_CARGA=$(date '+%Y%m%d')
bucket_key=$entradacerc".msg"
ES_VERSION="http://10.96.170.203:8081/nexus/service/local/repositories/central/content/org/elasticsearch/elasticsearch-spark-30_2.12/7.17.14/elasticsearch-spark-30_2.12-7.17.14.jar"
JARS_AWS=$(find / -name "*iceberg-spark-runtime*" -o -name "*emrfs-hadoop-assembly*" -type f 2> /dev/null | paste -sd ",")

# SFTP Variables
SFTP_HOST=$6
SFTP_PORT=$7
SFTP_USERNAME=$8
SFTP_PASSWORD_SECRET_ID=$9
SFTP_PASSWORD=$(aws secretsmanager get-secret-value --secret-id "$SFTP_PASSWORD_SECRET_ID" | jq ".SecretString" -r)
SFTP_REMOTE_FILE_BASE_PATH=${10}

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2

if [ $ENV == "dev_operador" ];then
    BUCKET_ODIN="serasaexperian-odin-data-mesh-dev-cerc-datamarts"
    BUCKET_CERC_LOCAL="odin-comportamental-output-dev"
    VERSION="0.0.11"
    elasticusuario="datahub_dev"
    elasticAdress="https://vpc-es-datahub-dikx4vnxw7e3lg72kuuj634r3i.sa-east-1.es.amazonaws.com/"
    s3DestinoFinal="s3://eec-aws-br-cs-opr-dev-datahub-operator-files-bucket/"
    packageElastic="org.elasticsearch:elasticsearch-spark-20_2.12:8.10.4"
    qtd_arquivos=10
elif [ $ENV == "uat_operador" ];then
    BUCKET_ODIN="serasaexperian-odin-data-mesh-uat-cerc-datamarts"
    qtd_arquivos=10
else
    BUCKET_ODIN="serasaexperian-odin-data-mesh-prod-cerc-datamarts"
    BUCKET_CERC_LOCAL="odin-comportamental-output-prod"
    VERSION="0.0.11"
    elasticusuario="datahub_prod"
    elasticAdress="https://vpc-datahub-prod-w3aestkv2hu5wwp7zmtaokpada.sa-east-1.es.amazonaws.com"
    s3DestinoFinal="s3://eec-aws-br-cs-opr-prd-datahub-operator-files-bucket/"
    packageElastic="org.opensearch.client:opensearch-spark-30_2.12:1.0.1"
    qtd_arquivos=50
fi

OPERADOR_CONFIGS_BASE_PATH="s3://$BUCKET_NAME/projetos/cerc/configs/operador"

function download_knar {
  VERSION="0.0.11"
  KNAR="experian-datahub-knarr-${VERSION}.jar"
  LINK_KNAR="http://10.96.170.203:8081/nexus/content/repositories/releases/br/com/experian/experian-datahub-knarr/${VERSION}/$KNAR"

  if [ -e "$KNAR" ] ; then
    echo "O jar do Knar ja existe"
  else
    echo "O jar do Knar não ja existe, fazendo download"
    wget $LINK_KNAR
  fi
}

function register_counter {
    echo
    echo "=== REGISTROS PROCESSADOS NA ${1} - ${2} ===" && echo 'spark.read.parquet("'${3}'").count' | spark-shell --master local[*] && echo "=== REGISTROS PROCESSADOS NA ${1} - ${2} ==="
    echo
}

function send_s3 {
    JSON="{\"no_arquivo\":\"$2\",\"tp_erro\":\"arquivo\",\"de_erro\":\"$3\" }"
    echo "Enviando SQS ${JSON}"
    aws sqs send-message --queue-url $1 --message-body "$JSON" --delay-seconds 10
}

function validation_file {
  echo "Criando diretorio da cerc" && mkdir cerc && cd cerc && echo "Fazendo download do AST $5 - $6"

python3 <<CODE
import pysftp

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with pysftp.Connection(
    host='$1', port=$2,
    username='$3', password='$4',
    cnopts=cnopts) as sftp:
    sftp.get('$5', '$6', preserve_mtime=True)
CODE

  echo "send $6 para s3://${BUCKET_NAME}/cerc/operador/backup/tmp/${6}"
  aws s3 cp "$6" s3://${BUCKET_NAME}/cerc/operador/backup/tmp/${6}

  echo "Fazendo unzip do arquivo $6"
  unzip $6
  echo "Removendo o zip $6"
  rm -rf $6

  qtdSubpastas=$(ls -D  | wc -l)

  if [ $qtdSubpastas -eq 4 ]; then
      echo "Existe 4 subpastas" && echo $(ls -D)
  else 
      echo "Error - 98"
      send_s3 $7 $6 "O arquivo zip contem uma quantidade diferente do esperado pelo fluxo"
      exit 1
  fi

  qtd_erros=$(find erros.parquet -maxdepth 1 -type f | wc -l)
  qtd_ur=$(find ur.parquet -maxdepth 1 -type f | wc -l)
  qtd_contrato=$(find contrato.parquet -maxdepth 1 -type f | wc -l)
  qtd_relacao_ur_contrato=$(find relacao_ur_contrato.parquet -maxdepth 1 -type f | wc -l)

  if [ $qtd_erros -eq 0 ] || [ $qtd_ur -eq 0 ] || [ $qtd_contrato -eq 0 ] || [ $qtd_relacao_ur_contrato -eq 0 ]; then
      send_s3 $7 $6 "O arquivo zip contem uma quantidade diferente do esperado pelo fluxo" 
      echo "Error - 110"
      exit 1
  else 
      echo "Todas as subpastas contem arquivos"
  fi

  echo "Movendo os arquivos para o hdfs" && echo "" && echo "Movendo URs"
  time find ur.parquet -name "part-*.parquet" | xargs -n$qtd_arquivos -P $(nproc) bash -c 'hdfs dfs -mkdir -p /cerc/${1%/*};hdfs dfs -put -f $@  /cerc/${1%/*}' bash
  echo "" && echo "Movendo ERROs"
  time find erros.parquet -name "part-*.parquet" | xargs -n$qtd_arquivos -P $(nproc) bash -c 'hdfs dfs -mkdir -p /cerc/${1%/*};hdfs dfs -put -f $@  /cerc/${1%/*}' bash
  echo "" && echo "Movendo CONTRATOs"
  time find contrato.parquet -name "part-*.parquet" | xargs -n$qtd_arquivos -P $(nproc) bash -c 'hdfs dfs -mkdir -p /cerc/${1%/*};hdfs dfs -put -f $@  /cerc/${1%/*}' bash
  echo "" && echo "Movendo RELACOEs"
  time find relacao_ur_contrato.parquet -name "part-*.parquet" | xargs -n$qtd_arquivos -P $(nproc) bash -c 'hdfs dfs -mkdir -p /cerc/${1%/*};hdfs dfs -put -f $@  /cerc/${1%/*}' bash
  echo "Arquivos enviado para o hdfs"

  ret_code=$?

  if [ $ret_code -ne 0 ]; then
    exists=$(aws s3 ls s3://${BUCKET_NAME}/sqsMessage/cerc/operador/$bucket_key | wc -l)
    if [ $exists -eq 0 ]; then
      message='{"no_arquivo":"'"$entradacerc"'","tp_erro":"'"Processamento"'","de_erro":"'"Erro Processamento - Validacao"'"}'
      echo $message > $bucket_key
      aws s3 cp $bucket_key s3://${BUCKET_NAME}/sqsMessage/cerc/operador/$bucket_key
      msgTxt='{"no_arquivo":"'"$entradacerc"'","tp_erro":"'"Processamento"'","de_erro":"'"s3://${BUCKET_NAME}/sqsMessage/cerc/controlador/$bucket_key"'"}'
      aws sqs send-message --queue-url $sqsqueuename --message-body "$msgTxt" --no-verify-ssl
      exit 0
    fi    
  fi

  cd ..
}

function bronze { 
  dataset=$1
  bucket_name=$2
  sqsqueuename=$3
  bucket_key=$4
  entradacerc=$5
  structure=$6
  config_bronze=$7
  jars_aws=$8

  structure_config_file_base_path="$OPERADOR_CONFIGS_BASE_PATH/stage"
  bronze_config_file_base_path="$OPERADOR_CONFIGS_BASE_PATH/bronze"

  aws s3 cp "$structure_config_file_base_path"/"$structure".json schema_validation.json

  echo "" && echo "********************************** Bronze do ${dataset} **********************************" && echo ""

  spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 6g \
  --num-executors 22 \
  --executor-memory 8g \
  --conf spark.driver.maxResultSize=2g \
  --files "$bronze_config_file_base_path/$config_bronze.json,schema_validation.json" \
  --jars "$jars_aws" \
  --conf "spark.yarn.maxAppAttempts=1" \
  --conf "spark.executor.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*" \
  --conf "spark.driver.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*" \
  --conf "spark.knarr.quality.aws.sqs.queue=$sqsqueuename" \
  --conf "spark.knarr.quality.aws.region=sa-east-1" \
  --conf "spark.knarr.quality.aws.s3.bucket.name=${bucket_name}" \
  --conf "spark.knarr.quality.aws.s3.bucket.key=/sqsMessage/cerc/operador/${bucket_key}" \
  --class "br.com.experian.Application" \
  "./experian-datahub-knarr-${VERSION}.jar" \
  --input_path "/cerc/$dataset.parquet/" \
  --bronze_conf_path "${config_bronze}.json" \
  --output_path "/cerc/bronze/${dataset}" \
  --dataset_name "cerc_${dataset}" \
  --layer_name "bronze"

  ret_code=$?

  if [ $ret_code -ne 0 ]; then
    exists=$(aws s3 ls s3://${bucket_name}/sqsMessage/cerc/operador/$bucket_key | wc -l)
    if [ $exists -eq 0 ]; then
      message='{"no_arquivo":"'"$entradacerc"'","tp_erro":"'"Processamento"'","de_erro":"'"Erro Processamento - Bronze do ${dataset}"'"}'
      echo $message > $bucket_key
      aws s3 cp $bucket_key s3://${bucket_name}/sqsMessage/cerc/operador/$bucket_key
      msgTxt='{"no_arquivo":"'"$entradacerc"'","tp_erro":"'"Processamento"'","de_erro":"'"s3://${bucket_name}/sqsMessage/cerc/controlador/$bucket_key"'"}'
      aws sqs send-message --queue-url $sqsqueuename --message-body "$msgTxt" --no-verify-ssl
    fi
    exit 1
  fi

}

function silver { 
  dataset=$1
  bucket_name=$2
  sqsqueuename=$3
  bucket_key=$4
  dt_carga=$5
  qg=$6
  config_silver=$7
  jars_aws=$8
  s3DestinoFinal=$9

  quality_config_file_base_path="$OPERADOR_CONFIGS_BASE_PATH/quality"
  silver_config_file_base_path="$OPERADOR_CONFIGS_BASE_PATH/silver"

  aws s3 cp "$quality_config_file_base_path"/"$qg".json conf_quality.json
  aws s3 cp "$silver_config_file_base_path"/"$config_silver".json "$config_silver".json

  echo "" && echo "********************************** Silver do ${dataset} **********************************" && echo ""

  if [ ${dataset} == "erros" ];then
    spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 6g \
    --num-executors 22 \
    --executor-memory 8g \
    --conf spark.driver.maxResultSize=2g \
    --files "${config_silver}.json" \
    --jars "$jars_aws" \
    --conf "spark.yarn.maxAppAttempts=1" \
    --conf "spark.knarr.quality.aws.sqs.queue=${sqsqueuename}" \
    --conf "spark.knarr.quality.aws.s3.bucket.name=${bucket_name}" \
    --conf "spark.knarr.quality.aws.s3.bucket.key=/sqsMessage/cerc/operador/${bucket_key}" \
    --class "br.com.experian.Application" \
    "./experian-datahub-knarr-${VERSION}.jar" \
    --input_path "/cerc/bronze/${dataset}/dt_processamento=${dt_carga}/" \
    --silver_conf_path "${config_silver}.json" \
    --dataset_name "cerc_${dataset}_silver" \
    --layer_name "silver" \
    --output_path "$s3DestinoFinal/${dataset}/" \
    --output_rechaco_path "$s3DestinoFinal/rechaco/${dataset}/"
  else
    spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 6g \
    --num-executors 22 \
    --executor-memory 8g \
    --conf spark.driver.maxResultSize=2g \
    --files "${config_silver}.json,conf_quality.json" \
    --jars "$jars_aws" \
    --conf "spark.yarn.maxAppAttempts=1" \
    --conf "spark.knarr.quality.aws.sqs.queue=${sqsqueuename}" \
    --conf "spark.knarr.quality.aws.s3.bucket.name=${bucket_name}" \
    --conf "spark.knarr.quality.aws.s3.bucket.key=/sqsMessage/cerc/operador/${bucket_key}" \
    --class "br.com.experian.Application" \
    "./experian-datahub-knarr-${VERSION}.jar" \
    --input_path "/cerc/bronze/${dataset}/dt_processamento=${dt_carga}/" \
    --silver_conf_path "${config_silver}.json" \
    --dataset_name "cerc_${dataset}_silver" \
    --layer_name "silver" \
    --output_path "$s3DestinoFinal/${dataset}/" \
    --quality_regex_file "conf_quality.json" \
    --output_rechaco_path "$s3DestinoFinal/rechaco/${dataset}/"
    fi
}

download_knar

SFTP_REMOTE_SOURCE_FILE_PATH="$SFTP_REMOTE_FILE_BASE_PATH$entradacerc.zip"
SFTP_LOCAL_TARGET_FILE_PATH="$entradacerc.zip"
validation_file "$SFTP_HOST" "$SFTP_PORT" "$SFTP_USERNAME" "$SFTP_PASSWORD" "$SFTP_REMOTE_SOURCE_FILE_PATH" "$SFTP_LOCAL_TARGET_FILE_PATH" "$sqsqueuename"

register_counter "Entrada" "UR" "/cerc/ur.parquet/"
register_counter "Entrada" "Contrato" "/cerc/contrato.parquet/"
register_counter "Entrada" "Relacao Ur e Contrato" "/cerc/relacao_ur_contrato.parquet/"
register_counter "Entrada" "Erros" "/cerc/erros.parquet/"

bronze "contrato" $BUCKET_NAME $sqsqueuename $bucket_key $entradacerc "conf_contrato_CNPJ_structure" "conf_contrato_CNPJ_bronze" "$JARS_AWS"
bronze "ur" $BUCKET_NAME $sqsqueuename $bucket_key $entradacerc "conf_ur_CNPJ_structure" "conf_ur_CNPJ_bronze" "$JARS_AWS"
bronze "relacao_ur_contrato" $BUCKET_NAME $sqsqueuename $bucket_key $entradacerc "conf_relacao_ur_contrato_CNPJ_structure" "conf_relacao_ur_contrato_CNPJ_bronze" "$JARS_AWS"
bronze "erros" $BUCKET_NAME $sqsqueuename $bucket_key $entradacerc "conf_erros_CNPJ_structure" "conf_erros_CNPJ_bronze" "$JARS_AWS"

grupoData=$(echo $entradacerc| cut -d'.' -f 9)
dtRef=$(echo "${grupoData:0:4}-${grupoData:4:2}-${grupoData:6:2}")
usuario=$(echo $entradacerc| cut -d'.' -f 8)
filenameDS="OPR.$(echo $entradacerc| cut -d'.' -f 8).$(echo $entradacerc| cut -d'.' -f 9).$(echo $entradacerc| cut -d'.' -f 10).$(echo $entradacerc| cut -d'.' -f 11)"
s3DestinoFinal=$(echo "$s3DestinoFinal$usuario/dt_ref=$dtRef/$filenameDS/output/")

silver "contrato" $BUCKET_NAME $sqsqueuename $bucket_key $DT_CARGA "conf_contrato_quality" "conf_contrato_CNPJ_silver" "$JARS_AWS" "$s3DestinoFinal"
silver "relacao_ur_contrato" $BUCKET_NAME $sqsqueuename $bucket_key $DT_CARGA "conf_relacao_ur_quality" "conf_relacao_ur_contrato_CNPJ_silver" "$JARS_AWS" "$s3DestinoFinal"
silver "ur" $BUCKET_NAME $sqsqueuename $bucket_key $DT_CARGA "conf_ur_quality" "conf_ur_CNPJ_silver" "$JARS_AWS" "$s3DestinoFinal"
silver "erros" $BUCKET_NAME $sqsqueuename $bucket_key $DT_CARGA "conf_contrato_quality" "conf_erros_CNPJ_silver" "$JARS_AWS" "$s3DestinoFinal"

register_counter "Saida" "UR" "$s3DestinoFinal/ur/"
register_counter "Saida" "Contrato" "$s3DestinoFinal/contrato/"
register_counter "Saida" "Relacao Ur e Contrato" "$s3DestinoFinal/relacao_ur_contrato/"
register_counter "Saida" "Erros" "$s3DestinoFinal/erros/"

exit 0
