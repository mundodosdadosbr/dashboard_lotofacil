#!/bin/sh

export http_proxy=http://spobrproxy.serasa.intranet:3128
export https_proxy=http://spobrproxy.serasa.intranet:3128

if [ ${1} == "dev" ];then
   :
elif [ ${1} == "uat" ];then
    :
else
    bucket="datahub-comportamental-input-prod"
    credentials=$(cyberArkDap -s USCLD_PAWS_662860092544 -c BUUserForDataServicePipelineProd -a 662860092544)
fi

export AWS_ACCESS_KEY_ID=$(echo $credentials | cut -d' ' -f2)
export AWS_SECRET_ACCESS_KEY=$(echo $credentials | cut -d' ' -f3)
export AWS_DEFAULT_REGION="sa-east-1"

aws cloudformation delete-stack --stack-name experian-datahub-aws-lambda-cross-ast-path-to-s3-prod
aws cloudformation delete-stack --stack-name experian-datahub-aws-lambda-cross-ast-to-s3-prod
aws cloudformation delete-stack --stack-name experian-datahub-aws-lambda-cross-s3_to_ast-prod
