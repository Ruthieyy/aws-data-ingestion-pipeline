import boto3
import json

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # 读取上传的文件
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read().decode('utf-8')
    
    # 处理数据：加一行标记
    processed = "processed=true\n" + content
    
    # 写回S3
    output_key = 'processed/' + key
    s3.put_object(Bucket=bucket, Key=output_key, Body=processed)
    
    print(f"处理完成！结果存到 s3://{bucket}/{output_key}")
    
    return {
        'statusCode': 200,
        'body': f'成功处理 {key}'
    }
