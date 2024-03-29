import boto3
import pprint as pp
import pandas as pd
import io
s3_client=boto3.client('s3')
bucket_list=s3_client.list_buckets()
bucket_name='data-eng-resources'

#open csv file in pandas
s3_object=s3_client.get_object(
    Bucket=bucket_name,
    Key='python/fish-market.csv'
)

pp.pprint(s3_object)
df=pd.read_csv(s3_object['Body'])
# print(df.head())

#cleanse the data
df_filtered = df[(df != 0).all(axis=1)]
#get the avg of each columns by species
avg_by_species=df_filtered.groupby('Species').mean()
# print(avg_by_species)

#round all numbers to two decimal places
two_decimal_result=avg_by_species.applymap(lambda x: round(x, 2))
# print(two_decimal_result)

#convert this Daraframe to csv file
str_buffer=io.StringIO()
two_decimal_result.to_csv(str_buffer)
#upload to S3
s3_client.put_object(
    Body=str_buffer.getvalue(),
    Bucket=bucket_name,
    Key='Data401/lihong.csv'
)