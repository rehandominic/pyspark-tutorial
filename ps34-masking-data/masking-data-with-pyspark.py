from pyspark.sql.functions import udf
from pyspark.sql.functions import *

def mask_email(colVal):
    mail_usr=colVal.split("@")[0]
    n=len(mail_usr)
    charList=list(mail_usr)
    charList[1:int(n)-1]='*'*int(n-2)
    out="".join(charList)+"@"+colVal.split("@")[1]
    return out
  
  def mask_mobile(colVal):
    n=len(colVal)
    charList=list(colVal)
    charList[2:int(n)-2]='x'*int(n-4)
    return "".join(charList)
  
  def mask_ip(colVal):
    n=len(colVal)
    charList=list(colVal)
    charList[2:int(n)-2]='x'*int(n-4)
    return "".join(charList)
  
mask_mobile_udf = udf(mask_mobile, StringType())
mask_email_udf = udf(mask_email, StringType()) 
mask_ip_udf = udf(mask_ip, StringType())

df_masked=df.withColumn("Masked_Email",mask_email_udf(df["email"])) \
            .withColumn("Masked_Mobile",mask_mobile_udf(df["mobile_number"])) \ 
            .withColumn("Masked_IP",mask_ip_udf(df["ip_address"]))

display(df_masked)
