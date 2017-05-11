#! /bin/bash

aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Name=Spark --bootstrap-actions '[{"Path":"s3://hardoopmapreduce/bootstrap.sh","Name":"Custom action"}]' --ec2-attributes '{"KeyName":"lab21","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-03ef0a4b","EmrManagedSlaveSecurityGroup":"sg-36d89849","EmrManagedMasterSecurityGroup":"sg-dcd898a3"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.5.0 --log-uri 's3n://aws-logs-717329333846-us-east-1/elasticmapreduce/' --name 'My cluster' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://hardoopmapreduce/wea1.py"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"=","Name":"Spark application"}]' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master - 1"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR --region us-east-1
