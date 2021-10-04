import boto3

def handler(event, context):
    """
    Lambda function that starts a job flow in EMR.
    """
    client = boto3.client('emr', region_name='us-east-2')

    # Função que vai criar um cluster EMR na AWS e vai executar alguns Steps que definimos
    cluster_id = client.run_job_flow(
                # Nome do Cluester que vamos Criar
                Name='EMR-Kelson-IGTI-edc-tf',
                ServiceRole='EMR_DefaultRole',
                JobFlowRole='EMR_EC2_DefaultRole',
                VisibleToAllUsers=True,
                # Caminho onde o cluster vai salvar os logs
                LogUri='s3://datalake-kelson-igti-edc-tf/emr-logs',
                ReleaseLabel='emr-6.3.0',
                Instances={
                    # escolhendo quais são a instancias que vamos criar
                    'InstanceGroups': [
                        {
                            'Name': 'Master nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        },
                        {
                            'Name': 'Worker nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'm5.xlarge',
                            'InstanceCount': 1,
                        }
                    ],
                    # Precisa Criar a key pair na EC2 pois é obrigatorio
                    'Ec2KeyName': 'minha_key_pair',
                    'KeepJobFlowAliveWhenNoSteps': True,
                    # Informando que não queremos proteção para destroir o cluester
                    'TerminationProtected': False,
                    # Precisar trocar a Subnet
                    'Ec2SubnetId': 'subnet-019fc60bfd76e3a7f'
                },
                # Informando quais as aplicações o cluster vai ter
                Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Hive'},
                    {'Name': 'Pig'},
                    {'Name': 'Hue'},
                    {'Name': 'JupyterHub'},
                    {'Name': 'JupyterEnterpriseGateway'},
                    {'Name': 'Livy'},
                ],
                # Configurando as variaveis de ambiente que o spark vai utilizar
                Configurations=[{
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }]
                },
                    {
                        "Classification": "spark-hive-site",
                        "Properties": {
                            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        }
                    },
                    {
                        "Classification": "spark-defaults",
                        "Properties": {
                            "spark.submit.deployMode": "cluster",
                            "spark.speculation": "false",
                            "spark.sql.adaptive.enabled": "true",
                            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                        }
                    },
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        }
                    }
                ],
                # Informando a concorrencia de Step, no casa aqui está 1
                StepConcurrencyLevel=1,
                # Criando os steps que serão executados depois que cluster for criado
                Steps=[{
                    'Name': 'Delta Insert do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://datalake-kelson-igti-edc-tf/emr-code/pyspark/01_delta_spark_insert.py'
                                 ]
                    }
                },
                {
                    'Name': 'Simulacao e UPSERT do ENEM',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                 '--packages', 'io.delta:delta-core_2.12:1.0.0', 
                                 '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension', 
                                 '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog', 
                                 '--master', 'yarn',
                                 '--deploy-mode', 'cluster',
                                 's3://datalake-kelson-igti-edc-tf/emr-code/pyspark/02_delta_spark_upsert.py'
                                 ]
                    }
                }],
            )
    
    return {
        'statusCode': 200,
        'body': f"Started job flow {cluster_id['JobFlowId']}"
    }
