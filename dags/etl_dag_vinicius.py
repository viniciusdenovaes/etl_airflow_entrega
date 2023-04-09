import pandas as pd
import datetime as dt
import os


# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
# from pyspark.sql import SparkSession


from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

from airflow.decorators import task

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator



def transform(table: pd.DataFrame) -> pd.DataFrame:
    name = table.name

    # unit of every product (hardcoded)
    unit = 'm3'

    # listing mapping brazilian months to integers
    months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    months_to_int_d = {m: i for m, i in zip(months, range(1, 12 + 1))}
    months_to_int = lambda x: months_to_int_d[x]

    # transforming the data
    # logger.info(f'melting table {table.name}')
    table = table.melt(id_vars=['ANO', 'COMBUSTÍVEL', 'ESTADO'],
                        value_vars=months, var_name='MONTH',
                        value_name='volume')
    table['month_int'] = table.apply(lambda x: months_to_int(x['MONTH']), axis=1)
    table['year_month'] = table.apply(lambda x: dt.datetime(x['ANO'], x['month_int'], 1), axis=1)
    table.rename({'COMBUSTÍVEL': 'product', 'ESTADO': 'uf'}, inplace=True, axis=1)
    table['unit'] = unit
    table['created_at'] = pd.Timestamp.now()
    table = table[['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']]
    table.name = name

    return table





def save(df: pd.DataFrame, path):
    # spark = SparkSession.builder.appName('PySpark').getOrCreate()

    # df = df.fillna(0)

    # schema = StructType(
    #     [StructField("year_month", DateType(), True),
    #      StructField("uf", StringType(), True),
    #      StructField("product", StringType(), True),
    #      StructField("unit", StringType(), True),
    #      StructField("volume", DoubleType(), True),
    #      StructField("created_at", TimestampType(), True),
    #      ])
    #
    # # print(f'makeing dataframe pd to spark {df.name}')
    # for e, (i, r) in enumerate(df.iterrows()):
    #     if e > 10: break
    #     print(i, r)
    #
    # pd_df = df
    # print(pd_df[(pd_df['product']=='QUEROSENE DE AVIAÇÃO (m3)') & (pd_df['uf']=='RONDÔNIA') & (pd_df['year_month']==dt.datetime(2020, 3, 1))])

    # sp_df = spark.createDataFrame(df, schema=schema)
    #
    # print(f'dataframe pd to spark complete {df.name}')
    #
    # sp_df.write.option("header", True) \
    #     .partitionBy("product", "year_month") \
    #     .mode("overwrite") \
    #     .parquet(os.pathlib.join(path, "vendas_p_y.parquet"))
    #
    df.to_parquet(path="/tmp/vendas_p_y.parquet",
                  partition_cols=["product"])



with DAG(
    "etl_all_pandas_vinicius",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="etl for raizen using only pandas",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["raizen", "entrega"],
) as dag:


    xlsx_to_ods = BashOperator(
        task_id='xlsx_to_ods',
        bash_command="soffice --headless --nologo --norestore --convert-to ods --outdir /tmp/ /tmp/vendas.xls",
    )


    download = BashOperator(
        task_id="download",
        bash_command="wget https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls -O /tmp/vendas.xls",
    )


    download >> xlsx_to_ods


    worksheets_names = {
        'diesel': 'DPCache_m3_2',
        'oilder': 'DPCache_m3',
    }
    load_tasks = {}
    for name_item, name_sheet in worksheets_names.items():


        @task(task_id=f'load_transform_save_{name_item}')
        def load_transform_save():
            print(f'load_worksheet_for_{name_item}')
            pd_df = pd.read_excel('/tmp/vendas.ods', name_sheet)
            print(f'loaded {name_item}')
            pd_df.name = name_item
            print(f'transforming {name_item}')
            pd_df = transform(pd_df)
            print(f'saving {name_item}')
            save(pd_df, '/tmp/')

        load_task = load_tasks[name_item] = load_transform_save()
        xlsx_to_ods >> load_task




