#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import smtplib
import email.message

# Define default arguments for the DAG
default_args = {
    'owner': 'Claudio Junior',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance with the defined default arguments
dag = DAG(
    'Gas_variation_dag',
    default_args=default_args,
    description='A simple Airflow DAG',
    schedule_interval=timedelta(days=7),  # Set the DAG to run daily
    catchup=False  # Disable backfilling
)

# Define tasks in the DAG

# Start the DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define a Python function to be executed as a task
def enviar_email():
    
    #scrapping
    url = 'http://gaswizard.ca/gas-price-predictions/'
    html = pd.read_html(url)
    df_1 = pd.DataFrame(html[0])
    df = df_1[['City', 'Regular','Premium']]
    Gas = pd.DataFrame(df[df['City'] == "Fredericton:"])
    df_2 = Gas[['Regular_','Aumento']] = Gas.Regular.str.split(" ",expand = True)
    df_regular = pd.DataFrame(df_2)
    df_3 = Gas[['Premium_','Aumento']] = Gas.Premium.str.split(" ",expand = True)
    amount_regular = float(df_2.iloc[0,0])
    aumento_regular_=(df_2.iloc[0,1])
    aumento_regular =aumento_regular_.replace('n/c','0%')
    amount_premium = float(df_3.iloc[0,0])
    aumento_premium_ = (df_3.iloc[0,1])
    aumento_premium =aumento_premium_.replace('n/c','0%')
    txt1 = 'The estimated regular price for gasoline today is {} CAD'.format(amount_regular)
    txt2 = 'it means a difference of  {} compared with last week'.format(aumento_regular)
    txt3 = 'For premium gasoline the estimated price today is {} CAD'.format(amount_premium)
    txt4 = 'it means a difference of  {} compared with last week'.format(aumento_premium)
    txt5 = 'Remember that is a prevision, the amount can vary according your region.'

    msg_1 = txt1+','+txt2+'.'+txt3+','+txt4+'.'+txt5
    #enviar email
    corpo_email = msg_1
    
    msg = email.message.Message()
    msg['Subject'] = 'Gasoline Variation'
    msg['From'] = 'YOUR EMAIL'
    msg['To'] = "RECEIVERS EMAILS SPLITED BY ,"
    password = 'YOUR PASSWORD'
    msg.add_header('Content-Type','text/html')
    msg.set_payload(corpo_email)
    
    s = smtplib.SMTP('smtp.gmail.com: 587')
    s.starttls()
    #login Credentials for sending email
    s.login(msg['From'],password)
    s.sendmail(msg['From'],msg['To'].split(","),msg.as_string().encode('utf-8'))
    print('Email enviado')

python_task = PythonOperator(
    task_id='python_task',
    python_callable= enviar_email,
    dag=dag,
)

# End the DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task dependencies
start_task >> python_task >> end_task

