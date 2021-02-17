These instructions are about installing Airflow on a Linux(amazon linux) server (not using Docker). We will be using Python 3.7.x

# 1. if you need som auth of your sever, do it 

# 2. update yum & install python
```
sudo yum update  
sudo yum -y groupinstall "Development Tools"  
sudo yum -y install gcc openssl-devel bzip2-devel libffi-devel wget  
sudo yum -y install python3 python3-devel
```

# 3. create your venv
```
mkdir venvs
python3 -m venv venvs/[YOUR VENV NAME]
source ~/venvs/[YOUR VENV NAME]/bin/activate
pip install boto3
```
## if you want to active your venv when you log in, do these. These are OPTIONAL
```
deactivate
echo "source ${HOME}/venvs/[YOUR VENV NAME]/bin/activate" >> ${HOME}/.bashrc
source ~/.bashrc
```

# 4. install airflow
```
pip install --upgrade pip==20.2.4
AIRFLOW_VERSION=1.10.14
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
sudo yum -y install python-devel mysql-devel
sudo yum install cyrus-sasl-devel
pip install wheel
pip install sasl
pip install sqlalchemy-redshift
pip install 'apache-airflow[all]'
pip install apache-airflow-providers-mysql
pip install apache-airflow-providers-snowflake
pip install oauth2client
cd airflow
mkdir dags
```
# 4_5. optional 
```
vim ~/airflow/airflow.cfg
```
```
load_examples -> False
load_default_connections -> False
sql_alchemy_conn,broker_url,result_backend -> your backend database
```

# 4. init your airflow
```airflow initdb```
# 5. turn on your airflow webserver
```
airflow webserver -p 8080
```
## close cmd/terminal
## and log in server again

# 6. turn on your airflow scheduler
```
airflow scheduler
```
## close cmd/terminal 
## and log in server again 

# 7. make zombie scheduler
```
nohup airflow scheduler &
```
# PRESS CONTROL C
```
tail -f nohup.out
```

# 8. make you can connect with only with your server's ip
```
sudo amazon-linux-extras install nginx1 -y
sudo service nginx start
ps -ef | grep nginx
sudo vi /etc/nginx/nginx.conf
```
merge this
```
Server {  

        location / {
        proxy_set_header    Host                $host;
        proxy_set_header    X-Forwarded-For     $proxy_add_x_forwarded_for;
        proxy_set_header    X-Forwarded-Proto   $scheme;
        proxy_set_header    Accept-Encoding     "";
        proxy_set_header    Proxy               "";
        proxy_pass          http://127.0.0.1:8080;
        }
}
```
```
sudo service nginx restart
```

## FINISH

airflow home : /home/ec2-user/airflow  
example dags : /home/ec2-user/venvs/[YOUR VENV NAME]/lib/python3.7/site-packages/airflow