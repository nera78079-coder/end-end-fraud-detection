## NB

### Type CASH_OUT or DEBIT or CASH-IN or PAYMENT or TRANSFER

### RIB (sender)or nameOrig: Name of the source account starts with C and is followed by 10 numbers.

### RIB (revciver) or nameDest: Name of the source account starts with C or M and is followed by 10 numbers.

### IsFlaggedFraud should be 0 or 1 mean not fraud or fraud

### Step is number represent hour in 12 format 

## Install services

### 1) install spark image

```bash
docker pull apache/spark-py
```


### 2) install kafka image

```bash
docker pull apache/kafka:4.0.1
```

### 3) install mongo databases

```bash
docker pull mongo
```



## Start services

### 1) start spark container

```bash
docker run -it apache/spark-py /opt/spark/bin/pyspark
```

### 2) start kafka container

```bash
docker run -p 9092:9092 apache/kafka:4.0.1
```

### 3) start mongo container

```bash
docker run mymong
```


### 4) start fastapi app
```bash
cd data_sience/end_to_end/fraud_detection/backend

fastapi dev backend.py 
```

### 5) start django app
```bash
cd data_sience/end_to_end/fraud_detection/app

python manage.py runserver 3000
```

## Setup

###  1) create the topic to store transaction credentiall

```bash 
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic transaction-credential \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```
###  2) create the topic to store the result after check transaction credential

```bash 
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic checked-transaction-credential \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
  
```

## Test 

### 1) kafka  

####  list all topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```


####  consume all transaction credential topics

```bash 
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic transaction-credential \
  --bootstrap-server localhost:9092 \
  --from-beginning
 ```
### 2) mongo databases  


```bash 
docker exec -it mongo mongosh

show dbs

use fraud-detection

show collections

db.getCollection("check-transaction-credential").find().pretty()

```


