# 추후 필요 업데이트

### 찾아보니 각 db 마다 add column 문법이 있다...(왜 안찾아봤을까)  이걸 사용하면 merge,increrasement 처리 가능할듯 더 빠르게

```
칼럼들이
삭제만 -> append 시킨다
추가만 -> add column 후 append
삭제 추가 둘다 -> add column 후 append
```
### 예외 처리 안된부분이 있을 수도 있다.
### csv는  delimeter의 제약이 크므로 yml 형태로 저장한다. 단, snowflake는 yml 로드가 불가하기 때문에 이는 해결방법이 필요하다. 
### 제공 db의 종류를 늘리고, db 뿐만 아니라 ga,jira 같은 소스데이터도 가져올 수 있게 한다.
### 필요하다면 데이터 저장을 서버에 하지 않고, s3에 별도로 저장한다.
##### 데이터는 바이너리 코드로 저장하지 않는 것이 좋다. 무엇인가가 잘못되었을 때 사용자가 raw data를 볼 수 있기 때문에
##### 먼저 s3에 넣을 수 도 있었지만 가장 나중으로 미룬 이유는 s3는 돈을 내가 사용하는 서비스이기 때문에 querypie에 붙일 때 s3도 붙이는게 적합한지 판단을 할 수가 없어서 미루었다. 논의가 필요하다

### top10 use database in 2021 을 첨부한다
```
1. MySQL
2. Oracle
3. PG
4. Miscrosoft SQL Server
5. MongoDB
6. Redis
7. Elasticsearch
8. Cassandra
9. MariaDb
10. IBM Db2
```

