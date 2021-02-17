# DBT
## ETL과 ELT

 ##### ETL은 Extract Transform Load 의 약자로, 데이터를 추출해서 변형시킨 다음 적재한다는 의미이다.
 ##### ELT란 ETL에서 T와 L의 순서가 바뀐것으로, 데이터를 추출해서 적재한 다음 변형시킨다는 의미이다. 
 ##### 최근 데이터 플로우는 ETL에서 ELT로 바뀌는 추세인데, 이는 저렴해지고 좋아진 db의 성능에 있다.
## DBT란? & DBT를 사용하는 이유

#####  DBT는 ELT에서 T만을 담당한다. 그렇기 때문에 ELT를 하고자 한다면 EL을 책임질 다른 툴을 같이 사용해야한다.
##### DBT는 .yml , .sql 파일만 읽고, 실행시킬 수 있다.
##### DBT는 사용자가 작성한 소스 코드를 각 db에 맞는 sql문으로 바꿔서 웨어하우스에서 실행시키는 일만 한다.
##### DBT가 하는 일은 한가지 밖에 없는데 많은 사용자가 이용하는 이유는 DBT를 사용하면 jinja를 사용해서 sql을 짤 수 있기 때문이다. 이것이 위에서 sql문이 아니라 소스 코드라고 표현한 이유이다.
##### 사용자 뿐만 아니라 DBT프로그램도 jinja를 사용해 sql문을 짜기 때문에 DBT model, macro, snapshot,  analysis, test, data 의 다섯가지 기능을 제공할 수 있다.
## 1. git을 통해 CLI형식 DBT를 설치하는 방법

 ### 1. dbt home 디렉토리를 만들고 (이하 dbt_home) dbt를 설치한다
 ```
  mkdir dbt_home
  cd dbt_home
  git clone https://github.com/fishtown-analytics/dbt.git
  cd dbt
  pip install -r editable_requirements.txt
```
 ### 2. DBT_PROFILES_DIR 환경변수 설정 , zsh를 깔았다면 .bashrc 대신 .zshrc를 사용하자!
  ```
cd location/to/your/dbt_home
  mkdir .dbt
  cd
  vim .bashrc
  export DBT_PROFILES_DIR='location/to/your/dbt_home/.dbt'      저장
```
 ### 3. dbt디렉토리에 profiles.yml 을 만든다. 이 파일은 나중에 프로젝트에서 연결할 커넥션들의 모임이 된다.
  ```
    cd location/to/your/dbt_home/.dbt
    vim profiles.yml
    커넥션 정보들 저장
```
  ### 4. projects 디렉토리에 자신의 프로젝트를 만든다. 이것은 dbt init을 사용한다. 여기서 무조건 자신의 db 종류(snowflake/PG...) 를 옵션으로 주는데 무조건 자신이 사용할 db에 맞게 설정하는것을 추천한다. (사실 dbt는 추천을 넘어서 사용할 db와 다르게 설정하지 말아달라고 언급했다)
  ```
  cd location/to/your/dbt_home
  mkdir projects
  cd projects
  dbt init {project_name} --adapter {your db type}
```
 ### 5. 자신의 프로젝트의 dbt_project.yml에서 프로젝트에 대한 설정을 한다. 여기에선 각종 테스트를 만들 수 있고, 각 소스들의 위치, 여러 디폴트값등을 지정 할 수있다. 또한 해당 프로젝트의 커넥션을 설정 할 수있다.
```
 프로젝트는 커넥션을 하나만 가질 수 있다. 즉, 같은 프로젝트에 있는 소스파일들은 모두 같은 커넥션에서만 돌게 되는 것이다. 
```

## 여기 까지하면 기본적인 설치가 끝난다.
## 다음은 dbt의 기능들에 대한 내용들이다.

### model  : DBT의 가장 주가 되는 기능이며 가장 강력한 기능 중 한가지
 ##### 단일 select 쿼리 하나가 하나의 파일이 되며 이 파일의 이름은 '모델'이다. (subquery 의 select는 괜찮다)
##### 그 이유는 한 모델의 실행 결과는 하나의 데이터 테이블로 만들어지기 때문이다.
##### 기본 데이타베이스, 스키마는 profiles.yml 에서 설정해둔 곳으로 자동지정 되며, 테이블의 이름은 기본적으론 파일의 이름이다. 이들을 포함한 여러 설정들을 모델의 가장 위에서 config를 통해 바꿀 수 있다.
##### config중 특히 materialized라는 변수가 있는데 이것은 결과값을 나타낼 방법을 선택한다. 선택지로는 table, veiw, incremental, ephemeral 가 있다.. 
   >> incremental은 증분테이블, ephemeral은 저장되지 않는 임시테이블을 의미한다.
   >> 디폴트 값은 위에서 설정한  dbt_project.yml에 설정되어 있는값이며 이것조차 설정하지 않았다면 디폴트 값은 뷰이다. 
   >> 주의 해야될점은 table,view엔 증분등 upsert를 설정할 수 없으며, 이들은 select의 결과값이 그대로 테이블로 만들어지기 때문에 기존에 있던 테이블/뷰를 drop 한다음 새로 만들어진다
   >> merge 혹은 upsert를 하기 위해선 incremental을 이용해야 한다.
##### dbt run을 통해 테이블을 실제로 만들며, dbt compile을 하면  jinja와 sql이 섞인 소스코드를 각 db에 맞는 sql문으로 바꿔서, target/compile 디렉토리에 넣는다. 
##### dbt run만 해도 모델은 compile이 된다.
##### 원하는 모델만 돌리는 방법은 dbt run --models {모델이름} 이다.
##### 또한 일종의 모델의 그룹을 선택해 그 그룹만 돌릴 수 있는데 그것은 문서의 tags를 참고하면 되겠다.
##### **모든 컴파일된 결과들은 target/compile 디렉토리 안에 있다.**

##### model들이 돌아가는 순서 알고리즘들

 >> ref 함수가 사용 되지 않았다면 model전체 파일들을 abc 순으로 나열한다음 순서대로 실행한다.
>>  ref model이 하나 있다면. ref 함수가 호출한 모델 먼저 실행하며, 나머지 모델들을 abc순으로 진행한다. 그리고 ref함수가 정의된 모델을 가장 마지막으로 돌린다.
>>  같은 디렉토리 안에 ref 함수가 정의 된 모델이 여러가지 일때, 모든것은 2 번 알고리즘에 의해 순서를 정하지만 ref함수가 정의 된 모델만은 순서가 2번과는 완전 반대이다. 역 abc 순이다.
>>  서로 다른 디렉토리 안에 ref 함수가 여러개 있을 경우. 가장 얕은 단계에 있는 ref 함수가 정의된 모델을 실행한다음 점점 깊이 있는 모델들을 돌린다. 그외 모든 순서는 1,2,3번 알고리즘을 따른다.

### analysis
#####  위의 model 과 완전히 같다. 하지만 이놈들은 실행결과가 db에 저장되지 않는다.
##### 이 기능은 사용자가 굳이 테이블로 만들지 않아도 되는 복잡한 쿼리문들을 jinja를 통해 간단히 만들어 두고 저장해 두고 공유하기위한 기능이다.
##### 이 파일들의 컴파일 된 결과를 그대로 db에서 돌리면 사용자가 원하는 결과값을 볼 수 있다.
##### 다른 파일들을 컴파일한 결과들은 결과값을 볼 수 있을 뿐 아니라 테이블 혹은 뷰로 저장까지 한다.
##### dbt run을 통해서 돌아가지 않고 dbt compile을 통해서만 컴파일 시킬 수 있다.

### snapshot
##### 각 파일에서 지정해둔 model의 결과인 테이블의 데이터가 바뀌었는지, 바뀌었으면 언제 바뀌었는지 까지 파악해서 파악한 내용을 테이블로 만들어주는 기능이다.
##### .sql 파일로 만들며, 무조건 jinja의 macro를 정의하듯이 만들어야 한다.
   >> 즉 맨 위엔 {% snapshot [이름] %} 마지막엔 {% endsnapshot %} 이 와야 한다.
##### snapshot도 결과값을 테이블을 만들지만 테이블이 아닌 뷰로는 만들지 못한다.
##### 두번째 줄엔 반드시 config가 있어야 한다. 그 config에는 필수 입력사항이 존재하는데, target_schema, unique_key,strategy 이다.
   >> target_schema는 어느 스키마에 테이블을 만들것인지를 의미한다.
   >> unique_key는 레코드를 어느 칼럼에 맞출 것인지를 의미한다.
   >> strategy는 스냅샷전략을 선택하는 것인데 dbt엔 timestamp/check두개뿐이다.
        타임스탬프 전략은 해당 데이터의 업로드/업데이트 시간을 보고 스냅샷을 뜨는 선택지이고,
        체크는 선택한 칼럼의 데이터가 변했는지에 따라서만 스냅샷을 떠주는 선택지이다.
##### 그 다음엔 단일 select 쿼리문을 만들어야 한다. 역시 jinja 사용이 가능하다.
##### 처음에 스냅샷을 실행하면 테이블이 만들어지지만 다음부터는 만들어지지 않고 기존의 테이블에 추가하는 형식이다.
##### a라는 row의 데이터가 바뀌었다면 a에 해당하는 row의 데이터를 바꾸는 것이 아니라 테이블의 가장 밑에 추가한다.
##### 그렇기 때문에 날짜형식으로 스냅샷을 뜰때 가장 최근 데이터가 가장 위로 올라오게 하고 싶다면 order by 를 무조건 설정해줘야한다.
#####  dbt run으로 돌지 않고 dbt snapshot을 통해 실행가능하다.

### test
##### dbt를 통해 만들어진 테이블들이 사용자의 기준에 맞게 만들어졌는지를 확인해 보는 기능이다.
##### dbt의 test는 두가지 종류가 있는데
##### 하나는 테이블을 만들때 옵션으로 사용하는 notnull,pk,uniquekey같은 기준들을 테스트해보는 test(이하 옵션 테스트),
   >> 옵션 테스트는 models 디렉토리 안에 .yml 로 만든다.. 여기에선 테스트할 모델의 이름, 칼럼 이름, 테스트 종류를 설정하며 그 외 자잘한 설정들을 한다.
##### 데이터가 사용자가 쿼리로 짤 수 있는 조건에 맞는지 확인 하는 test (이하 쿼리 테스트) 두가지가 있다.
   >> 쿼리 테스트는 tests 디렉토리 안에 .sql 파일로 만들며 단일 select 쿼리를 짠다.
   >> select 쿼리의 결과가 곧 에러임. select 쿼리의 결과가 0개이면, 에러가 나지 않았다는 것이다.
##### test의 옵션중 에러가 나면 error 를 낼지, warning을 낼지 선택 할 수 있다.
##### 두 경우 모두 쿼리의 결과가 db에 데이터로 저장되지는 않는다.
##### 결과로 error/warning/success 중 한가지 다음 숫자가 실행결과로 뜨는데 이 숫자가 에러( 사용자가 위에서 지정한 옵션 혹은 쿼리의 결과) 갯수이다.
##### dbt run을 통해 돌아가지 않으며 dbt test를 통해서만 돌아간다.

### 총평
 ##### 장점
   >> jinja가 포함된 소스코드를 sql로 변환하는 기능 덕분에 보통 transform 툴보다 많은 기능을 제공할 수 있다.
   >> 실행해야만 하는 쿼리들의 dependency또한 설정가능하고 스케쥴링도 가능하다.

##### 단점
   >> 스케줄링을 하기 위해선 에어플로우를 사용해야한다,
   >> ELT 중 T만 감당할 수 있으므로 EL을 해결해야 하는 툴을 사용해야 한다.
   >> 가장 큰 단점으론 지원하는 db의 종류가 적다.

##### 단점 해결법
   >> airflow와 연동하여 사용한다.
   >> airflow의 스케쥴러, 웹서버를 사람이 실행시키지 않고 설치과정에서 실행되게 한다.
   >> db들에 맞는 드라이버, 훅들을 개발한다.

##### 이 단점들만 해결이 가능하다면, 엔진 사용을 하기애 충분한것으로 판단 된다

[dbt docs](https://docs.getdbt.com/docs/introduction)  
[jinja docs](https://jinja.palletsprojects.com/en/2.11.x/templates/#template-designer-documentation)  
[rogan dbt test](https://github.com/chequer-io/dbt_engine)  
(코로나 관련 데이터로 dbt를 시험해봤습니다. 물론 가설이 맞는지도 모르고, 모든 데이터를 뽑지는 않았지만, dbt를 테스트 할만큼 뽑아놨습니다.출처는 공공데이터 포털이며, projects/covid를 보시면 될겁니다..)   
(데이터는 querypie workspace - snowflake - airflow_database - test&snapshots 에 있습니다.)



