# QUERYPIE EL

## 각 파일들에 대한 설명
###  start_elt

##### querypie_el 프로그램을 실행시키고 싶다면 그냥 이 실행파일을 실행시키면 된다. 실행시키는 방법은 {start_elt까지의 경로}/start_elt 를 입력하면 자동으로 실행된다.

### el_proto.py

##### 사용자로부터 데이터베이스 정보부터 어떤 테이블의 어떤 칼럼을 받고 싶은지 입력받는다. 단 프론트가 붙어있지 않기 때문에 몇가지 입력 규칙이 있다. 
###### (입력 받는 순서엔 미스가 있으나 프론트가 있으면 해결될 문제이기 때문에 무시하고 봐주길 바란다....)
    owner과 몇가지는 필수 입력사항이 아니다. 
    owner을 제외하곤 모두 치지 않을 경우에 대한 설명이 프로그램 실행중 나온다.
    입력을 다 받은 이후에는 사용자가 airflow.cfg에 설정해놓은 metadata의 위치에다가 el_metadata, metadata_tables_to_replicate라는 테이블들을 생성한다.
    metadata_tables_to_replicate엔 사용자가 입력한 정보중 테이블, 칼럼과 관련된 정보들이 저장된다.
    el_metadata 엔 사용자가 입력한 정보중 metadata_tables_to_replicate 엔 없는 dag의 구성요소들, db접속정보등이 저장된다.
    만약 위 metadata의 값을 바꾸면 실시간으로 사용자가 이미 만들어둔 dag에 반영이되며, dag_id는 절대 바꾸면 안된다.
>입력:
   job name,owner,schedule_interval,integrate_db_type,db login id, db login password,tables,rule,primary key, updated_at,columns,db 접속정보들
>>규칙:
  >>> ##### integrate_db_type : 소문자로 mysql/postgresql/snowflake/redshift를 정확히 입력해야 한다. 그렇지 않을 경우 계속 정확히 치라고 요구한다.
  >>> ##### db login id, db login password: 반드시 공백이 없게 쳐야하며 입력을 해야만 한다. 그렇지 않으면 재요구한다.
  >>> ##### tables : 스트링형태이며 여러개의 테이블을 선택 할 경우 a,b,c,d,e 와 같이 ,로 나눠야 한다.
  >>> ##### rule : el의 룰이며, 반드시 위에 입력한 테이블의 갯수만큼 입력해야 한다. 입력은 truncate/merge/increasement중 하나를 정확히 입력해야 한다. 만족하지 않으면 재요구한다. ex) merge,merge, truncate,increasement,merge
  >>> ##### priamary key : merge할 테이블의 pk이다. merge와 increasement는 이 pk기준으로 작동한다. merge에 쓰이는 pk는 상관없지만 increasement에 쓰이는 pk는 반드시 증분형 칼럼이어야 한다.
  >>> ##### updated_at column : 테이블에서  updated_at에 해당하는 칼럼을 입력한다. merge는 이 칼럼을 기준으로 데이터를 추출한다.
  >>> ##### columns : 반드시 입력해야하며  프로그램 실행중 입력하지 않으면 *이라는 내용은 프론트에선 디폴트가 모든 칼럼이 체크된 상태이기 때문에 넣어 놓은 문구이다. 리스트로 입력한다. ex) ['c,o,lum,n','columns','must','b,e','l,i,s,t']
  >>> ##### database,schema: 위에 입력한 table갯수에 맞춰 입력해야한다. 그렇지 않으면 재요구한다.
  >>> ##### 위 내용은 destination db에 대해 입력할때에도 마찬가지로 작용한다.
### get_sql_alchemy_conn.py
  ##### airflow.cfg에 저장한 sql_alchemy_conn 변수의 값을 가져오는데 사용되는 파일(함수)이다.
  ##### 여기서 sql_alcehmy_conn 은 위에서 언급한 metadata가 저장되는 위치의 url정보를 의미한다.

### make_a_dag.py
  ##### 이름대로 dag를 만드는 파일(함수)이다. 
  ##### dag의 구성요소는 아까의 메타데이타에서 모두 가져와 쓴다.

## do_extract.py / do_load.py
  ##### 추출.로드하는 파일(함수)이다.
  ##### 이 파일들은 dag이 실행될때 실행된다