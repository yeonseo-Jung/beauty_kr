## Project Beauty Kr - Data Manager
### Data Crawling & Scraping | Preprocessing | Product Mapping | Database Management
  
[Datamanager Tutorial](https://olivine-wasabi-3fc.notion.site/DataManager-Tutorial-f093d41ef86748399f377d6359e18714)
  
[프로그램 실행파일 다운로드(Datamanager.exe)](https://drive.google.com/file/d/1UOSebjt6qwn9PDJC8v-dcSCGWu9TRk4m/view?usp=sharing)
  
[QA Test Spread Sheets](https://docs.google.com/spreadsheets/d/1jJBlk58GxtYhEoQnbeEgve5x7tizPBMC4AxM113je10/edit?usp=sharing)

---

### version 0.0.1
  
#### Connect Database

##### 데이터 베이스 연동을 위한 로그인 화면 & 메뉴 선택

**→ user name, password, database 입력** 

**→  작업 메뉴 선택** 

**→ Connect 버튼 클릭**

![login_db_connect.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/login_db_connect.png)

#### Mapping Products

##### 상품 매핑 프로세스 메뉴

→ **Table List에서 매핑 대상 테이블 선택 (체크)** 

**→ Import 버튼 클릭 (db에서 테이블 가져오기)** 

**→ View 버튼 :  테이블 보기 (table viewer)** 

**→ Save 버튼 : 테이블 다른이름으로 저장 (.csv)**

**→ Preprocess :  상품명 전처리, 카테고리 재분류**  

**→ Compare :  상품 그룹핑 후 데이터 비교 연산 작업** 

**→ Mapping :  매핑 기준에 따라 매핑테이블 생성** 

**→ Status :  매핑 현황 보기** 

**→ Update :  db에 매핑 테이블 업데이트**   

![스크린샷 2022-04-28 오후 6.32.51.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2022-04-28_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_6.32.51.png)

#### Scraping Product Info

##### 네이버 뷰티윈도 미매핑 상품 크롤링

**→ 상품 카테고리 선택 (체크, 다중체크 가능)**

**→ Accept 버튼 :  db에서 매핑 테이블과 기준테이블 가져온 후 미매핑 상품 추출하기 (미매핑 상품 테이블 생성)**

**→ Run 버튼 :  스크레이핑 시작** 

**-> Pause 버튼 :  스크레이핑 일지정지 (Run 버튼 다시 클릭 시 이어서 스크레이핑 가능)**

**→ View 버튼 :  스크레이핑 데이터 실시간으로 확인** 

**→ Save 버튼 :  스크레이핑 데이터 다른이름으로 저장 (.csv)**

![crawling_naver_products.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/crawling_naver_products.png)

#### Get Table From Database

##### db에서 원하는 테이블, 원하는 컬럼 가져오기

**→ 테이블 선택 → Select 버튼 클릭**

**→ 컬럼 선택 (체크) → Import 버튼 클릭** 

**→ View 버튼 :  테이블 보기 (table viewer)**

**→ Save 버튼 : 테이블 다른이름으로 저장 (.csv)**

![스크린샷 2022-04-28 오후 7.12.45.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2022-04-28_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_7.12.45.png)
  
---
  
### version 0.0.2 Update
  
#### Update Glowpick Products

##### 글로우픽 신규 상품 개체 & 리뷰 업데이트

→ 카테고리 선택

→ 신규 상품코드 수집 (Run)

→ 신규 상품 개체 & 리뷰 수집 (Run)

→ 수집과 동시에 db 테이블 형식에 맞게 전처리 

→ Table Upload

- glowpick_product_update_{date}
- glowpick_product_update_review_{date}

![update_glowpick_products.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/update_glowpick_products.png)

#### Update Naver Products Sales Status

##### 네이버 매핑 상품 판매 현황 및 링크 업데이트

→ 카테고리 선택

→ 수집과 동시에 db테이블 형식에 맞게 전처리 

→ Table Upload

- beauty_kr_{category}_info_all : 카테고리별 매핑 상품 개체 테이블

![update_naver_products_status.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/update_naver_products_status.png)

#### Update Review Table

##### 매핑 상품에 대한 리뷰 테이블 업데이트

→ Select review table

→ Select category

→ Duplicate Check: 리뷰 중복 제거 

→ 매핑 상품 추출 및 리뷰 테이블 전처리 후 업로드 진행 

![update_review_table.png](DataManager%20c4b4fcb75c0c444092530c8f5cbdbc10/update_review_table.png)