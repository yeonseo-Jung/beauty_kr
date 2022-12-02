class AccessDb:
    username = "yeonseosla"
    password = "jys9807"
    database = "beauty_kr"
    host_url = "db.ds.mycelebs.com"
    port_num = 3306
    
class CreateQuery:
    def __init__(self, table_name):
        self.table_name = table_name
        
        if 'info_all' in self.table_name:
            self.category = self.table_name.replace('beauty_kr_', '').replace('_info_all', '')
        elif 'reviews_all' in self.table_name:
            self.category = self.table_name.replace('beauty_kr_', '').replace('_reviews_all', '')
        else:
            self.category = None
                
        self.query_dict = {
            'beauty_kr_mapping_table': "CREATE TABLE beauty_kr_mapping_table (\
            `item_key` int(11) DEFAULT NULL COMMENT '매핑 기준 상품 id',\
            `item_keep_words` varchar(255) DEFAULT NULL COMMENT '매핑 기준 상품 세부정보',\
            `mapped_id` int(11) DEFAULT NULL COMMENT '매핑 대상 상품 id',\
            `mapped_keep_words` varchar(255) DEFAULT NULL COMMENT '매핑 대상 상품 세부정보',\
            `source` varchar(255) DEFAULT NULL COMMENT '매핑 대상 상품 소스 테이블명'\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            f'beauty_kr_{self.category}_info_all': f"CREATE TABLE `beauty_kr_{self.category}_info_all` (\
            `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `item_key` int(11) DEFAULT NULL COMMENT '매핑 기준 상품 id',\
            `product_url` text COMMENT '상품 url',\
            `product_store` text COMMENT '상품 판매 스토어',\
            `product_store_url` text COMMENT '스토어 별 판매 링크',\
            `product_price` varchar(255) COMMENT '스토어 별 판매 가격',\
            `delivery_fee` varchar(255) COMMENT '스토어 별 배송비',\
            `naver_pay` varchar(255) COMMENT '네이버페이 유무',\
            `product_status` int(11) DEFAULT NULL COMMENT '상품 판매 상태 (1: 판매중, 0: 판매중단)',\
            `page_status` int(11) DEFAULT NULL COMMENT '상품 판매 페이지 상태 (1: 네이버 뷰티윈도 가격비교 탭, 2: 네이버 뷰티윈도 전체 탭, -1: 페이지 누락)',\
            `product_code` int(11) DEFAULT NULL,\
            `product_name` varchar(255),\
            `brand_code` int(11),\
            `brand_name` varchar(255),\
            `product_url_glowpick` text,\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            `descriptions` text,\
            `product_keywords` varchar(255),\
            `color_type` varchar(255),\
            `volume` varchar(255),\
            `image_source` text,\
            `ingredients_all_kor` text,\
            `ingredients_all_eng` text,\
            `ingredients_all_desc` text,\
            `ranks` varchar(255),\
            `product_awards` text,\
            `product_awards_sector` text,\
            `product_awards_rank` text,\
            `price` varchar(255) COMMENT '정가',\
            `product_stores` varchar(255) COMMENT '글로우픽 기준 판매 스토어',\
            `status` int(11) COMMENT '단종 여부 (0: 단종, 1: 판매중)',\
            `dup_check` int(11) COMMENT '중복 여부 (0: 단일상품(중복x), -1: 종속상품(중복o), 1: 대표상품(중복o))',\
            `dup_id` varchar(255) COMMENT '종속상품 id 리스트',\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자',\
            `category` varchar (255) DEFAULT NULL COMMENT '카테고리',\
            PRIMARY KEY (`pk`),\
            KEY `item_key` (`item_key`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            f'beauty_kr_{self.category}_reviews_all': f"CREATE TABLE `beauty_kr_{self.category}_reviews_all` (\
            `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `item_key` int(11) DEFAULT NULL COMMENT '매핑 기준 상품 id',\
            `txt_data` text COMMENT '리뷰 데이터',\
            `write_date` datetime COMMENT '리뷰 작성일자',\
            `product_rating` int(11) COMMENT '리뷰 평점',\
            `source` text COMMENT '데이터 출처 테이블 명',\
            `regist_date` datetime COMMENT '데이터 업로드 일자',\
            PRIMARY KEY (`pk`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'glowpick_product_info_final_version': f"CREATE TABLE `glowpick_product_info_final_version` (\
            `id` int(11) DEFAULT NULL COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `product_code` int(11) DEFAULT NULL COMMENT '글로우픽 내부 상품 코드',\
            `product_name` varchar(255),\
            `brand_code` int(11) COMMENT '글로우픽 내부 브랜드 코드',\
            `brand_name` varchar(255),\
            `product_url` varchar(255),\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            `descriptions` text COMMENT '상품 설명',\
            `product_keywords` varchar(255) COMMENT '상품 키워드',\
            `color_type` varchar(255) COMMENT '색상 or 타입',\
            `volume` varchar(255) COMMENT '용량',\
            `image_source` text COMMENT '상품 이미지 소스',\
            `ingredients_all_kor` text COMMENT '성분명(한글)',\
            `ingredients_all_eng` text COMMENT '성분명(영문)',\
            `ingredients_all_desc` text COMMENT '성분 설명',\
            `ranks` varchar(255) COMMENT '카테고리별 상품 랭킹',\
            `product_awards` text COMMENT '글로우픽 어워드명',\
            `product_awards_sector` text COMMENT '글로우픽 어워드 카테고리',\
            `product_awards_rank` text COMMENT '글로우픽 어워드 랭킹',\
            `price` varchar(255) COMMENT '정가',\
            `product_stores` varchar(255) COMMENT '글로우픽 기준 판매 스토어',\
            `status` int(11) COMMENT '단종 여부 (0: 단종, 1: 판매중)',\
            `dup_check` int(11) COMMENT '중복 여부 (0: 단일상품(중복x), -1: 종속상품(중복o), 1: 대표상품(중복o))',\
            `dup_id` varchar(255) COMMENT '종속상품 id 리스트',\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자',\
            KEY `id` (`id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'glowpick_product_info_final_version_review': f"CREATE TABLE glowpick_product_info_final_version_review (\
            `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `id` int(11) DEFAULT NULL COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `product_code` int(11) DEFAULT NULL COMMENT '글로우픽 내부 상품 코드',\
            `user_id` varchar(100) DEFAULT NULL COMMENT '유저 아이디',\
            `product_rating` int(11) DEFAULT NULL COMMENT '상품 평점',\
            `review_date` varchar(100) DEFAULT NULL COMMENT '리뷰 작성 일자',\
            `product_review` text DEFAULT NULL COMMENT '리뷰 내용',\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자',\
            PRIMARY KEY (`pk`),\
            KEY `id` (`id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'oliveyoung_product_info_final_version': f"CREATE TABLE `oliveyoung_product_info_final_version` (\
            `id` int(11) NOT NULL COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `product_code` varchar(100) DEFAULT NULL,\
            `product_name` varchar(100) DEFAULT NULL,\
            `product_url` varchar(255) DEFAULT NULL,\
            `brand_name` varchar(100) DEFAULT NULL,\
            `price` int(11) DEFAULT NULL,\
            `sale_price` int(11) DEFAULT NULL,\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            `brand_code` varchar(100) DEFAULT NULL,\
            `brand_url` varchar(100) DEFAULT NULL,\
            `product_rating` float DEFAULT NULL,\
            `product_size` varchar(100) DEFAULT NULL,\
            `skin_type` varchar(100) DEFAULT NULL,\
            `expiration_date` varchar(100) DEFAULT NULL,\
            `how_to_use` text,\
            `manufacturer` varchar(100) DEFAULT NULL,\
            `manufactured_country` varchar(100) DEFAULT NULL,\
            `ingredients_all` text,\
            `status` int(11) DEFAULT NULL,\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자'\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'oliveyoung_product_info_final_version_review': f"CREATE TABLE `oliveyoung_product_info_final_version_review` (\
            `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `id` int(11) NOT NULL COMMENT '자체부여 id: 매핑 시 item_key로 활용,\
            `product_code` varchar(100) DEFAULT NULL,\
            `product_url` varchar(255) DEFAULT NULL,\
            `user_id` varchar(100) DEFAULT NULL,\
            `product_rating` int(11) DEFAULT NULL,\
            `review_date` datetime DEFAULT NULL,\
            `product_review` text NOT NULL,\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자',\
            PRIMARY KEY (`pk`),\
            KEY `id` (`id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'beauty_kr_data_dashboard': f"CREATE TABLE `beauty_kr_data_dashboard` (\
            `pk` int(11) unsigned NOT NULL AUTO_INCREMENT,\
            `item_key` int(11) NOT NULL,\
            `brand_name` varchar(100) DEFAULT NULL,\
            `category` varchar(100) DEFAULT NULL,\
            `mapping_status` tinyint(1) DEFAULT NULL,\
            `available_status` tinyint(1) DEFAULT NULL,\
            `review_count` int(11) DEFAULT NULL,\
            PRIMARY KEY (`pk`),\
            KEY `item_key` (`item_key`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'glowpick_product_info_update_new': f"CREATE TABLE `glowpick_product_info_update_new` (\
            `id` int(11) DEFAULT NULL COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `product_code` int(11) DEFAULT NULL COMMENT '글로우픽 내부 상품 코드',\
            `product_name` varchar(255),\
            `brand_code` int(11) COMMENT '글로우픽 내부 브랜드 코드',\
            `brand_name` varchar(255),\
            `product_url` varchar(255),\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            `descriptions` text COMMENT '상품 설명',\
            `product_keywords` varchar(255) COMMENT '상품 키워드',\
            `color_type` varchar(255) COMMENT '색상 or 타입',\
            `volume` varchar(255) COMMENT '용량',\
            `image_source` text COMMENT '상품 이미지 소스',\
            `ingredients_all_kor` text COMMENT '성분명(한글)',\
            `ingredients_all_eng` text COMMENT '성분명(영문)',\
            `ingredients_all_desc` text COMMENT '성분 설명',\
            `ranks` varchar(255) COMMENT '카테고리별 상품 랭킹',\
            `product_awards` text COMMENT '글로우픽 어워드명',\
            `product_awards_sector` text COMMENT '글로우픽 어워드 카테고리',\
            `product_awards_rank` text COMMENT '글로우픽 어워드 랭킹',\
            `price` varchar(255) COMMENT '정가',\
            `product_stores` varchar(255) COMMENT '글로우픽 기준 판매 스토어',\
            `crawling_status` tinyint(1) DEFAULT NULL COMMENT '네이버 크롤링 여부',\
            `regist_date` datetime DEFAULT NULL COMMENT '개체 수집 일자'\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'naver_beauty_product_info_final_version': f"CREATE TABLE `naver_beauty_product_info_final_version` (\
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `product_url` text,\
            `product_name` varchar(255),\
            `brand_name` varchar(255),\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            `gruop_details` varchar(100) DEFAULT NULL,\
            `volume` varchar(100) DEFAULT NULL,\
            `main_feature` varchar(100) DEFAULT NULL,\
            `detail_feature` varchar(100) DEFAULT NULL,\
            `pa_factor` varchar(100) DEFAULT NULL,\
            `sun_protectiom_factor` varchar(100) DEFAULT NULL,\
            `skin_type` varchar(100) DEFAULT NULL,\
            `usage_area` varchar(100) DEFAULT NULL,\
            `usage_time` varchar(100) DEFAULT NULL,\
            `color` varchar(100) DEFAULT NULL,\
            `type` varchar(100) DEFAULT NULL,\
            `review_status` tinyint(1) DEFAULT NULL COMMENT '리뷰 존재(수집) 여부',\
            `regist_date` datetime DEFAULT NULL,\
            PRIMARY KEY (`id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'lalavla_product_info_final_version': f"CREATE TABLE `lalavla_product_info_final_version` (\
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '자체부여 id: 매핑 시 item_key로 활용',\
            `brand_name` varchar(100),\
            `product_name` varchar(100),\
            `price` int(11) DEFAULT NULL,\
            `sale_price` int(11) DEFAULT NULL,\
            `product_code` int(11) DEFAULT NULL,\
            `product_url` varchar(255) NOT NULL,\
            `selection` varchar(100) DEFAULT NULL,\
            `division` varchar(100) DEFAULT NULL,\
            `groups` varchar(100) DEFAULT NULL,\
            PRIMARY KEY (`id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'ewg_ingredients_all': f"CREATE TABLE `ewg_ingredients_all` (\
            `ewg_url` varchar(255) DEFAULT NULL COMMENT 'ewg 성분 상세 페이지 url',\
            `ewg_score` int(11) DEFAULT NULL COMMENT 'ewg 등급',\
            `ewg_ingredient_name` varchar(1000) DEFAULT NULL COMMENT 'ewg 기준 성분명',\
            `availability` varchar(100) DEFAULT NULL COMMENT 'ewg 등급 유효셩',\
            `score_img_src` varchar(255) DEFAULT NULL COMMENT 'ewg 등급 이미지 source',\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;",

            'beauty_kr_ingredients_all': f"CREATE TABLE `beauty_kr_ingredients_all` (\
            `ingredient_key` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '성분 key값 (개체 테이블과 조인 시 사용되는 key)',\
            `ingredient_en` varchar(1000) DEFAULT NULL COMMENT '영문 성분명',\
            `ingredient_ko` varchar(1000) DEFAULT NULL COMMENT '한글 성분명',\
            `ingredient_desc` varchar(1000) DEFAULT NULL COMMENT '성분 설명',\
            `ewg_url` varchar(255) DEFAULT NULL COMMENT 'ewg 성분 상세 페이지 url',\
            `ewg_ingredient_name` varchar(1000) DEFAULT NULL COMMENT 'ewg 기준 성분명',\
            `availability` varchar(100) DEFAULT NULL COMMENT 'ewg 등급 유효셩',\
            `score_img_src` varchar(255) DEFAULT NULL COMMENT 'ewg 등급 이미지 source',\
            `ewg_score` int(11) DEFAULT NULL COMMENT 'ewg 등급',\
            PRIMARY KEY (`ingredient_key`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='성분 테이블';",

            'beauty_kr_ingredients_bridge_table': f"CREATE TABLE `beauty_kr_ingredients_bridge_table` (\
            `item_key` int(11) NOT NULL COMMENT 'glowpick_product_info_final_version id (자체부여 상품 id)',\
            `ingredient_key` int(11) NOT NULL COMMENT 'beauty_kr_ingredients_all ingredient_key (자체부여 성분 key)'\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='성분테이블, 글로우픽 개체 테이블간의 bridge_table';",
        }