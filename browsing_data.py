spark-submit \
--driver-class-path mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
--jars mysql-connector-java-5.1.40/mysql-connector-java-5.1.40-bin.jar \
/home/mouna_balghouthi/browsing_activity/browsing_data.py \
IP_Address \
DB_name \
user \
pass

##!/usr/bin/env python
import sys
import re
import itertools
from pyspark.sql import SparkSession

def parseActivity(row):
	return(row.td_ip, row.td_referrer, row.td_path, row.td_url)

if __name__ == "__main__":

	spark = SparkSession \
	        .builder \
	        .getOrCreate()

	CLOUDSQL_INSTANCE_IP = sys.argv[1] 
	CLOUDSQL_DB_NAME = sys.argv[2]
	CLOUDSQL_USER = sys.argv[3] 
	CLOUDSQL_PWD  = sys.argv[4] 

	jdbcUrl = 'jdbc:mysql://%s:3306/%s' % (CLOUDSQL_INSTANCE_IP, CLOUDSQL_DB_NAME)

	dfHistory = spark.read \
	    .format("jdbc") \
	    .option("url", jdbcUrl) \
	    .option("dbtable", 'morecon.users_browsing_history') \
	    .option("user", CLOUDSQL_USER) \
	    .option("password", CLOUDSQL_PWD) \
	    .load()


	RDDHistory = dfHistory.rdd.map(parseActivity).collect()
    #RDDHistory converted to list because od collect  
    


     # we want to filter 

        for act in RDDHistory:
                if act[3].find("search") > -1:
                        #td_url = "https://morecon.jp/search?category_id=&orderby=&power%5B%5D=%E5%BA%A6%E3%8
                        #len(td_url) = 268 ==> varchar(400)
                        search_filters = act[3].split("&")
                        power = search_filters[2][12:]
                        span = search_filters[3][11:]
                        color = search_filters[4][12:]
                        print power,span,color
                elif act[3].find("cart") > -1:
                        #td_url = "https://morecon.jp/cart/?product_id=Y&transactionid=XXX"
                        #product_id_cart = act[3].split("&")[0].split("=")[1]
                        print "product_id_cart"#, product_id_cart
                elif act[2].find("/i/") > -1:
                   # td_path = "/morecon.jp/i/283"
                   viewed_item = act[2].split("/",3)[3]
                   print "viewed_item",viewed_item
                elif act[2].find("/b/") > -1:
                   # td_path = "/morecon.jp/b/1109"
                   viewed_brand = act[2].split("/",3)[3]
                   print "viewed_brand",viewed_brand
                # get the last part of the string : http://192.168.216.68/morecon.jp/cart/categ?ref=66666bdfh
                if (act[1] == "https://www.facebook.com/") and (act[3].find("ref") > -1) :
                        referee = act[3].split("=")[1]
                        print "referee", referee
************************************************************************************************************************

SELECT do.customer_id , do.remote_addr, ubh.td_url , ubh.td_path , ubh.td_referrer
from dtb_order do 
join users_browsing_history ubh ON do.remote_addr = ubh.td_ip COLLATE utf8_unicode_ci 
where do.order_user_agent = ubh.td_user_agent COLLATE utf8_unicode_ci ;

#Inconvenient Time 

************************************************************************************************************************

TD => sp : automated workflow 

get data from Google cloud sql : automated?

map(customer_id, values)?





************************************************************************************************************************
# Save to cleaned Tables

dfRatings.write \
    .jdbc(jdbcUrl, "morecon.dtb_ratings",
            properties={"user": CLOUDSQL_USER, "password": CLOUDSQL_PWD})

# #[END save_top]
