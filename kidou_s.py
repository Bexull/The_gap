import telebot
import psycopg2
import requests
import fake_useragent
import re
import io
import smtplib


import psycopg2.extras as extras
import pandas as pd
from psycopg2 import Error


from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File


from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


from bs4 import BeautifulSoup as BS


class Config():
    TOKEN = {
        "CITY" : "5125330775:AAEIDx_dMH421aX1PIdBkPOoHECWhbqeSKQ",
        "KENMART": "5314863628:AAGjAXLKzkcQFtgEKc0vc_EegZTAUIgZfxg",
        "MH" : "AAGsN139gGTnmtIRcvYJ6yj6ShmuE3ywKGc",
        "ALASH" : "5875443816:AAF9H-trV93Nn_JFuA35oajIaPZHh_rPsEY",
        "LAPLACE" : "5418831945:AAFZqeg1EiklkNVF3U41ohjecArnawHslYg",
        "EXPRESS_01" : "5888973232:AAGFzVKiVGIB1heQcaDLhTZJwd6Byw7g7qQ",
        "EXPRESS_02" : "6166777092:AAEDhJCM5JRYT_ITs46ZrCWtGTU8_LyLGWo",
        "EXPRESS_03" : "6640594385:AAHUw0kEThJ3OyBBaW4N8uHCrwAPhW8r76g",
        "SB_JOURNAL" : "6230380280:AAF6hbhPGBCvlLjJ4o3ogNJxuLdWTzeA_H0",
        "JARVIS" : "5169310712:AAFxLSyyTy4CyX2GEdCCwM1CUMT-SpssD4Y",
        "EDITH" : "5965627466:AAHjtkI589riGWMwhZxIF-9PaaRBMfec6Lk",
        "MINUS_1K" : "6617194073:AAFlDmHap22uUnWaOEbGUl-7Z-iaeZKNK0U"
    }
    
    
    TB_GROUP = {
        "TB_ERROR":-878206567
    }

    MICROSOFT_ACC = {
        "USERNAME" : "bexultan.kobetay@e-magnum.kz",
        "PASSWORD" : "Magnum2025!!!."
    }
    #TODO

    EMAIL_ACC = {
        "USERNAME":'laplace.mail.test@gmail.com',
        "PASSWORD":'hzfsuvhblryyzfvn',
    }
    
    

    LWH_LOGIN_PASS = {"username": "77089765060",
                        "password": "9765060"}
    #TODO

    LWH_URL = {
        "AUTH" : "https://generic-k8s-prod.e-magnum.kz/lwh/auth",
        "SET_LIMIT" : "https://generic-k8s-prod.e-magnum.kz/lwh/setAvailableLimitFromFile",
        "STOCK" : "https://generic-k8s-prod.e-magnum.kz/lwh/store?storeId=",
        "LIMIT" : "https://generic-k8s-prod.e-magnum.kz/lwh/availableLimitEdit?storeId="
        }


    SQL_STOCK_ACC = {
        "USER" : "stock_user_serv",
        "PASSWORD" : "9y0h90MRO7Ay",
        "HOST" : "pg14-uran-prod.e-magnum.kz",
        "PORT" : "5432",
        "DATABASE" : "stock"
    }
    

    SQL_DWH_ACC = {
        "USER" : "bexultan_kobetay",
        "PASSWORD" : "T8rst5fEbFe51n6!",
        "HOST" : "pg-dwh-prod.e-magnum.kz",
        "PORT" : "35432",
        "DATABASE" : "dwh_prod"
    }
    #TODO

    SQL_HUB_ACC = {
        "USER" : "bexultan_kobetay",
        "PASSWORD" : "T8rst5fEbFe51n6!",
        "HOST" : "pg-dwh-prod.e-magnum.kz",
        "PORT" : "35432",
        "DATABASE" : "datahub"
    }

    SQL_WMS_ACC = {
        "USER" : "tbot_user",
        "PASSWORD" : "mYkSOIzTPfmf7Du6GN!",
        "HOST" : "pg14-uran-prod.e-magnum.kz",
        "PORT" : "5432",
        "DATABASE" : "wms_bot"
    }
    #TODO

    def lwh_api_def(self, merchant_id):
        api = f"https://lwh-lv.e-magnum.kz/?storeId={merchant_id}&sku="
        return api
        #API ссылкы для гет запроса остатков по СКЮ  
    

    def lwh_stock_url(self, merchant_id):
        url = self.LWH_URL['STOCK'] + str(merchant_id)
        return url


    def lwh_limit_url(self, merchant_id):
        url = self.LWH_URL['LIMIT'] + str(merchant_id)
        return url


    def bot_def(self, merchant):
        bot = telebot.TeleBot(self.TOKEN[merchant]) 
        return bot



class Dict_catalog():
    pass



class SQL_requests(Config):   
    def sql_connection_open(self, database):
        try:
            if database == "stock":
                acc = self.SQL_STOCK_ACC
            elif database == "dwh_prod":
                acc = self.SQL_DWH_ACC
            elif database == "datahub":
                acc = self.SQL_HUB_ACC
            elif database == "wms":
                acc = self.SQL_WMS_ACC

            connection = psycopg2.connect(user = acc['USER'],
                                          password = acc['PASSWORD'],
                                          host = acc['HOST'],
                                          port = acc['PORT'],
                                          database = acc['DATABASE'])
            return connection
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        

    def sql_select(self, connection, select_request):
        try:
            close = False
            db_list = ['stock', 'dwh_prod', 'datahub', 'wms']
            
            if connection in db_list:
                connection = self.sql_connection_open(database=connection)
                close = True
            
            with connection.cursor() as cursor:
                    cursor.execute(select_request)
                    res = cursor.fetchall()
                    colnames = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(data=res, columns=colnames)
            return df 

        except (Exception, Error) as error:  
            return  error
        finally:
            try:
                if close is True:
                    if connection:
                        connection.close()
            except:
                pass


    def sql_select_one(self, connection, select_request):
        try:
            close = False
            db_list = ['stock', 'dwh_prod', 'datahub', 'wms']
            
            if connection in db_list:
                connection = self.sql_connection_open(database=connection)
                close = True
            
            with connection.cursor() as cursor:
                    cursor.execute(select_request)
                    res = cursor.fetchone()
            return res

        except (Exception, Error) as error:  
            return  error
        finally:
            try:
                if close is True:
                    if connection:
                        connection.close()
            except:
                pass
    
    
    def sql_insert(self, connection, insert_request, insert_list):
        try:
            close = False
            db_list = ['stock', 'dwh_prod', 'datahub']
            
            if connection in db_list:
                connection = self.sql_connection_open(database=connection)
                close = True

            with connection.cursor() as cursor:
                cursor.execute(insert_request, insert_list)
                connection.commit()
                t = cursor.fetchone()
                return t
        except (Exception, Error) as error: 
            return  error
        
        finally:
            try:
                if close is True:
                    if connection:
                        connection.close()
            except:
                pass


    def sql_execute_df(self, connection, df, table):
        try:
            close = False
            db_list = ['stock', 'dwh_prod', 'datahub', 'wms']
            
            if connection in db_list:
                connection = self.sql_connection_open(database=connection)
                close = True
            

            tuples = [tuple(x) for x in df.to_numpy()]
            query  = "INSERT INTO %s VALUES %%s" % (table)
            cursor = connection.cursor()
            try:
                extras.execute_values(cursor, query, tuples)
                connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                connection.rollback()
                cursor.close()
                return 1
            print("execute_df done")
            cursor.close()
        except (Exception, Error) as error:
            return  error
        
        finally:
            try:
                if close is True:
                    if connection:
                        connection.close()
            except:
                pass       


    def sql_delete(self, connection, delete_request):
        try:
            close = False
            db_list = ['stock', 'dwh_prod', 'datahub' , 'wms']
            
            if connection in db_list:
                connection = self.sql_connection_open(database=connection)
                close = True

            with connection.cursor() as cursor:
                cursor.execute(delete_request)
                connection.commit()        

        except (Exception, Error) as error:
            print(error) 
            return  error
        finally:
            try:
                if close is True:
                    if connection:
                        connection.close()
            except:
                pass
        


class LWH_requests(Config):
    def lwh_api(self, api, sku):
        respons = requests.get(api + sku)   
        lwh_list = respons.json()

        stock = (lwh_list[0]["stockQuantity"])
        reserve = (lwh_list[0]["reservedQuantity"])
        limit = (lwh_list[0]["availableLimit"])
        available = stock - reserve
                
        return  stock , reserve , available, limit
    


    def lwh_api_all(self, api, sku):
        respons = requests.get(api + sku)   
        lwh_list = respons.json()

        return  lwh_list



    def lwh_session(self):
        user = fake_useragent.UserAgent().chrome
        header = {
                'user-agent': user    
                }
        
        ses = requests.Session()
        ses.post(self.LWH_URL['AUTH'], data=self.LWH_LOGIN_PASS , headers= header)
        return ses 
    


    def lwh_stock(self, ses, lwh_url):
        info = ses.get(lwh_url)
        soup = BS(info.text, 'lxml')
        stock_raw = soup.find('table', class_='table table-striped table-hover')
        stock_dictionary = []
        

        for i in stock_raw.find_all("tr"):
            title = i.text
            title = title.split('\n')
            stock_dictionary.append(
                 { 
                      'Name'     : title[1],
                      'sku'      : title[2],
                      'stock'    : title[3],
                      'reserv'   : title[4],
                      'available': title[5]
                 }
              )
            
        lwh = pd.DataFrame(stock_dictionary)
        lwh = lwh.iloc[1:]
        return lwh



    def lwh_limit(self, limit_url):
        ses = self.session()
        limit_info = ses.get(limit_url)
        soup_limit = BS(limit_info.text, 'lxml')
        limit_raw = soup_limit.find('table', class_='table table-striped table-hover')
        limit_dictionary = []
        
        for l in limit_raw.find_all("tr"):
           title = l.text
           title = title.split('\n')
           limit_dictionary.append(
                 { 
                      'Name'     : title[1],
                      'sku'      : title[2],
                      'limit'    : title[3]
                 }
              )
        
        limit = pd.DataFrame(limit_dictionary)
        limit = limit.iloc[1:]
        return limit



    def lwh_stock_limit(self, ses, lwh_url, limit_url):
        info = ses.get(lwh_url)
        soup = BS(info.text, 'lxml')
        stock_raw = soup.find('table', class_='table table-striped table-hover')
        
        limit_info = ses.get(limit_url)
        soup_limit = BS(limit_info.text, 'lxml')
        limit_raw = soup_limit.find('table', class_='table table-striped table-hover') 

        stock_dictionary = []
        limit_dictionary = []
        
        for i in stock_raw.find_all("tr"):
           title = i.text
           title = title.split('\n')
           stock_dictionary.append(
                 { 
                      'Name'     : title[1],
                      'sku'      : title[2],
                      'stock'    : title[3],
                      'reserv'   : title[4],
                      'available': title[5]
                 }
              )
        
        
        for l in limit_raw.find_all("tr"):
           title = l.text
           title = title.split('\n')
           limit_dictionary.append(
                 { 
                      'Name'     : title[1],
                      'sku'      : title[2],
                      'limit'    : title[3]
                 }
              )
    
        
        lwh = pd.DataFrame(stock_dictionary)
        limit = pd.DataFrame(limit_dictionary)
        stock = lwh.merge(limit, left_on = 'sku', right_on = 'sku', suffixes=('', 'right'))
        stock = stock.drop('Nameright', axis = 1)
        stock = stock.iloc[1:]
        return stock



    def lwh_request(self, city = False, market_hall = False, kenmart = False, alash = False, gallery = False, express = False, bekzhan = False):
        ses = self.lwh_session()

        if city == True:
            city_lwh = self.lwh_stock_limit(ses = ses, lwh_url= self.lwh_stock_url(5001), limit_url=self.lwh_limit_url(5001))
        else :
            city_lwh = None
        
        if market_hall == True:
            market_hall_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(5002), limit_url=self.lwh_limit_url(5002))
        else :
            market_hall_lwh = None
        
        if kenmart == True:
            kenmart_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(6001) , limit_url=self.lwh_limit_url(6001))
        else :
            kenmart_lwh = None
        
        if express == True:
            express_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(5101), limit_url=self.lwh_limit_url(5101))
        else:
            express_lwh = None
    
        if gallery == True:
            gallery_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(5000), limit_url=self.lwh_limit_url(5000))
        else :
            gallery_lwh = None

        if alash == True:
            alash_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(6002), limit_url=self.lwh_limit_url(6002))
        else :
            alash_lwh = None

        if bekzhan == True:
            bekzhan_lwh = self.lwh_stock_limit(ses = ses, lwh_url=self.lwh_stock_url(6002), limit_url=self.lwh_limit_url(6002))
        else :
            bekzhan_lwh = None
        

        return city_lwh, market_hall_lwh, kenmart_lwh, alash_lwh, gallery_lwh, express_lwh
    


    def lwh_set_limit(self, store_id, text):
        try:
            ses = self.lwh_session()
            tact = ("lim-%s.txt" %(store_id))
            f=open(tact,'w+')
            f.write(text)
            f.close()
            files = {'file': open(tact, 'rb')}
            
            limit = ses.post(self.LWH_URL['SET_LIMIT'],files = files,data = {'storeId':store_id})
            
            soup_limit = BS(limit.text, 'lxml')
            limit_raw = soup_limit.find('table', class_='table table-striped table-hover')
            limit_dictionary = []
            

            for l in limit_raw.find_all("tr"):
               title = l.text
               title = title.split('\n')
               limit_dictionary.append(
                     { 
                          'Name'     : title[1],
                          'sku'      : title[2],
                          'limit'    : title[3]
                     }
                  )
            
            limit = pd.DataFrame(limit_dictionary)
            limit = limit.iloc[1:]

            return limit
        
        except:
            ans = "False"



class Bakudo():
    def bakudo_is_digit(self, n):
        try:
            int(n)
            return True
        except ValueError:
            try:
                float(n)
                return True
            except ValueError:    
                return  False


    def bakudo_cyrillic(self, text):
        pattern = re.compile('[а-яА-ЯЁё]')
        return bool(pattern.search(text)) 



class Kai(SQL_requests):
    def kai_flow(self, connection):
        count_req = ("""select distinct counter from target_counter tc 
                        order by counter desc limit 1""")
        result = self.sql_select_one(connection = connection, select_request = count_req)
        return result[0]


    def kai_insert_limit(self, connection, sku, user_id, msg_date, limit, new_limit, stock, reserve, cause, STORE_ID, message_id, flow, chat_id):
        insert_req = ("""insert into change_limit(sku, user_id, date_time, old_limit, new_limit, stock, reserve, cause, store_id, message_id, flow, chat_id) 
                             values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
        
        insert_list = (sku, user_id, msg_date, limit, new_limit, stock, reserve, cause, STORE_ID, message_id, flow, chat_id)
        insert =  self.sql_insert(connection=connection, insert_request=insert_req, insert_list=insert_list)
        return insert


    def kai_insert_limit_ri(self, connection, sku, user_id, msg_date, limit, new_limit, stock, reserve, cause, STORE_ID, message_id, flow, chat_id):
        insert_req = ("""insert into change_limit(sku, user_id, date_time, old_limit, new_limit, stock, reserve, cause, store_id, message_id, flow, chat_id) 
                             values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                             returning id""")
        
        insert_list = (sku, user_id, msg_date, limit, new_limit, stock, reserve, cause, STORE_ID, message_id, flow, chat_id)
        insert =  self.sql_insert(connection=connection, insert_request=insert_req, insert_list=insert_list)
        return insert[0]



class Hado(Config):
    def hado_sharepoint_open(self, url_path, sheet_name):
        site_url = url_path.split('teams/')[0] + 'teams/' + url_path.split('teams/')[1].split('/')[0]
        file_url = '/teams' + url_path.split('/teams')[1]
        
        ctx = ClientContext(site_url).with_credentials(UserCredential(self.MICROSOFT_ACC['USERNAME'], self.MICROSOFT_ACC['PASSWORD']))        

        response = File.open_binary(ctx, file_url)
        df = pd.read_excel(io.BytesIO(response.content), sheet_name=sheet_name)
        return df



    def hado_sharepoint_save(self, url_path, file_name, file_path):
        site_url = url_path.split('teams/')[0] + 'teams/' + url_path.split('teams/')[1].split('/')[0]
        file_url = '/teams' + url_path.split('/teams')[1]

        ctx = ClientContext(site_url).with_credentials(UserCredential(self.MICROSOFT_ACC['USERNAME'], self.MICROSOFT_ACC['PASSWORD']))
        target_folder = ctx.web.get_folder_by_server_relative_url(file_url)
        
        with open(file_path + file_name, 'rb') as content_file:
            file_content = content_file.read()
            target_folder.upload_file(file_name, file_content).execute_query()



    def hado_send_email(self, subject, message, to_addresses, file_paths, file_names):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.EMAIL_ACC['USERNAME']
        msg['To'] = ", ".join(to_addresses)

        html_content = MIMEText(message, 'html')
        msg.attach(html_content)


        for file_path, file_name in zip(file_paths, file_names):
            binary_file = open(file_path, 'rb')
            mime_part = MIMEBase('application', 'octet-stream')
            mime_part.set_payload(binary_file.read())
            encoders.encode_base64(mime_part)


            mime_part.add_header('Content-Disposition', 'attachment; filename="%s"' % file_name)

            msg.attach(mime_part)


        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(self.EMAIL_ACC['USERNAME'], self.EMAIL_ACC['PASSWORD'])
        server.sendmail(self.EMAIL_ACC['USERNAME'], to_addresses, msg.as_string())
        server.quit()



    def hado_send_email_lite(self, subject, message, to_addresses):
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.EMAIL_ACC['USERNAME']
        msg['To'] = ", ".join(to_addresses)

        # Create the message (HTML).
        html_content = MIMEText(message, 'html')
        # Attach parts into message container
        msg.attach(html_content)

        # Sending the email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(self.EMAIL_ACC['USERNAME'], self.EMAIL_ACC['PASSWORD'])
        server.sendmail(self.EMAIL_ACC['USERNAME'], to_addresses, msg.as_string())
        server.quit()
