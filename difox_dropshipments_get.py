# voorbereidingen met rechten en links etc om bestanden van ftp server voor difox af te kunnen halen, om ze dan te verwerken:
# /home/dongen/odin_website/dropshipment
# /home/toopbv/domains/toop.nl/public_html/ftpdifox/
# sudo ln -s /home/toopbv/domains/toop.nl/public_html/ftpdifox/ /home/dongen/odin_website/dropshipment/
# sudo setfacl -R -m u:dongen:rwx /home/toopbv/domains/toop.nl/public_html/ftpdifox/
# usermod -a -G toopbv dongen
# sudo chmod g+w /home/toopbv/domains/toop.nl/public_html/ftpdifox/
# /usr/bin/rsync -a /home/toopbv/domains/toop.nl/public_html/ftpdifox/ /home/dongen/odin_website/dropshipment/
# sudo cp -R Source_Folder Destination_Folder
# setfacl -m g:dongen:rwx /home/toopbv/domains/toop.nl/public_html/ftpdifox/
# ls -ls /home/toopbv/domains/toop.nl/public_html/ftpdifox/

import configparser
import datetime
import mysql.connector
import pandas as pd
import logging
import shutil
from pathlib import Path

config = configparser.ConfigParser()

try:
    config.read_file(open(Path.home() / "Dropbox" / "MACRO" / "bol_export_files.ini"))
except FileNotFoundError as e:
    config.read(Path.home() / "bol_export_files.ini")

connection_server = mysql.connector.connect(**config["database odin"])
cursor = connection_server.cursor()


logger = logging.getLogger("process_difox")
logging.basicConfig(filename="process_difox_" + datetime.date.today().strftime("%V") + ".log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")#nieuwe log elke week

invoices = [p for p in Path('/home/webshops/domains/toopbv.nl/public_html/ftpdifox/invoices/').iterdir() if p.is_file() and p.suffix == '.csv']

def check_local_drop(order_nr):
    get_current_drop_number = "SELECT dropship FROM orders_info_bol WHERE orderid = %s"
    cursor.execute(get_current_drop_number, (order_nr,))
    try:
        number = cursor.fetchone()[0]
        return int(number)
    except:
        return 0

for data_file in invoices:
    invoice = pd.read_csv(data_file, delimiter=";", encoding='latin-1', header=None)
    try:
        orderid_bron = invoice.iloc[0][2].split(":")[1]
        count = orderid_bron.count("-")
        if count == 0: #bol
            orderid = orderid_bron[1:]
            order_id_leverancier = str(invoice.iloc[0][1])
            drop_num = check_local_drop(orderid)
            if drop_num < 5:
                mySql_insert_query = "UPDATE orders_info_bol SET dropship = 2, verkooporder_id_leverancier = %s WHERE orderid = %s "
                cursor.execute(mySql_insert_query, (order_id_leverancier,orderid))
                connection_server.commit()
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "log"
                logger.info(f"processed stap 2 {file_name}")
            else:
                logger.info(f"local dropship {orderid} step 2 done")
        if count == 1: #blokker
            orderid = orderid_bron[1:]
            order_id_leverancier = str(invoice.iloc[0][1])
            drop_num = check_local_drop(orderid)
            if drop_num < 5:
                mySql_insert_query = "UPDATE blokker_order_items SET dropship = 2, verkooporder_id_leverancier = %s WHERE orderid = %s "
                cursor.execute(mySql_insert_query, (order_id_leverancier,orderid))
                connection_server.commit()
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "log"
                logger.info(f"processed stap 2 {file_name}")
            else:
                logger.info(f"local dropship {orderid} step 2 done")
        if count == 2: #conrad
            orderid = orderid_bron[1:]
            order_id_leverancier = str(invoice.iloc[0][1])
            drop_num = check_local_drop(orderid)
            if drop_num < 5:
                mySql_insert_query = "UPDATE conrad_order_items SET dropship = 2, verkooporder_id_leverancier = %s WHERE orderid = %s "
                cursor.execute(mySql_insert_query, (order_id_leverancier,orderid))
                connection_server.commit()
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "log"
                logger.info(f"processed stap 2 {file_name}")
            else:
                logger.info(f"local dropship {orderid} step 2 done")
        else:
            file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
            logger.info(f"Failed to convert, because not dropship {file_name}")
    except AttributeError as error:
        file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
        logger.info(f"Failed to convert becourse number missing {error,file_name}") #normal, only dropshipments have number
    except mysql.connector.Error as error:
        logger.error(f"Failed to update {error}")
        file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
    shutil.move(path / file_name, new_path / file_name) 

backorder = [p for p in Path('/home/webshops/domains/toopbv.nl/public_html/ftpdifox/ordrsp/').iterdir() if p.is_file() and p.suffix == '.csv']

for data_file in backorder:
    invoice = pd.read_csv(data_file, delimiter=";", encoding='latin-1', dtype=object, header=None)
    order = invoice.iloc[0][2]
    if not pd.isna(order):
        if order.startswith("BestelNr"): # only process when dropshipment.
            try:
                orderid = order.split(":")[1][1:]
                order_id_leverancier = invoice.iloc[0][0]
                t_t_dropshipment = "Backorder"
                drop_num = check_local_drop(orderid)
                if drop_num < 5:
                    mySql_insert_query = "UPDATE orders_info_bol SET dropship = 3, t_t_dropshipment = %s,order_id_leverancier = %s WHERE orderid = %s "
                    cursor.execute(mySql_insert_query, (t_t_dropshipment,order_id_leverancier,orderid))
                    connection_server.commit()
                    file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "log"
                    logger.info(f"processed stap 3 {file_name}")
                else:
                    logger.info(f"local dropship {orderid} step 3 done")
            except mysql.connector.Error as error:
                logger.error(f"Failed to update {error}")
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
            shutil.move(path / file_name, new_path / file_name)
    else:
        file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
        logger.error(f"Geen dropshipment {file_name}")
        shutil.move(path / file_name, new_path / file_name)

verzendingen = [p for p in Path('/home/webshops/domains/toopbv.nl/public_html/ftpdifox/desadv/').iterdir() if p.is_file() and p.suffix == '.csv']

for data_file in verzendingen:
    invoice = pd.read_csv(data_file, delimiter=";", encoding='latin-1', dtype=object, header=None)
    order = invoice.iloc[0][2]
    if not pd.isna(order):
        if order.startswith("BestelNr"): # only process when dropshipment.
            try:
                orderid = order.split(":")[1][1:]
                order_id_leverancier = invoice.iloc[0][0]
                t_t_dropshipment = invoice.iloc[0][53]
                if len(t_t_dropshipment) != 15:
                    t_t_dropshipment = "ongeldig"
                else:
                    t_t_dropshipment = f"https://extranet.dpd.de/status/nl_NL/parcel/{t_t_dropshipment}"
                drop_num = check_local_drop(orderid)
                if drop_num < 5:
                    mySql_insert_query = "UPDATE orders_info_bol SET dropship = 4, t_t_dropshipment = %s,order_id_leverancier = %s WHERE orderid = %s "
                    cursor.execute(mySql_insert_query, (t_t_dropshipment,order_id_leverancier,orderid))
                    connection_server.commit()
                    file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "log"
                    logger.info(f"processed stap 4 {file_name}")
                else:
                    logger.info(f"local dropship {orderid} step 4 done")
            except AttributeError as error:
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
                logger.info(f"Failed to convert becourse number missing {error,file_name}") #normal, only dropshipments have number
            except mysql.connector.Error as error:
                logger.error(f"Failed to update {error}")
                file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
            shutil.move(path / file_name, new_path / file_name)
        else:
            file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
            logger.error(f"Geen dropshipment {file_name}")
            shutil.move(path / file_name, new_path / file_name)  
    else:
        file_name,path,new_path =  data_file.name,data_file.parent.resolve(),data_file.parent.resolve()/ "err"
        logger.error(f"Geen dropshipment {file_name}")
        shutil.move(path / file_name, new_path / file_name)

cursor.close()
connection_server.close()
