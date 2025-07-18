import base64
import configparser
import datetime
import io
import json
import logging
import mimetypes
import os
import pickle
import re
import subprocess
import sys
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from ftplib import FTP
import time
import httpx
import lxml.etree as et
from MySQLdb import OperationalError # pip install mysqlclient op windows
from dataclasses import dataclass

def install(package):
    subprocess.call([sys.executable, "-m", "pip", "install", package])


try:
    from google.auth.transport.requests import Request
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("google-auth")
    from google.auth.transport.requests import Request

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("google-api-python-client")
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("google-auth-oauthlib")
    from google_auth_oauthlib.flow import InstalledAppFlow

try:
    from lxml import etree
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("lxml")
    from lxml import etree
try:
    from sqlalchemy import MetaData, Table, and_, create_engine, select, text, update
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("sqlalchemy")
    from sqlalchemy import MetaData, Table, create_engine, select, update, text, and_

from sqlalchemy import MetaData, Table, and_, create_engine, select, text, update
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError


sys.path.insert(0, str(Path.cwd().parent))
from bol_export_file import get_file

config = configparser.ConfigParser(interpolation=None)

try:
    config.read_file(open(Path.home() / "Dropbox" / "MACRO" / "bol_export_files.ini"))
except FileNotFoundError as e:
    config.read(Path.home() / "bol_export_files.ini")

url = URL.create(**config["database odin alchemy"])
try:
    engine = create_engine(url)
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("pymysql")
    engine = create_engine(url)
try:
    conn = engine.connect()
except OperationalError as e:
    print(e)
metadata = MetaData()

logger = logging.getLogger("process_gmail")
logging.basicConfig(
    filename="process_gmail_" + datetime.date.today().strftime("%V") + ".log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)  # nieuwe log elke week
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

aantal_dagen = "15d"

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.modify"]


def get_autorisation_gooogle_api():
    # not renewing need to run on desktop, and then copy token picle if not working.
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if Path("token.pickle").is_file():
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials_gmail.json", SCOPES)
            authorization_url, state = flow.authorization_url(
                access_type="offline", login_hint="toopbv@gmail.com", include_granted_scopes="true"
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return creds


def gmail_create_connection(login):
    return build("gmail", "v1", credentials=login)


def set_order_info_db_bol(order_info, track_en_trace_url, track_en_trace_num):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
    logger.info(f"start stap 3 bol {order_info}")
    if order_info:
        try:
            order_id = order_info['orderid']
            order_orderitemid = order_info['order_orderitemid']
        except TypeError:
            order_id = order_info[0]
            order_orderitemid = order_info[1]
        drop_send = (
            update(orders_info_bol)
            .where(
                and_(
                    orders_info_bol.columns.orderid == order_id,
                    orders_info_bol.columns.order_orderitemid == order_orderitemid,
                )
            )
            .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
        )
        with engine.begin() as conn:
            conn.execute(drop_send)
    else:
        logger.info(f"start stap 3 bol failed no order_info {track_en_trace_url} ")


def set_order_info_db_blokker(order_info, track_en_trace_url, track_en_trace_num):
    orders_info_blokker = Table("blokker_order_items", metadata, autoload_with=engine)
    logger.info(f"start stap 3 blokker {order_info}")
    drop_send = (
        update(orders_info_blokker)
        .where(orders_info_blokker.columns.order_line_id == order_info[0])
        .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
    )
    with engine.begin() as conn:
        conn.execute(drop_send)

def set_mailsend_db_bol(order_info):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
    logger.info(f"info {order_info} mail send ")
    mail_send = (
        update(orders_info_bol)
        .where(orders_info_bol.columns.orderid == order_info)
        .values(extra_info_mail_verzonden=True)
    )
    with engine.begin() as conn:
        conn.execute(mail_send)

def set_replay_mailsend_db_bol(order_info):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
    logger.info(f"info {order_info} mail reply is received ")
    mail_send = (
        update(orders_info_bol)
        .where(orders_info_bol.columns.orderid == order_info)
        .values(extra_info_replay_mail_ontvangen=True)
    )
    with engine.begin() as conn:
        conn.execute(mail_send)

def get_body_email(mess):
    try:
        body = base64.urlsafe_b64decode(mess["payload"]["body"]["data"].encode("UTF8"))
    except KeyError:
        try:
            body = base64.urlsafe_b64decode(mess["payload"]["parts"][1]["body"]["data"].encode("UTF8"))
        except (KeyError, IndexError):
            try:
                body = base64.urlsafe_b64decode(mess["payload"]["parts"][0]["body"]["data"].encode("UTF8"))
            except (KeyError, IndexError) as e:
                try:
                    body = base64.urlsafe_b64decode(
                        mess["payload"]["parts"][0]["parts"][00]["body"]["data"].encode("UTF8")
                    )
                except (KeyError, IndexError) as e:
                    logger.error(f"processing body {mess} failed {e}")
    except Exception as e:
        logger.error(f"processing body {mess} failed {e}")
    if body:
        # print(body)
        return etree.parse(io.BytesIO(body), etree.HTMLParser())


def get_info_db(querys):
    with engine.connect() as connection:
        for query in querys:
            try:
                info = connection.exec_driver_sql(query[0], query[1]).first()
                if info:
                    return info, query[2]
            except Exception as e:
                logger.error(f"stap 3 failed {query[0],query[1]} {e}")
        else:
            return False, False


def set_order_info_in_db(order_infos, info_mail, winkel):
    if isinstance(order_infos, list):
        for order_info in order_infos:
            if (order_info[-1] is None or int(order_info[-1]) < 3) and winkel == "bol":
                set_order_info_db_bol(order_info, info_mail["tt_url"], info_mail["tt_num"])
                return True
            elif (order_info[-1] is None or int(order_info[-1]) < 3) and winkel == "blok":
                set_order_info_db_blokker(order_info, info_mail["tt_url"], info_mail["tt_num"])
                return True
            elif order_info[-1] == 3:
                logger.info(f'stap 3 {info_mail["dienst"]} {info_mail}{order_info} order already processed')
                return True
            else:
                return False
    else:
        if (order_infos[-1] is None or int(order_infos[-1]) < 3) and winkel == "bol":
            set_order_info_db_bol(order_infos, info_mail["tt_url"], info_mail["tt_num"])
        elif (order_infos[-1] is None or int(order_infos[-1]) < 3) and winkel == "blok":
            set_order_info_db_blokker(order_infos, info_mail["tt_url"], info_mail["tt_num"])
        elif order_infos[-1] == 3:
            logger.info(f'stap 3 {info_mail["dienst"]} {info_mail} {order_infos} order already processed')
        else:
            return False
        return True


def get_messages(conn, search_query, gewenste_aantal_dagen=aantal_dagen):
    return (
        conn.users()
        .messages()
        .list(
            userId="me",
            q=f"{search_query} newer_than:{gewenste_aantal_dagen} -label:verzending-verwerkt_odin AND -label:odin_verwerkt",
        )
        .execute()
        .get("messages", [])
    )


def get_set_info_database(info_mail):
    logger.info(f"begin processing {info_mail}")
    # first build all posible querys, from all mails for both stores, then check them and set value if match
    info_bol_db = "SELECT I.orderid,I.order_orderitemid,O.shipmentdetails_streetname,O.shipmentdetails_city,I.dropship FROM orders_info_bol I LEFT JOIN orders_bol O ON I.orderid = O.orderid "
    info_bol_db_end = "AND I.created_on_artikel > DATE_ADD(NOW(), INTERVAL -1 MONTH) ORDER BY O.updated_on DESC"
    info_blok_db = "SELECT I.order_line_id,O.shipping_address_zip_code,O.shipping_address_city,I.dropship FROM blokker_orders O LEFT JOIN blokker_order_items I ON O.commercialid = I.commercialid "
    info_blok_db_end = "AND O.created_date > DATE_ADD(NOW(), INTERVAL -2 MONTH) ORDER BY O.created_date DESC"
    list_querys = []
    if "order_num" in info_mail:
        if (len(info_mail["order_num"]) > 9) and (len(info_mail["order_num"]) < 16):
            if "postcode" in info_mail:
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE I.orderid LIKE %s AND O.shipmentdetails_zipcode = %s {info_bol_db_end}",
                        (info_mail["order_num"] + "%%", info_mail["postcode"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE I.order_line_id LIKE %s AND REPLACE(O.shipping_address_zip_code,' ','') = %s {info_blok_db_end}",
                        (info_mail["order_num"] + "%%", info_mail["postcode"]),
                        "blok",
                    )
                )
            else:
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE I.orderid LIKE %s AND O.shipmentdetails_city = %s {info_bol_db_end}",
                        (info_mail["order_num"] + "%%", info_mail["city"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE I.order_line_id LIKE %s AND O.shipping_address_city = %s {info_blok_db_end}",
                        (info_mail["order_num"] + "%%", info_mail["city"]),
                        "blok",
                    )
                )
        else:
            if "postcode" in info_mail:
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_city LIKE %s AND O.shipmentdetails_housenumber = %s {info_bol_db_end}",
                        (info_mail["postcode"], info_mail["city"] + "%%", info_mail["house_number"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["city"] + "%%", info_mail["house_number"]),
                        "blok",
                    )
                )
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_firstname LIKE %s AND O.shipmentdetails_housenumber = %s {info_bol_db_end}",
                        (info_mail["postcode"], info_mail["first_name"] + "%%", info_mail["house_number"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_firstname = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["first_name"] + "%%", info_mail["house_number"]),
                        "blok",
                    )
                )
            else:
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE O.shipmentdetails_city LIKE %s AND O.shipmentdetails_housenumber = %s AND O.shipmentdetails_surname = %s {info_bol_db_end}",
                        (info_mail["city"] + "%%", info_mail["house_number"], info_mail["last_name"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_firstname = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["city"], info_mail["first_name"]),
                        "blok",
                    )
                )

    elif "postcode" in info_mail:
        if "leverancier" in info_mail:
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_city = %s AND O.shipmentdetails_firstname = %s AND (I.order_offerreference LIKE %s OR I.order_offerreference LIKE 'WIN%%') {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["first_name"], info_mail["leverancier"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_firstname = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["first_name"]),
                    "blok",
                )
            )
            if "house_number" in info_mail:
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_city = %s AND O.shipmentdetails_housenumber = %s AND (I.order_offerreference LIKE %s OR I.order_offerreference LIKE 'WIN%%') {info_bol_db_end}",
                        (info_mail["postcode"], info_mail["city"], info_mail["house_number"], info_mail["leverancier"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["city"], info_mail["house_number"]),
                        "blok",
                    )
                )
        if "city" in info_mail:
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_city = %s AND O.shipmentdetails_firstname = %s {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["first_name"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_city = %s AND O.shipmentdetails_housenumber = %s {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["house_number"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_firstname = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["first_name"]),
                    "blok",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["city"], info_mail["house_number"]),
                    "blok",
                )
            )
        else:
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_firstname = %s AND O.shipmentdetails_surname = %s {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["first_name"], info_mail["last_name"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_firstname = %s {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["first_name"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_surname = %s {info_bol_db_end}",
                    (info_mail["postcode"], info_mail["last_name"]),
                    "bol",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_firstname = %s AND O.shipping_address_lastname = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["first_name"], info_mail["last_name"]),
                    "blok",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_firstname = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["first_name"]),
                    "blok",
                )
            )
            list_querys.append(
                (
                    f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_lastname = %s {info_blok_db_end}",
                    (info_mail["postcode"], info_mail["last_name"]),
                    "blok",
                )
            )

    elif "street" in info_mail:
        list_querys.append(
            (
                f"{info_bol_db} WHERE O.shipmentdetails_streetname = %s AND O.shipmentdetails_city = %s AND O.shipmentdetails_firstname = %s {info_bol_db_end}",
                (info_mail["street"], info_mail["city"], info_mail["first_name"]),
                "bol",
            )
        )
        list_querys.append(
            (
                f"{info_bol_db} WHERE O.shipmentdetails_city = %s AND O.shipmentdetails_firstname = %s AND O.shipmentdetails_surname = %s {info_bol_db_end}",
                (info_mail["city"], info_mail["first_name"], info_mail["last_name"]),
                "bol",
            )
        )
        list_querys.append(
            (
                f"{info_blok_db} WHERE O.shipping_address_street_1 = %s AND O.shipping_address_city = %s AND O.shipping_address_firstname = %s {info_blok_db_end}",
                (info_mail["street"], info_mail["city"], info_mail["first_name"]),
                "blok",
            )
        )
        list_querys.append(
            (
                f"{info_blok_db} WHERE O.shipping_address_city = %s AND O.shipping_address_firstname = %s AND O.shipping_address_lastname = %s {info_blok_db_end}",
                (info_mail["city"], info_mail["first_name"], info_mail["last_name"]),
                "blok",
            )
        )

    result_info_from_db, shop = get_info_db(list_querys)

    if result_info_from_db:
        return set_order_info_in_db(result_info_from_db, info_mail, shop)


def dhl_info(info_mail):
    # dit omdat sommige dhl mails geen postcode hebben
    logger.info(f"begin processing_dhl {info_mail}")
    # first build all posible querys, from all mails for both stores, then check them and set value if match
    info_bol_db = (
        "SELECT O.shipmentdetails_zipcode FROM orders_info_bol I LEFT JOIN orders_bol O ON I.orderid = O.orderid "
    )
    info_bol_db_end = "AND I.created_on_artikel > DATE_ADD(NOW(), INTERVAL -15 DAY) ORDER BY O.updated_on DESC"
    info_blok_db = "SELECT O.shipping_address_zip_code FROM blokker_orders O LEFT JOIN blokker_order_items I ON O.commercialid = I.commercialid "
    info_blok_db_end = "AND O.created_date > DATE_ADD(NOW(), INTERVAL -20 DAY) ORDER BY O.created_date DESC"
    list_query_dbs = [
        (
            f"{info_bol_db} WHERE O.shipmentdetails_firstname = %s AND O.shipmentdetails_surname = %s {info_bol_db_end}",
            (info_mail["first_name_search"], info_mail["last_name_search"]),
            "bol",
        ),
        (
            f"{info_blok_db} WHERE O.shipping_address_firstname = %s AND O.shipping_address_lastname = %s {info_blok_db_end}",
            (info_mail["first_name_search"], info_mail["last_name_search"]),
            "blok",
        ),
        (
            f"{info_bol_db} WHERE O.shipmentdetails_firstname = %s {info_bol_db_end}",
            (info_mail["first_name_search"],),
            "bol",
        ),
    ]
    result_info_from_db, _ = get_info_db(list_query_dbs)
    if result_info_from_db:
        return result_info_from_db


def mark_read(conn, message):
    conn.users().messages().modify(
        userId="me", id=message["id"], body={"removeLabelIds": ["UNREAD"]}
    ).execute()  # mark mail read
    conn.users().messages().modify(userId="me", id=message["id"], body={"removeLabelIds": ["INBOX"]}).execute()


def add_label_processed_verzending(conn, message):
    conn.users().messages().modify(
        userId="me", id=message["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
    ).execute()


def add_label_processed_return(conn, message):
    conn.users().messages().modify(
        userId="me", id=message["id"], body={"addLabelIds": ["Label_1372612835680541088"]}
    ).execute()


def process_bpost_messages(conn):
    # get send info exellent.
    message_treads_ids = get_messages(
        conn,
        'from:(noreply@bpost.be) subject:("LANCKRIET" AROUND 2 "bpost") OR subject:("leveren" AROUND 3 "LANCKRIET") OR subject:("parcel" AROUND 2 deliver)',
    )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "bpost",
                "tt_url": mail_body.xpath(
                    "//tr//a[contains(text(),'Details pakje')]/@href | //tr//*[contains(text(),'Details pakje')]//../@href | //tr//a[contains(text(),'Parcel details')]/@href | //tr//a[contains(text(),' colis')]/@href | //tr//a[contains(text(),'Pakje volgen')]/@href | //tr//a[contains(text(),'Track Parcel')]/@href"
                )[0],
                "tt_num": mail_body.xpath(
                    "//p[contains(text(),'Barcode')] | //p[contains(text(),'Code-barres')] | //p/span[contains(text(),'Barcode')]"
                )[0].text.split(" ")[-1],
            }
            *_, mail_info["city"] = [
                x.strip().split(" ")[0]
                for x in re.split(
                    ",", mail_body.xpath("//td[contains(@class,'destination')]/div/p/strong")[0].text.lower()
                )
            ]
            post_id = mail_info["tt_url"].split("itemCode=")[1].split("&")[0]
            bpost_api_info = httpx.get(f"https://track.bpost.cloud/track/items?itemIdentifier={post_id}").json()[
                "items"
            ][0]
            mail_info["first_name"], *mail_info["last_name"] = bpost_api_info["receiver"]["name"].lower().split()
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = bpost_api_info["receiver"]["streetName"].lower()
            mail_info["house_number"] = bpost_api_info["receiver"]["streetNumber"].lower()
            mail_info["postcode"] = bpost_api_info["receiver"]["postcode"].lower()
        except Exception as e:
            logger.error(f"stap 3 bpost error {e}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            get_order_info_db = get_set_info_database(mail_info)
            if not get_order_info_db:
                del mail_info["city"]  # omdat city wel eens wil afwijken, vreemde belgen...
                get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 bpost {mail_info} failed {e}")
    # delivered/afhaalpunt pakketten
    message_treads_ids = get_messages(
        conn, 'from:(noreply@bpost.be) subject:("Je pakje " pakjesautomaat|Afhaalpunt|geleverd)'
    )
    for message_treads_id in message_treads_ids:
        mark_read(conn, message_treads_id)
        add_label_processed_verzending(conn, message_treads_id)


def process_dhl_messages(conn):
    # get send info Lanckriet/Exellent nederland.
    message_treads_ids = get_messages(
        conn,
        'from:(noreply@dhlparcel.nl) subject:("We staan" AROUND 1 "voor de deur")  OR subject:("komen we bij je langs")',
    )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dhl",
                "tt_url": mail_body.xpath(
                    "//tbody//td//p[contains(text(),'pakket')]/..//a[string-length( text()) = 15]/@href | //tbody//td//p[contains(text(),'bezorger')]/..//a[string-length(text()) = 13]/@href"
                )[0],
                "tt_num": mail_body.xpath(
                    "//tbody//td//p[contains(text(),'pakket')]/..//a[string-length( text()) = 15] | //tbody//td//p[contains(text(),'bezorger')]/..//a[string-length(text()) = 13]"
                )[0].text,
            }
        except IndexError:
            logger.error("stap 3 dhl uitgevoerd voor een van de pakketten die voor ons zijn")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            add_label_processed_verzending(conn, message_treads_id)
            continue
        try:  # addres not in mail (and postcode also missing in first), and needs javascript..
            link_info_encoded = mail_info["tt_url"].split("/")[-1].encode("ascii")
            input_len = len(link_info_encoded)
            padding = b"=" * (3 - ((input_len + 3) % 4))
            plain_url = base64.b64decode(link_info_encoded + padding, altchars=b"-_")
            if "sorted" in str(plain_url):
                _, _, mail_info["first_name_search"], *mail_info["last_name_search"], _ = [
                    x.strip(",").strip()
                    for x in re.split(" ", mail_body.xpath("//td//p[contains(text(),'Beste')]/text()")[0].lower())
                ]
                if mail_info["first_name_search"] in (
                    "ten",
                    "de",
                    "het",
                    "van",
                    "van den",
                    "van der",
                    "van het",
                ):  # sommige doen het net omgedraaid, daarom checken, of tussenvoegsel waarschijndelijk is
                    _, _, *mail_info["last_name_search"], mail_info["first_name_search"], _ = [
                        x.strip(",").strip()
                        for x in re.split(" ", mail_body.xpath("//td//p[contains(text(),'Beste')]/text()")[0].lower())
                    ]
                mail_info["last_name_search"] = " ".join(filter(None, mail_info["last_name_search"]))
                if "video van gils b.v." in mail_info["last_name_search"]:
                    add_label_processed_verzending(conn, message_treads_id)
                    continue
                try:
                    postal_code = dhl_info(mail_info)[0]
                except TypeError:
                    conn.users().messages().modify(
                        userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
                    ).execute()
                    continue
                trace_nr = str(plain_url).split("?")[0].split("/")[-1]
            else:
                if "pc=4823AB" in str(plain_url):
                    add_label_processed_verzending(conn, message_treads_id)
                    logger.info(f"stap 3 dhl pakketje voor ons")
                    continue
                else:
                    try:
                        trace_nr, postal_code = (
                            str(plain_url).split("/")[5],
                            str(plain_url).split("?")[0].split("/")[-1],
                        )
                    except IndexError:
                        trace_nr = re.search(r"tc=([A-Z0-9]+)", str(plain_url)).group(1)
                        logger.info(
                            f"stap 3 dhl geen postcode {trace_nr}"
                        )  # geen betrouwbare manier om postcode te achterhalen voor zover ik kan zien
                        mark_read(conn, message_treads_id)
                        add_label_processed_verzending(conn, message_treads_id)
                        continue
            dhl_api_info = httpx.get(
                f"https://api-gw.dhlparcel.nl/track-trace?key={trace_nr}%2B{postal_code}"
            ).json()[0]
            mail_info["first_name"], *mail_info["last_name"] = dhl_api_info["receiver"]["name"].split()
            if mail_info["first_name"].lower() in (
                "ten",
                "de",
                "het",
                "van",
                "van den",
                "van der",
                "van het",
            ):  # sommige doen het net omgedraaid, daarom checken, of tussenvoegsel waarschijndelijk is
                *mail_info["last_name"], mail_info["first_name"] = dhl_api_info["receiver"]["name"].split()
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = dhl_api_info["receiver"]["address"]["street"]
            mail_info["house_number"] = dhl_api_info["receiver"]["address"]["houseNumber"]
            mail_info["postcode"] = dhl_api_info["receiver"]["address"]["postalCode"]
            mail_info["city"] = dhl_api_info["receiver"]["address"]["city"]
            if mail_body.xpath("//td/p[contains(text()[2],'LANCKRIET')]"):
                mail_info["leverancier"] = "EXL%"
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dhl {mail_info} failed {e}")
    # delivered/afhaalpunt pakketten
    message_treads_ids = get_messages(conn, 'from:(noreply@dhlparcel.nl) subject:("Je pakket")')
    for message_treads_id in message_treads_ids:
        mark_read(conn, message_treads_id)
        add_label_processed_verzending(conn, message_treads_id)


def process_dynalogic_messages(conn):
    # get send info Exellent nederland.
    message_treads_ids = get_messages(
        conn, 'from:(noreply@dynalogic.eu) subject:("Wij komen eraan" OR "Afspraakbevestiging")'
    )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dynalogic",
                "tt_url": mail_body.xpath(
                    "//td//span[contains(text(),'Track & Trace')]/../@href | //td//span[contains(text(),'Mijn Afspraak')]/../@href"
                )[0],
                "tt_num": mail_body.xpath(
                    "//td//span[contains(text(),'Track & Trace')]/../@href | //td//span[contains(text(),'Mijn Afspraak')]/../@href"
                )[0]
                .split("=")[-2]
                .split("&")[0]
                .replace("+", " "),
            }
        except IndexError:
            logger.error(f"stap 3 dynalogic failed, message id {message_treads_id}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            post_land_temp = mail_body.xpath("//span[normalize-space()='Afspraak details']/../text()")[-5]
            if "B-" in post_land_temp:
                mail_info["postcode"], mail_info["city"] = post_land_temp.split(" ", 1)
                mail_info["postcode"] = mail_info["postcode"].replace("B-", "")
            else:
                mail_info["postcode_cijfers"], mail_info["postcode_letters"], *mail_info["city"] = post_land_temp.split(
                    " ", 2
                )
                mail_info["postcode"] = mail_info["postcode_cijfers"] + mail_info["postcode_letters"]
                if len(mail_info["postcode_cijfers"]) > 4:
                    mail_info["postcode"], *mail_info["city"] = post_land_temp.split(" ", 2)
                mail_info["city"] = " ".join(mail_info["city"])
            dynalogic_api_info = httpx.get(
                f"https://track.mydynalogic.eu/api/transportorder/full/ordernumber/{mail_info['tt_num']}/zipcode/{mail_info['postcode']}",
                headers={"referer": "https://track.mydynalogic.eu/track/order", "X-Requested-With": "XMLHttpRequest"},
            ).json()
            mail_info["order_num"] = str(dynalogic_api_info["data"]["OrderData"]["OrderNumber"])

            mail_info["company"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["Company"]
            if " " in mail_info["company"]:
                mail_info["first_name"], mail_info["last_name"] = dynalogic_api_info["data"]["OrderData"]["Addressee"][
                    "Company"
                ].split(" ", 1)
            else:
                mail_info["first_name"] = mail_info["company"]
            mail_info["house_number"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["HouseNumber"]
            mail_info["street"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["Street"]

            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dynalogic {mail_info} failed {e}")


def process_transmision_messages(conn):
    # get send info Exellent nederland.
    message_treads_ids = get_messages(conn, 'from:(expeditie@schuurman.nl) subject:("Schuurman")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        if mail_body.xpath("//td[contains(text(),'0010264')]"):
            drop = True
        else:
            drop = None
        try:
            mail_info = {
                "dienst": "transmision",
                "tt_url": mail_body.xpath("//a[contains(text(),'Link naar zendingstatus')]/@href")[0],
                "order_num": mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../../..//tr[3]/td[1]/text()")[0].strip(),
            }
            if "_" not in mail_info["order_num"] and "-" not in mail_info["order_num"] and not drop:
                logger.info(f"stap 3 transmission no order_nr bol/blokker, message id {message_treads_id}")
                conn.users().messages().modify(
                    userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
                ).execute()
                continue
        except IndexError:
            logger.info(f"stap 3 transmission failed, message id {message_treads_id}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            page = httpx.get(mail_info["tt_url"])
            page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
            mail_info["tt_num"] = page_body.xpath(
                "//p[normalize-space()='Zendingnummer']/../h4/text()"
            )[0].replace(" ", "")
        except Exception:
            logger.error(f"getting t&t transmission failed, message id {message_treads_id}")
            continue

        try:
            mail_info["order_num"] = mail_info["order_num"].replace("-","_")
            mail_info["first_name"], *mail_info["last_name"] = [
                x.strip()
                for x in re.split(
                    r"[ .]", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[1].lower(), maxsplit=1
                )
            ]
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"], mail_info["house_number"], *_ = [
                x.strip()
                for x in re.split(
                    r"(\d+)", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[2].lower()
                )
            ]
            mail_info["postcode"], mail_info["city"] = [
                x.strip()
                for x in re.split(" ", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[3].lower(), maxsplit=1)
            ]
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 transmission {mail_info} failed {e}")


def process_gls_messages(conn):
    # get send info Amacom.
    message_treads_ids = get_messages(conn, 'from:(*@gls-netherlands.com) subject:"jouw pakket van GLS!"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        mail_info = {
            "dienst": "gls",
            "tt_url": mail_body.xpath("//a[contains(@href,'tracking')]/@href")[0],
        }
        try:
            mail_info["tt_num"] = mail_info["tt_url"].split("=")[1].split("&")[0]
            if not mail_body.xpath("//td/table[contains(@bgcolor,'#F3F3F3')]//tr[7]/td//text()"):
                logger.info(f"gls bestelling voor ons zelf {mail_info}")
                add_label_processed_verzending(conn, message_treads_id)
                continue
            _, mail_info["last_name"], mail_info["first_name"] = [
                x.strip(",")
                for x in re.split(
                    r"(\w+)$",
                    mail_body.xpath("//td/table[contains(@bgcolor,'#F3F3F3')]//tr[7]/td//text()")[1].strip().lower(),
                )
            ]
            mail_info["street"], *mail_info["house_number"] = [
                x.strip(",")
                for x in re.split(
                    r"(\d.+)",
                    mail_body.xpath("//td/table[contains(@bgcolor,'#F3F3F3')]//tr[8]/td/text()")[1]
                    .replace("\r\n", "")
                    .strip()
                    .lower(),
                )
            ]
            mail_info["house_number"] = " ".join(mail_info["house_number"])
            mail_info["postcode"], mail_info["city"] = (
                mail_body.xpath("//td/table[contains(@bgcolor,'#F3F3F3')]//tr[9]/td/text()")[1].strip().lower().split()
            )
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 gls {mail_info} failed {e}")

    message_treads_ids = get_messages(conn, 'from:(*@gls-netherlands.com) subject:"Informatie over uw zending via GLS"')
    for message_treads_id in message_treads_ids:
        mark_read(conn, message_treads_id)
        add_label_processed_verzending(conn, message_treads_id)


def process_dpd_messages(conn):
    # get send info difox
    message_treads_ids = get_messages(conn, 'from:(*@difox.com) subject:"Verzend informatie"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dpd",
                "tt_url": mail_body.xpath("//a[contains(text(),'Zending volgen')]/@href")[0],
                "tt_num": mail_body.xpath("//td[contains(text(),'Paket')]")[0]
                .text.split(":")[-1]
                .split("-")[0]
                .strip(),
            }
        except (IndexError, ValueError) as e:
            logger.info(f"stap 3 dpd failed because of no tt_link, message id {message_treads_id} {e}")
            add_label_processed_verzending(conn, message_treads_id)
            continue
        try:
            *_, mail_info["order_num"], _ = [
                x.strip().strip(",").replace("-", "")
                for x in re.split(
                    r"(.*)bestelnr: (.+\s)",
                    mail_body.xpath("//p[contains(text(),'Referentienummer:')]/text()")[0].lower(),
                )
            ]
        except (IndexError, ValueError) as e:
            logger.info(f"stap 3 dpd failed because of local delivery to us, message id {message_treads_id} {e}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            mail_info["full_name"], mail_info["street_nr"], mail_info["county_post_city"] = [
                x.strip()
                for x in re.split(
                    ",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower()
                )
            ]
        except (IndexError, ValueError) as e:
            mail_info["full_name"] = (
                mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower().strip()
            )
            try:
                mail_info["street_nr"], mail_info["county_post_city"] = [
                    x.strip()
                    for x in re.split(
                        ",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[3]")[0].lower()
                    )
                ]
            except (IndexError, ValueError) as e:
                *mail_info["full_name"], mail_info["street_nr"], mail_info["county_post_city"] = [
                    x.strip()
                    for x in re.split(
                        ",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower()
                    )
                ]
                mail_info["full_name"] = " ".join(mail_info["full_name"])
        try:
            mail_info["first_name"] = mail_info["full_name"].split(" ", 1)[0]
            if len(mail_info["county_post_city"]) < 5:
                # sometime info on next line
                mail_info["full_name"], mail_info["street_nr"], _ = [
                    x.strip()
                    for x in re.split(
                        ",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower()
                    )
                ]
                mail_info["county_post_city"] = mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[3]")[
                    0
                ].lower()

            *_, mail_info["postcode"], mail_info["city"], _ = re.split(
                r"(\w+-)(\d+\w+)\s(.+)", mail_info["county_post_city"]
            )
            try:
                _, mail_info["street"], mail_info["house_number"], _ = re.split(
                    r"(\D+) (\d+.*)", mail_info["street_nr"]
                )
            except (IndexError, ValueError) as e:
                logger.info(f"amazon ? {e}")
                mark_read(conn, message_treads_id)
                add_label_processed_verzending(conn, message_treads_id)
                continue
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dpd {mail_info} failed {e}")

    # get send info excellent, so different that i need to do it separate
    message_treads_ids = get_messages(conn, 'from:(*@dpd.nl) subject:("Je pakket")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        # print(etree.tostring(mail_body, pretty_print=True, encoding='unicode'))
        try:
            mail_info = {
                "dienst": "dpd",
                "tt_url": mail_body.xpath(
                    "//img[@alt='Volg je pakket']/../@href | //img[@style='float:right']/../@href"
                )[0],
                "tt_num": mail_body.xpath("//td[contains(text(), 'Pakketnummer')]/text()")[0].strip().split('Pakketnummer')[1].strip(),
                "full_name" : mail_body.xpath("//p[contains(text(), 'op dit adres')]/../b/text()")[0].strip(),
                "street_house": mail_body.xpath("//p[contains(text(), 'op dit adres')]/../b/text()")[1].strip(),
                "postcode_plaats" : mail_body.xpath("//p[contains(text(), 'op dit adres')]/../b/text()")[2].strip(),
            }
        except (IndexError, ValueError) as e:
            logger.info(
                f"stap 3 dpd failed because of no tt_link(zoals in bezorgd links), message id {message_treads_id} {e}"
            )
            mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
            continue
        try:
            mail_info["first_name"], *mail_info["last_name"] = mail_info["full_name"].lower().split(" ", 1)
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            *mail_info["street"], mail_info["house_number"] = mail_info["street_house"].split(" ")
            mail_info["street"] = " ".join(mail_info["street"])
            mail_info["postcode"], *mail_info["city"] = re.split(" ", mail_info["postcode_plaats"])
            mail_info["city"] = " ".join(mail_info["city"])
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dpd {mail_info} failed {e}")
    # get send info excellent, so different in be that i need to do it separate
    message_treads_ids = get_messages(conn, 'from:(*@dpd.be ) subject:("Je pakket is")')
    num = 1
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        # print(etree.tostring(mail_body, pretty_print=True, encoding='unicode'))
        try:
            mail_info = {
                "dienst": "dpd",
                "tt_url": mail_body.xpath(
                    "//img[@alt='Volg je pakket']/../@href | //img[@style='float:right']/../@href"
                )[0],
                "tt_num": re.search(r"(\d+)", mail_body.xpath("//span[contains(text(), 'pakketnummer')]/text()")[0]).group(1),
                "full_name" : mail_body.xpath("//span[text()='leveradres*:']//text()")[1].strip(),
                "street_house": mail_body.xpath("//span[text()='leveradres*:']//text()")[2].strip(),
                "postcode_plaats" : mail_body.xpath("//span[text()='leveradres*:']//text()")[3].strip(),
            }
        except (IndexError, ValueError) as e:
            logger.info(
                f"stap 3 dpd failed because of no tt_link(zoals in bezorgd links), message id {message_treads_id} {e}"
            )
            mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
            continue
        try:
            mail_info["first_name"], *mail_info["last_name"] = mail_info["full_name"].lower().split(" ", 1)
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            *mail_info["street"], mail_info["house_number"] = mail_info["street_house"].split(" ")
            mail_info["street"] = " ".join(mail_info["street"])
            mail_info["postcode"], *mail_info["city"] = re.split(" ", mail_info["postcode_plaats"])
            mail_info["city"] = " ".join(mail_info["city"])
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dpd {mail_info} failed {e}")

def process_postnl_ur_messages(conn):
    # get send info united retail.
    message_treads_ids = get_messages(conn, 'from: info@vangilsweb.nl subject:("Uw bestelling is verzonden")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        mail_info = {
            "dienst": "united",
            "tt_url": mail_body.xpath("//*[contains(text(),'Volg mijn ')]/../a/@href")[0],
        }
        try:
            mail_info["tt_num"] = mail_info["tt_url"].split("/")[-1].split("-")[0]
            postnl_api_info = (
                httpx.get(
                    f"https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/{mail_info['tt_url'].split('/')[-1]}?language=nl"
                )
                .json()["colli"]
                .get(mail_info["tt_num"])
            )
            if postnl_api_info["recipient"]["names"].get("personName"):
                mail_info["first_name"], *mail_info["last_name"] = (
                    postnl_api_info["recipient"]["names"].get("personName").split()
                )
            elif postnl_api_info["recipient"]["names"].get("companyName"):
                mail_info["first_name"], *mail_info["last_name"] = (
                    postnl_api_info["recipient"]["names"].get("companyName").split()
                )
            else:  # sometimes, name not processed bij postnl,so use from mail
                _, mail_info["first_name"], *mail_info["last_name"] = [
                    x.strip().strip(",")
                    for x in re.split(
                        r"^beste\s+(\w+)", mail_body.xpath("//strong[contains(text(),'Beste ')]")[0].text.lower()
                    )
                ]
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = postnl_api_info["recipient"]["address"]["street"]
            mail_info["house_number"] = postnl_api_info["recipient"]["address"]["houseNumber"]
            mail_info["postcode"] = postnl_api_info["recipient"]["address"]["postalCode"]
            mail_info["city"] = postnl_api_info["recipient"]["address"]["town"]
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 united {mail_info} failed {e}")
    message_treads_ids = get_messages(
        conn,
        'from:(*@vangilsweb.nl| *@edm.postnl.*) subject:("Dit is de bezorgtijd van je pakket" | "We zijn naar je onderweg met je pakket")',
    )
    for message_treads_id in message_treads_ids:
        mark_read(conn, message_treads_id)
        add_label_processed_verzending(conn, message_treads_id)

def process_eltric_messages(conn):
    message_treads_ids = get_messages(conn, 'from: info@eltric.com subject:("Versand E-Mail - eltric GmbH")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        # print(etree.tostring(mail_body, pretty_print=True, encoding='unicode'))
        if mail_body is not None and mail_body.xpath("//*[contains(text(),'4823AB Breda')]"):
            logger.info(f"pakket voor ons zelf")
            mark_read(conn, message_treads_id)
            add_label_processed_verzending(conn, message_treads_id)
        elif mail_body is not None:
            mail_info = {
                "dienst": "electric",
                "tt_url": mail_body.xpath("//*[contains(text(),'Tracking-Nummer')]/../../tr[2]//a/@href")[0],
                "tt_num": mail_body.xpath("//*[contains(text(),'Tracking-Nummer')]/../../tr[2]//a/text()")[0],
            }
            try:
                if "PORTAL-WEB" in mail_info["tt_url"]: # voor gls want link klopt niet !!
                    mail_info["tt_url"] = f"https://gls-group.eu/EU/en/parcel-tracking?match={mail_info['tt_num']}"
                mail_info["first_name"], *mail_info["last_name"] = mail_body.xpath("//*[contains(text(),'Lieferung-Adresse')]/../td[2]/text()")[0].strip().split(" ", 1)
                mail_info["last_name"]= mail_info["last_name"][0]
                mail_info["street"], mail_info["house_number"] = mail_body.xpath("//*[contains(text(),'Lieferung-Adresse')]/..//../tr[6]/td[2]/text()")[0].strip().split(" ", 1)
                _ ,mail_info["postcode"], mail_info["city"] = mail_body.xpath("//*[contains(text(),'Lieferung-Adresse')]/..//../tr[7]/td[2]/text()")[0].strip().split(" ", 2)
                get_order_info_db = get_set_info_database(mail_info)
                if get_order_info_db:
                    mark_read(conn, message_treads_id)
                add_label_processed_verzending(conn, message_treads_id)
            except Exception as e:
                logger.error(f"stap 3 eltric {mail_info} failed {e}")
        else:
            logger.error(f"Mail body is None for message id {message_treads_id}")


def process_beekman_messages(conn):
    query = """
        SELECT 
            I.orderid,
            order_orderitemid,
            offer_sku,
            verkooporder_id_leverancier,
            shipmentdetails_zipcode,
            shipmentdetails_countrycode 
        FROM 
            orders_bol O 
        LEFT JOIN 
            orders_info_bol I 
        ON 
            O.orderid = I.orderid 
        WHERE 
            offer_sku LIKE 'APD%' 
            AND dropship = 1
            AND active_order = 1
            AND order_id_leverancier IS NULL 
            AND verkooporder_id_leverancier IS NOT NULL
    """
    with engine.connect() as connection:
        open_orders = connection.execute(text(query)).mappings().all()
    session = httpx.Client()
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {config.get("beekman", "api_key")}'
    }
    for order in open_orders:
        order_status = session.get(f"https://rest.beekman.nl/dropshipment?id={order['verkooporder_id_leverancier']}", headers=headers)
        if order_status.status_code == 200:
            order_info = order_status.json()
            if order_info.get("error"):
                logger.error(f"{order_info['orderid']} {order_info['error_message']}")
            else:
                shipments = order_info.get("shipments", [])
                for shipment in shipments:
                    trackcode = shipment.get('trackcode').split('|')[0]
                    trackurl = shipment.get('trackurl')
                    if order_info.get('items'):
                        for item in order_info.get('items').get('articles', []):
                            if "".join(filter(str.isdigit, order.get("offer_sku"))) == item.get('code'):
                                set_order_info_db_bol(order, trackurl, trackcode)
                    if not trackcode:
                       logger.info(f"{order_info.get('orderid')} Nog geen T&T nummer bekend ")  

    message_treads_ids = get_messages(conn, 'from:(*@beekman.nl) "Verzend bevestiging"')
    for message_treads_id in message_treads_ids:
        mark_read(conn, message_treads_id)
        add_label_processed_verzending(conn, message_treads_id)

def process_visynet_api():
    # get still open orders and check if they have a track and trace
    query = """
        SELECT 
            I.orderid,
            order_orderitemid,
            verkooporder_id_leverancier,
            shipmentdetails_zipcode,
            shipmentdetails_countrycode 
        FROM 
            orders_bol O 
        LEFT JOIN 
            orders_info_bol I 
        ON 
            O.orderid = I.orderid 
        WHERE 
            offer_sku LIKE 'VIS%' 
            AND dropship = 1 
            AND active_order = 1 
            AND verkooporder_id_leverancier IS NOT NULL
    """
    with engine.connect() as connection:
        open_orders = connection.execute(text(query)).mappings().all()
    session = httpx.Client(verify=False)
    access_token = session.post(
        config.get("visynet api", "basis_url") + "/auth/requesttoken",
        data=json.dumps({
            "email": config.get("visynet api", "email"),
            "password": config.get("visynet api", "password"),
        }),
        headers = {'Content-Type': 'application/json'},
        timeout=10,
    ).json()["token"]
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    for order_info in open_orders:
        order_status = session.post(f"{config.get('visynet api', 'basis_url')}/order/status", json={"orderid" : order_info['verkooporder_id_leverancier'] })
        if order_status.status_code == 200:
            if order_status.json().get("error_message"):
                logger.error(f"{order_info['orderid']} {order_status.json()['error_message']}")
            else:
                if order_status.json()['result']['Carrier']:
                    if "PostNL" in order_status.json()['result']['Carrier']: 
                        set_order_info_db_bol(order_info, f"https://jouw.postnl.nl/#!/track-en-trace/{order_status.json()['result']['trackingnumber']}/{order_info['shipmentdetails_countrycode']}/{order_info['shipmentdetails_zipcode']} ", order_status.json()['result']['trackingnumber'])
                    elif "GLS" in order_status.json()['result']['Carrier']: 
                        set_order_info_db_bol(order_info, f"https://gls-group.eu/EU/en/parcel-tracking?match={order_status.json()['result']['trackingnumber']}", order_status.json()['result']['trackingnumber'])
                    elif "UPS" in order_status.json()['result']['Carrier']:
                        set_order_info_db_bol(order_info, f"https://www.ups.com/track?loc=nl_NL&{order_status.json()['result']['trackingnumber']}", order_status.json()['result']['trackingnumber'])
                else:
                       logger.info(f"{order_info.get('orderid')} Nog geen T&T nummer bekend ")     


def process_ftp_files_tt_exl(server, login, wachtwoord):
    """Omdat ze deze nu alleen via ftp beschikbaar maken voor dropshipment"""
    with FTP(server) as ftp:
        ftp.login(login, passwd=wachtwoord)

        file_names = ftp.nlst()
        tt_files = [line for line in file_names if "xml" in line]

        for file in tt_files:
            file_lines = []
            ftp.retrlines(f"RETR {file}", file_lines.append)
            xml_content = "\n".join(file_lines)
            parse_xml = et.fromstring(xml_content)
            if parse_xml.find(".//carrier_id").text == "DPD":
                logger.info(f"transporteur {parse_xml.find('.//carrier_id').text}")
                order_id = parse_xml.find(".//OrderExternalId_01").text
                tt_number = parse_xml.find(".//trackingnumber").text
                track_en_trace_url = parse_xml.find(".//trackingurl").text
                if not track_en_trace_url:
                    track_en_trace_url = f"https://www.dpdgroup.com/be/mydpd/my-parcels/search?lang=nl&parcelNumber={tt_number }"
            else:
                logger.info(f"transporteur {parse_xml.find('.//carrier_id').text}")
                order_id = parse_xml.find(".//OrderExternalId_01").text
                tt_number = parse_xml.find(".//trackingnumber").text
                track_en_trace_url = parse_xml.find(".//trackingurl").text
            if order_id: 
                if "_" in order_id:
                    info_bol_db = f"SELECT orderid,order_orderitemid FROM orders_info_bol WHERE orderid = '{order_id}'"
                    with engine.connect() as connection:
                        order_info = connection.exec_driver_sql(info_bol_db).first()
                    set_order_info_db_bol(order_info, track_en_trace_url, tt_number)
                    logger.info(f"{order_info} order bol tt {tt_number}, {track_en_trace_url} exellent toegevoegd ")  
                elif "-" in order_id:
                    info_blok_db = f"SELECT I.order_line_id FROM blokker_orders O LEFT JOIN blokker_order_items I ON O.commercialid = I.commercialid WHERE order_id = '{order_id}'"
                    with engine.connect() as connection:
                        order_line_id = connection.exec_driver_sql(info_blok_db).first()
                    set_order_info_db_blokker(order_line_id, track_en_trace_url, tt_number)
                    logger.info(f"{order_info} order blokker tt {tt_number}, {track_en_trace_url} exellent toegevoegd ")
                ftp.delete(file)
                pass
            else:
                if parse_xml.find(".//customer_line_id").text == "Manually Inserted":
                    ftp.delete(file)


def gmail_send_mail(
    conn, order_id, kvk_winkel, bol_email, waarvoor
):  # meerdere bol winkel onder zelfde kvk, maar kvk bepaald de layout
    """Create and insert a draft email.
    Print the returned draft's message and id.
    Returns: Draft object, including draft id and message meta data."""
    try:
        # html text for mail
        standaard_begin_text = "Beste klant,<br><br>Bedankt voor uw bestelling!<br><br>We willen er zeker van zijn dat het onderdeel dat u heeft besteld perfect in uw apparaat past. Veel modellen lijken op elkaar, maar kleine verschillen kunnen ervoor zorgen dat het onderdeel niet past. Daarom helpen we u graag om dit vooraf te controleren.<br><br>"
        standaard_eind_text = "<br><br><br>Met vriendelijke groet,<br><br>Louise<br>"
        appraat_begin = f"U heeft bij ons een {waarvoor} besteld voor uw "
        appraat_eind = f". Nu blijkt uit ervaring dat er vaak wat onduidelijkheid heerst over het type {waarvoor} dat er nodig is.<br>"
        typeplaatje_begin = "Mocht u twijfelen of u het juiste exemplaar heeft besteld, willen wij u vragen om een foto te maken van het typeplaatje dat op uw apparaat staat. In sommige gevallen zit er geen typeplaatje op het apparaat maar staat het typenummer in het apparaat zelf gedrukt. Het typeplaatje vindt u meestal aan de "
        typeplaatje_midden = " van uw apparaat"
        typeplaatje_midden_2 = "en bestaat uit een combinatie van letters en cijfers. Let op! Ook de cijfers na het /-teken zijn belangrijk. Een foto van uw "
        typeplaatje_eind = f" of {waarvoor} is niet voldoende om het typenummer te bepalen. <br> <br> Mocht u het typeplaatje niet kunnen vinden laat het ons dan weten, wij helpen u alsnog graag verder.<br> <br> In de bijlage sturen wij een afbeelding mee van de meest voorkomende plaatsen waar u het typenummer kunt vinden op uw apparaat.<br><br> "

        type_info = {
            "waterreservoir": {
            "type_device": f"{appraat_begin}apparaat{appraat_eind} {typeplaatje_begin}onderkant{typeplaatje_midden}{typeplaatje_midden_2}apparaat{typeplaatje_eind}",
            "afbeelding": "padhouder",
            },
            "padhouder": {
            "type_device": f"{appraat_begin}apparaat{appraat_eind} {typeplaatje_begin}onderkant{typeplaatje_midden}{typeplaatje_midden_2}apparaat{typeplaatje_eind}",
            "afbeelding": "padhouder",
            },
            "draaiplateau": {
            "type_device": f"{appraat_begin}magnetron{appraat_eind} {typeplaatje_begin}achterkant of binnenkant{typeplaatje_midden}{typeplaatje_midden_2}magnetron{typeplaatje_eind}",
            "afbeelding": "draaiplateau",
            },
            "deurbak": {
            "type_device": f"{appraat_begin}koelkast{appraat_eind} {typeplaatje_begin}achterkant{typeplaatje_midden} of in het koelgedeelte, boven de bovenste plank op een van de zijwanden van de koelkast {typeplaatje_midden_2}koelkast{typeplaatje_eind}",
            "afbeelding": "koelkast",
            },
            "groentelade": {
            "type_device": f"{appraat_begin}koelkast{appraat_eind} {typeplaatje_begin}achterkant{typeplaatje_midden} of in het koelgedeelte, boven de bovenste plank op een van de zijwanden van de koelkast {typeplaatje_midden_2}koelkast{typeplaatje_eind}",
            "afbeelding": "koelkast",
            },
            "flessenrek": {
            "type_device": f"{appraat_begin}koelkast{appraat_eind} {typeplaatje_begin}achterkant{typeplaatje_midden} of in het koelgedeelte, boven de bovenste plank op een van de zijwanden van de koelkast {typeplaatje_midden_2}koelkast{typeplaatje_eind}",
            "afbeelding": "koelkast",
            },
        }
        info = type_info.get(waarvoor)
        # info["type_device"]
        message = EmailMessage()
        message["To"] = bol_email

        image_cid = make_msgid(domain=f"{kvk_winkel}.nl")
        image_cid2 = make_msgid(domain=f"{kvk_winkel}.nl")
        if kvk_winkel == "toopbv":
            message["From"] = "info@toopbv.nl"
            sign_picture = "vangils - TOOP Fulfilment.jpg"
            naam_shop = "toop"
        elif kvk_winkel == "vangilsweb":
            message["From"] = "info@vangilsweb.nl"
            sign_picture = "vangils - TOOP Fulfilment.jpg"
            naam_shop = "vangils web" 
        message.add_alternative(f"<html><body>{standaard_begin_text}{info['type_device']} Vanwege onze vlugge orderverwerking is het belangrijk dat u tijdig reageert op dit mailbericht! <br>\
                                    Heeft u besteld voor 00:00 uur, dan hebben wij vandaag voor 9:00 uur de juiste bestelgegevens nodig om de 24 uur service te kunnen leveren.<br>\
                                    Heeft u besteld voor 15:00 uur, dan hebben wij vandaag voor 15:30 uur de juiste bestelgegevens nodig om de 24 uur service te kunnen leveren.<br><br>\
                                    Om gebruik te kunnen blijven maken van onze 1-3 werkdagen bezorgservice is het belangrijk dat u binnen 24 uur reageert op dit mailbericht!<br><br>\
                                    Zonder tegenbericht wordt uw bestelling ongewijzigd doorgevoerd.\
                                {standaard_eind_text}<img src='cid:{image_cid[1:-1]}' alt='https://toop.nl/uitleg.html'> <br> <br> <img src='cid:{image_cid2[1:-1]}' alt={naam_shop}></body></html>",subtype="html",)

        with open(f"afbeelding_met_uitleg_{info['afbeelding']}.png", "rb") as img:
            maintype, subtype = mimetypes.guess_type(img.name)[0].split("/")
            message.get_payload()[0].add_related(
                img.read(), maintype=maintype, subtype=subtype, cid=image_cid, filename=f"afbeelding_met_uitleg_{info['afbeelding']}"
            )

        with open(sign_picture, "rb") as img:
            # know the Content-Type of the image
            maintype, subtype = mimetypes.guess_type(img.name)[0].split("/")
            message.get_payload()[0].add_related(
                img.read(), maintype=maintype, subtype=subtype, cid=image_cid2, filename=sign_picture
            )
        message["Subject"] = f"Juiste {waarvoor} Besteld ?! {order_id} \U00002705 \U0000274C" #unicode for nice icons https://unicode.org/emoji/charts/full-emoji-list.html
        # encoded the message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {"raw": encoded_message}
        send_message = conn.users().messages().send(userId="me", body=create_message).execute()
        logger.info(f"sending succesful {send_message['id']}")
        return True

    except HttpError as e:
        logger.error(f"sending failed {e}")
        send_message = None
        return False


def process_bol_orders(conn, product_type, zoek_string):
    message_treads_ids = get_messages(
        conn,
        f'to:*@vangilsweb.nl OR to:*@toopbv.nl subject:"Nieuwe bestelling:" {zoek_string}',
        gewenste_aantal_dagen="1d",
    )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        headers = message["payload"]["headers"]
        order_nr = re.search(r"bestelnummer:\s*([A-Za-z0-9-]+)", [i["value"] for i in headers if i["name"] == "Subject"][0]).group(1).replace("-","")

        try:
            _, webwinkel, to, _ = re.split("[@ .]", [i["value"] for i in headers if i["name"] == "To"][0])
        except ValueError as e:
            webwinkel, to, _ = re.split("[@ .]", [i["value"] for i in headers if i["name"] == "To"][0])
        winkel = {"alldayelektro": "_ADE", "info": "_TB", "tpshopper": "_TS", "typischelektro": "_TE"}
        winkel_short = winkel.get(webwinkel)
        odin_order_nr = f"{order_nr}{winkel_short}"
        try:
            with engine.begin() as db_conn:
                bol_email_addres = db_conn.exec_driver_sql(
                    "SELECT shipmentdetails_email FROM orders_bol WHERE orderid LIKE %s", (odin_order_nr,)
                ).first()[0]
            send = gmail_send_mail(conn, odin_order_nr, to, bol_email_addres, product_type)
        except Exception as e:
            send = False
            logger.error(f"order_id not found {odin_order_nr} {e}")
        if send:
            add_label_processed_return(conn, message_treads_id)
            set_mailsend_db_bol(odin_order_nr)

def process_if_replays_juiste_product(conn):
    message_treads_ids = get_messages(
        conn,
        f'to:*@vangilsweb.nl OR to:*@toopbv.nl OR to:toopbv@gmail.com subject:"Juiste"',
        gewenste_aantal_dagen="5d",
    )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        headers = message["payload"]["headers"]
        match = re.search(r"\?\!\s*(.*?)\s*✅", [i["value"] for i in headers if i["name"] == "Subject"][0].rsplit(":")[-1])
        if match:  # Check if a match was found
            order_nr = match.group(1)
            set_replay_mailsend_db_bol(order_nr)
            add_label_processed_verzending(conn, message_treads_id)

def klantvragen_bol(conn):
    message_treads_ids = get_messages(
            conn,
            f'to:*@vangilsweb.nl OR to:*@toopbv.nl subject:"klantvraag in je verkoopaccount"',
            gewenste_aantal_dagen="10d",
        )
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        headers = message["payload"]["headers"]
        try:
            # print(next((header["value"] for header in message["payload"]["headers"] if header["name"] == "Subject"), "Subject header not found."))
            order_nr = re.search(r"bestelnummer \s*([A-Za-z0-9-]+)", [i["value"] for i in headers if i["name"] == "Subject"][0]).group(1).replace("-","")
            mail_body = get_body_email(message)
            klantvragen_url = mail_body.xpath("//*[contains(text(),'Klantvraag')]/../a/@href")[0]
        except (ValueError, AttributeError) as e:
            add_label_processed_verzending(conn, message_treads_id)
            mark_read(conn, message_treads_id)
            logger.error(f"orderid not found in email header {e}")
            continue
        try:
            _, webwinkel, to, _ = re.split("[@ .]", [i["value"] for i in headers if i["name"] == "To"][0])
        except ValueError as e:
            webwinkel, to, _ = re.split("[@ .]", [i["value"] for i in headers if i["name"] == "To"][0])
        winkel = {"alldayelektro": "_ADE", "info": "_TB", "tpshopper": "_TS", "typischelektro": "_TE"}
        winkel_short = winkel.get(webwinkel)
        if order_nr:
            odin_order_nr = f"{order_nr}{winkel_short}"
            try:
                with engine.begin() as db_conn:
                    sql = text("UPDATE orders_bol SET klantvragen = :klantvragen, klantvragen_url =:klantvragen_url  WHERE orderid = :orderid")
                    db_conn.execute(sql, {"klantvragen": True, "klantvragen_url":klantvragen_url, "orderid": odin_order_nr})
                    add_label_processed_verzending(conn, message_treads_id)
                    mark_read(conn, message_treads_id)
            except Exception as e:
                logger.error(f"orderid not found {odin_order_nr} {e}")


def verkrijgen_shipmentids_bol():
    # doorlopen van https://api.bol.com/retailer/public/redoc/v10/retailer.html#tag/Shipments/operation/get-shipments om all shipmentid's te krijgen, eerst locken, want dit kan prima 1 keer per dag.
    LOCK_FILE = Path("shipment.txt")
    TARGET_HOUR = 8

    if datetime.datetime.now().hour < TARGET_HOUR and LOCK_FILE.exists():
        try:
            LOCK_FILE.unlink()
        except Exception:
            pass  # Ignore errors on lock file removal

    if LOCK_FILE.exists():
        return


    LOCK_FILE.touch()
    for winkel, credentials in config["bol_winkels_api"].items():
        bulk_updates = []
        user_key, passkey, winkel_name, winkel_short = credentials.split(",")
        try:
            auth_response = httpx.post(
                "https://login.bol.com/token",
                data={"grant_type": "client_credentials"},
                auth=(user_key, passkey),
            )
            auth_response.raise_for_status()
            access_token = auth_response.json().get("access_token")
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f"Authentication failed for {winkel_name}: {e}")
            continue

        session = httpx.Client(headers={
            "Accept": "application/vnd.retailer.v10+json",
            "Authorization": f"Bearer {access_token}",
        })

        for page in range(1,10):
            try:
                response = session.get(
                    f"https://api.bol.com/retailer/shipments?page={page}"
                )
                time.sleep(3)  # Respect rate limits
                response.raise_for_status()
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                logger.error(f"Request failed for {winkel_name}: {e}")
                break

            if not (shipments := response.json().get("shipments", [])):
                break

            for shipment in shipments:
                shipment_id = shipment.get("shipmentId")
                shipment_items = shipment.get("shipmentItems", [])

                if shipment_id and shipment_items:
                    bulk_updates.append({
                        "shipmentid": shipment_id,
                        "order_item_id": shipment_items[0].get("orderItemId"),
                        "order_item_ean": shipment_items[0].get("ean")
                    })
        if bulk_updates:
            try:
                update_query = text(
                    "UPDATE orders_info_bol SET shipmentid = :shipmentid "
                    "WHERE order_orderitemid = :order_item_id AND order_ean = :order_item_ean"
                )
                
                with engine.begin() as connection:
                    connection.execute(update_query, bulk_updates)
                
                logger.info(f"Bulk updated {len(bulk_updates)} shipment records")
            
            except Exception as e:
                logger.error(f"Bulk update failed: {e}")
                # Optionally, fallback to individual updates if bulk fails
                for update in bulk_updates:
                    try:
                        with engine.begin() as connection:
                            connection.execute(update_query, update)
                    except Exception as individual_e:
                        logger.error(f"Failed to update individual record: {update}, Error: {individual_e}")

@dataclass
class RateLimitData:
    window: int = 0      # Current 1-minute window (timestamp)
    count: int = 0 

def automatische_facturen_bol():
    query = """
        SELECT
            O.orderid,
            order_ean,
            shipmentid,
            winkel_artikel
        FROM 
            orders_bol O 
        LEFT JOIN 
            orders_info_bol I 
        ON
            O.orderid = I.orderid 
        WHERE
            shipmentid IS NOT NULL
            AND I.factuur_verstuurd IS NULL
            AND O.billingdetails_countrycode IN ('NL', 'BE')
        ORDER BY
            winkel_artikel DESC, O.orderid ASC
            """
    try:
        with engine.connect() as connection:
            nog_geen_factuur_verzonden = connection.execute(text(query)).mappings().all()
    except SQLAlchemyError as e:
        logger.error(f"Database query failed: {e}")
        return
    
    winkels = {"ADE": "vangils", "TB": "toop", "TS": "toop", "TE": "toop"}
    odin_url = "https://toop.nl:25583/odin_website/pdf_factuur_maken"
    # odin_url = "http://127.0.0.1:7002/odin_website/pdf_factuur_maken" # for testing
    credentials_map = {}
    for shop, credentials in config["bol_winkels_api"].items():
        try:
            user_key, passkey, winkel_name, winkel_short = [x.strip() for x in credentials.split(",")]
            credentials_map[winkel_name] = (user_key, passkey, winkel_short)
        except Exception as e:
            logger.error(f"Unexpected error parsing credentials for '{shop}': {e}. Entry: '{credentials}'")
            continue

    current_shop = None
    current_session = None
    invoice_upload_rate_limiter = RateLimitData()
    MAX_REQUESTS_PER_MINUTE = 25

    for factuur in nog_geen_factuur_verzonden:
        order_shop = factuur['winkel_artikel']
        if order_shop not in credentials_map:
            logger.error(f"No credentials configured for shop name: {order_shop} (Order ID: {factuur['orderid']}). Skipping.")
            continue

        # Shop changed - reauthenticate
        if order_shop != current_shop:
            if current_session:
                current_session.close()

            if order_shop not in credentials_map:
                logger.error(f"No credentials found for {order_shop} when trying to re-authenticate.")
                current_shop = None
                current_session = None
                continue

            try:
                user_key, passkey, winkel_short = credentials_map[order_shop]
                auth_response = httpx.post(
                    "https://login.bol.com/token",
                    data={"grant_type": "client_credentials"},
                    auth=(user_key, passkey),
                    timeout=30
                )
                auth_response.raise_for_status()
                access_token = auth_response.json().get('access_token')
                if not access_token:
                    logger.error(f"Authentication for {order_shop} (UserKey: {user_key}) succeeded but no access_token in response: {auth_response.json()}")
                    current_shop = None
                    current_session = None
                    continue

                current_session = httpx.Client(
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Accept": "application/vnd.retailer.v10+json"
                    },
                    timeout=30
                )
                current_shop = order_shop
                logger.info(f"Authenticated for {order_shop}")

            except Exception as e:
                logger.error(f"General auth failed for {order_shop}: {str(e)}")
                current_shop = None
                current_session = None
                continue

        if current_session:
            # try:
                # Generate PDF
                with httpx.Client(verify=False) as client:
                    verkrijgen_factuur = client.get(f"{odin_url}/{factuur['orderid']}/{factuur['order_ean']}/bol/{winkels.get(factuur['orderid'].split('_')[1]) if '_' in factuur['orderid'] else None}")
                    verkrijgen_factuur.raise_for_status()

                files = {
                        "invoice": (
                            f"invoice_{factuur['orderid']}.pdf",
                            verkrijgen_factuur.content,
                            "application/pdf"
                        )
                    }

                current_time = int(time.time())
                current_minute_window = current_time // 60
               
                if current_minute_window != invoice_upload_rate_limiter.window:
                    invoice_upload_rate_limiter.window = current_minute_window
                    invoice_upload_rate_limiter.count = 0
                    logger.debug(f"Rate limit window reset for invoice uploads at {current_minute_window}")

                if invoice_upload_rate_limiter.count >= MAX_REQUESTS_PER_MINUTE:
                    sleep_duration = 60 - (current_time % 60)
                    logger.info(f"Invoice upload rate limit reached ({MAX_REQUESTS_PER_MINUTE}/min). Sleeping for {sleep_duration} seconds.")
                    time.sleep(sleep_duration)
                    # After sleeping, we are in a new window
                    invoice_upload_rate_limiter.window = (current_time + sleep_duration) // 60
                    invoice_upload_rate_limiter.count = 0
                    logger.debug(f"Rate limit window reset after sleep for invoice uploads at {invoice_upload_rate_limiter.window}")


                invoice_upload_rate_limiter.count += 1
                logger.debug(f"Invoice upload attempt {invoice_upload_rate_limiter.count}/{MAX_REQUESTS_PER_MINUTE} for order {factuur['orderid']}.")
                
                upload_response = current_session.post(
                    f"https://api.bol.com/retailer/shipments/invoices/{factuur['shipmentid']}",
                    files=files
                )
                upload_response.raise_for_status() # Will raise an exception for 4xx/5xx responses
                
                logger.info(f"Invoice upload successful for {factuur['orderid']}, status: {upload_response.status_code}, response: {upload_response.json()['status']}")

                if upload_response.status_code == 202:
                    try:
                        with engine.begin() as conn:
                            conn.execute(
                                text("""
                                    UPDATE orders_info_bol 
                                    SET factuur_verstuurd = 1 
                                    WHERE orderid = :orderid
                                """),
                                {"orderid": factuur["orderid"]}
                            )
                            logger.info(f"Successfully marked order {factuur['orderid']} as sent")
                            
                    except SQLAlchemyError as e:
                        logger.error(f"Database update failed for {factuur['orderid']}: {str(e)}")
                else:
                    logger.error(f"Failed to upload invoice for {factuur['orderid']}: {upload_response.text}")


if __name__ == "__main__":
    credentials = get_autorisation_gooogle_api()
    connection = gmail_create_connection(credentials)
    try:
        process_beekman_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process Beekman messages: {e}")

    try:
        process_bpost_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process Bpost messages: {e}")
    try:
        process_dhl_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process DHL messages: {e}")

    try:
        process_dynalogic_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process Dynalogic messages: {e}")

    try:
        process_transmision_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process Transmision messages: {e}")

    try:
        process_gls_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process GLS messages: {e}")

    try:
        process_dpd_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process DPD messages: {e}")

    try:
        process_postnl_ur_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process PostNL UR messages: {e}")
    try:
        process_eltric_messages(connection)
    except Exception as e:
        print(f"ERROR: Failed to process electric messages: {e}")       
    try:
        process_ftp_files_tt_exl(
            config["excellent dropship tt"]["server"],
            config["excellent dropship tt"]["login"],
            config["excellent dropship tt"]["wachtwoord"],
        )
    except Exception as e:
        print(f"ERROR: Failed to process FTP files: {e}")
    try:
        process_visynet_api()
    except Exception as e:
        print(f"ERROR: Failed to process Visynet API: {e}")
    try:
        verkrijgen_shipmentids_bol()
    except Exception as e:
        print(f"ERROR: Failed to obtain Bol shipment IDs: {e}")
    try:
        automatische_facturen_bol()
    except Exception as e:
        print(f"ERROR: Failed to process automatic Bol invoices: {e}")

    # auto replay on bol, sommige bol mailtje automatisch beantwoorden, om het aantal retouren te verminderen
    process_bol_orders(connection, product_type="waterreservoir", zoek_string="Reservoir -karcher -Philips")
    process_bol_orders(connection, product_type="waterreservoir", zoek_string="Waterreservoir")
    process_bol_orders(connection, product_type="padhouder", zoek_string="padhouder")
    process_bol_orders(connection, product_type="padhouder", zoek_string="Capsule houder")
    process_bol_orders(connection, product_type="draaiplateau", zoek_string="Draaiplateau")
    process_bol_orders(connection, product_type="deurbak", zoek_string="Deurbak")
    process_bol_orders(connection, product_type="deurbak", zoek_string="Deurvak")
    process_bol_orders(connection, product_type="groentelade", zoek_string="Groentelade")
    process_bol_orders(connection, product_type="flessenrek", zoek_string="Flessenrek")

    process_if_replays_juiste_product(connection)
    klantvragen_bol(connection)