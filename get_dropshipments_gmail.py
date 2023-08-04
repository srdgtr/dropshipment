import base64
import configparser
import datetime
from email.utils import make_msgid
import io
import logging
import mimetypes
import pickle
import re
from sqlite3 import OperationalError
import sys
import subprocess
from pathlib import Path
from bs4 import BeautifulSoup
import requests
from email.message import EmailMessage

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
    from sqlalchemy import MetaData, Table, create_engine, select, update, text, and_
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("sqlalchemy")
    from sqlalchemy import MetaData, Table, create_engine, select, update, text, and_

from sqlalchemy import MetaData, Table, create_engine, select, update, text, and_
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

config = configparser.ConfigParser()

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
            authorization_url, state = flow.authorization_url(access_type="offline", login_hint="toopbv@gmail.com", include_granted_scopes="true")
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
    drop_send = (
        update(orders_info_bol)
        .where(and_(orders_info_bol.columns.orderid == order_info[0], orders_info_bol.columns.order_orderitemid == order_info[1]))
        .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
    )
    with engine.begin() as conn:
        conn.execute(drop_send)


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
                    body = base64.urlsafe_b64decode(mess["payload"]["parts"][0]["parts"][00]["body"]["data"].encode("UTF8"))
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
            return False,False


def set_order_info_in_db(order_infos, info_mail, winkel):
    if isinstance(order_infos, list):
        for order_info in order_infos:
            if (order_info[-1] is None or int(order_info[-1]) < 3) and winkel == "bol":
                set_order_info_db_bol(order_info, info_mail["tt_url"], info_mail["tt_num"])
                return True
            elif (order_info[-1] is None or int(order_info[-1]) < 3) and winkel == "blok":
                set_order_info_db_blokker(order_info, info_mail["tt_url"], info_mail["tt_num"])
                return True
            elif order_info[-1] >= 3:
                logger.info(f'stap 3 {info_mail["dienst"]} {info_mail}{order_info} order already processed')
                return True
            else:
                return False
    else:
        if (order_infos[-1] is None or int(order_infos[-1]) < 3) and winkel == "bol":
            set_order_info_db_bol(order_infos, info_mail["tt_url"], info_mail["tt_num"])
        elif (order_infos[-1] is None or int(order_infos[-1]) < 3) and winkel == "blok":
            set_order_info_db_blokker(order_infos, info_mail["tt_url"], info_mail["tt_num"])
        elif order_infos[-1] >= 3:
            logger.info(f'stap 3 {info_mail["dienst"]} {info_mail} {order_infos} order already processed')
        else:
            return False
        return True


def get_messages(conn, search_query, gewenste_aantal_dagen = aantal_dagen):
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
                        (info_mail["postcode"], info_mail["city"] + "%%",info_mail["house_number"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_city = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["city"] + "%%",info_mail["house_number"]),
                        "blok",
                    )
                )
                list_querys.append(
                    (
                        f"{info_bol_db} WHERE O.shipmentdetails_zipcode = %s AND O.shipmentdetails_firstname LIKE %s AND O.shipmentdetails_housenumber = %s {info_bol_db_end}",
                        (info_mail["postcode"], info_mail["first_name"] + "%%",info_mail["house_number"]),
                        "bol",
                    )
                )
                list_querys.append(
                    (
                        f"{info_blok_db} WHERE REPLACE(O.shipping_address_zip_code,' ','') = %s AND O.shipping_address_firstname = %s AND O.shipping_address_housenumber = %s {info_blok_db_end}",
                        (info_mail["postcode"], info_mail["first_name"] + "%%",info_mail["house_number"]),
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
    #dit omdat sommige dhl mails geen postcode hebben
    logger.info(f"begin processing_dhl {info_mail}")
    # first build all posible querys, from all mails for both stores, then check them and set value if match
    info_bol_db = "SELECT O.shipmentdetails_zipcode FROM orders_info_bol I LEFT JOIN orders_bol O ON I.orderid = O.orderid "
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



def mark_read(conn,message):
    conn.users().messages().modify(userId="me", id=message["id"], body={"removeLabelIds": ["UNREAD"]}).execute()  # mark mail read
    conn.users().messages().modify(userId="me", id=message["id"], body={"removeLabelIds": ["INBOX"]}).execute()

def add_label_processed_verzending(conn,message):
    conn.users().messages().modify(userId="me", id=message["id"], body={"addLabelIds": ["Label_8612133870834283528"]}).execute()

def add_label_processed_return(conn,message):
    conn.users().messages().modify(userId="me", id=message["id"], body={"addLabelIds": ["Label_1372612835680541088"]}).execute()

def process_bpost_messages(conn):
    # get send info exellent.
    message_treads_ids = get_messages(conn, 'from:(noreply@bpost.be) subject:("LANCKRIET" AROUND 2 "bpost") OR subject:("leveren" AROUND 3 "LANCKRIET") OR subject:("parcel" AROUND 2 deliver)')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "bpost",
                "tt_url": mail_body.xpath(
                    "//tr//a[contains(text(),'Details pakje')]/@href | //tr//*[contains(text(),'Details pakje')]//../@href | //tr//a[contains(text(),'Parcel details')]/@href | //tr//a[contains(text(),' colis')]/@href | //tr//a[contains(text(),'Pakje volgen')]/@href | //tr//a[contains(text(),'Track Parcel')]/@href"
                )[0],
                "tt_num": mail_body.xpath("//p[contains(text(),'Barcode')] | //p[contains(text(),'Code-barres')] | //p/span[contains(text(),'Barcode')]")[0].text.split(" ")[-1],
            }
            *_, mail_info["city"] = [
                x.strip().split(" ")[0] for x in re.split(",", mail_body.xpath("//td[contains(@class,'destination')]/div/p/strong")[0].text.lower())
            ]
            post_id = mail_info["tt_url"].split("itemCode=")[1].split("&")[0]
            bpost_api_info = requests.get(f"https://track.bpost.cloud/track/items?itemIdentifier={post_id}").json()["items"][0]
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
                del mail_info["city"] #omdat city wel eens wil afwijken, vreemde belgen...
                get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 bpost {mail_info} failed {e}")
    #delivered/afhaalpunt pakketten
    message_treads_ids = get_messages(conn, 'from:(noreply@bpost.be) subject:("Je pakje " pakjesautomaat|Afhaalpunt|geleverd)')
    for message_treads_id in message_treads_ids:
        mark_read(conn,message_treads_id)
        add_label_processed_verzending(conn,message_treads_id)


def process_dhl_messages(conn):
    # get send info Lanckriet/Exellent nederland.
    message_treads_ids = get_messages(conn, 'from:(noreply@dhlparcel.nl) subject:("We staan" AROUND 1 "voor de deur")  OR subject:("komen we bij je langs")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dhl",
                "tt_url": mail_body.xpath("//tbody//td//p[contains(text(),'pakket')]/..//a[string-length( text()) = 15]/@href | //tbody//td//p[contains(text(),'bezorger')]/..//a[string-length(text()) = 13]/@href")[0],
                "tt_num": mail_body.xpath("//tbody//td//p[contains(text(),'pakket')]/..//a[string-length( text()) = 15] | //tbody//td//p[contains(text(),'bezorger')]/..//a[string-length(text()) = 13]")[0].text,
            }
        except IndexError:
            logger.error("stap 3 dhl uitgevoerd voor een van de pakketten die voor ons zijn")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            add_label_processed_verzending(conn,message_treads_id)
            continue
        try:  # addres not in mail (and postcode also missing in first), and needs javascript..
            link_info_encoded = mail_info["tt_url"].split("/")[-1].encode("ascii")
            input_len = len(link_info_encoded)
            padding = b"=" * (3 - ((input_len + 3) % 4))
            plain_url = base64.b64decode(link_info_encoded + padding, altchars=b"-_")
            if "sorted" in str(plain_url):
                _,_, mail_info["first_name_search"], *mail_info["last_name_search"], _ = [
                    x.strip(",").strip() for x in re.split(" ", mail_body.xpath("//td//p[contains(text(),'Beste')]/text()")[0].lower())
                ]
                if mail_info["first_name_search"] in ("ten","de","het","van","van den", "van der", "van het"): # sommige doen het net omgedraaid, daarom checken, of tussenvoegsel waarschijndelijk is 
                    _,_, *mail_info["last_name_search"], mail_info["first_name_search"], _ = [
                    x.strip(",").strip() for x in re.split(" ", mail_body.xpath("//td//p[contains(text(),'Beste')]/text()")[0].lower())
                    ]
                mail_info["last_name_search"] = " ".join(filter(None,mail_info["last_name_search"]))
                if "video van gils b.v." in mail_info["last_name_search"]:
                    add_label_processed_verzending(conn,message_treads_id)
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
                trace_nr,postal_code = str(plain_url).split("/")[5],str(plain_url).split("?")[0].split("/")[-1]
            dhl_api_info = requests.get(f"https://api-gw.dhlparcel.nl/track-trace?key={trace_nr}%2B{postal_code}").json()[0]
            mail_info["first_name"], *mail_info["last_name"] = dhl_api_info["receiver"]["name"].split()
            if mail_info["first_name"].lower() in ("ten","de","het","van","van den", "van der", "van het"): # sommige doen het net omgedraaid, daarom checken, of tussenvoegsel waarschijndelijk is 
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
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dhl {mail_info} failed {e}")
    #delivered/afhaalpunt pakketten
    message_treads_ids = get_messages(conn, 'from:(noreply@dhlparcel.nl) subject:("Je pakket")')
    for message_treads_id in message_treads_ids:
        mark_read(conn,message_treads_id)
        add_label_processed_verzending(conn,message_treads_id)



def process_dynalogic_messages(conn):
    # get send info Exellent nederland.
    message_treads_ids = get_messages(conn, 'from:(noreply@dynalogic.eu) subject:("Wij komen eraan" OR "Afspraakbevestiging")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dynalogic",
                "tt_url": mail_body.xpath("//td//span[contains(text(),'Track & Trace')]/../@href | //td//span[contains(text(),'Mijn Afspraak')]/../@href")[0],
                "tt_num": mail_body.xpath("//td//span[contains(text(),'Track & Trace')]/../@href | //td//span[contains(text(),'Mijn Afspraak')]/../@href")[0].split("=")[-2].split("&")[0].replace("+", " "),
            }
        except IndexError:
            logger.error(f"stap 3 dynalogic failed, message id {message_treads_id}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            post_land_temp = mail_body.xpath("//span[normalize-space()='Afspraak details']/../text()")[3]
            if "B-" in post_land_temp:
                mail_info["postcode"], mail_info["city"] = post_land_temp.split(" ", 1)
                mail_info["postcode"] = mail_info["postcode"].replace("B-", "")
            else:
                mail_info["postcode_cijfers"],mail_info["postcode_letters"], *mail_info["city"] = post_land_temp.split(" ", 2)
                mail_info["postcode"] = mail_info["postcode_cijfers"]+mail_info["postcode_letters"]
                if len(mail_info["postcode_cijfers"]) > 4:
                    mail_info["postcode"], *mail_info["city"] = post_land_temp.split(" ", 2)
                mail_info["city"] = " ".join(mail_info["city"])
            dynalogic_api_info = requests.get(f"https://track.mydynalogic.eu/api/transportorder/full/ordernumber/{mail_info['tt_num']}/zipcode/{mail_info['postcode']}", headers={'referer': 'https://track.mydynalogic.eu/track/order','X-Requested-With': 'XMLHttpRequest'}).json()
            mail_info["order_num"] = str(dynalogic_api_info["data"]["OrderData"]["OrderNumber"])

            mail_info["company"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["Company"]
            if " " in mail_info["company"]:
                mail_info["first_name"],mail_info["last_name"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["Company"].split(" ", 1)
            else:
                mail_info["first_name"] = mail_info["company"]
            mail_info["house_number"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["HouseNumber"]
            mail_info["street"] = dynalogic_api_info["data"]["OrderData"]["Addressee"]["Street"]

            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dynalogic {mail_info} failed {e}")


def process_transmision_messages(conn):
    # get send info Exellent nederland.
    message_treads_ids = get_messages(conn, 'from:(expeditie@schuurman.nl) subject:("Schuurman")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        if mail_body.xpath("//td[contains(text(),'Kosten dropshipment')]"):
            drop = True
        else:
            drop = None
        try:
            mail_info = {
                "dienst": "transmision",
                "tt_url": mail_body.xpath("//a[contains(text(),'Link naar zendingstatus')]/@href")[0],
                "order_num": mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../../..//tr[3]/td[1]/text()")[0],
            }
            if (
                "_" not in mail_info["order_num"]
                and "-" not in mail_info["order_num"]
                and not drop
            ):
                logger.info(f"stap 3 transmission no order_nr bol/blokker, message id {message_treads_id}")
                conn.users().messages().modify(userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}).execute()
                continue
        except IndexError:
            logger.info(f"stap 3 transmission failed, message id {message_treads_id}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            page = requests.get(mail_info["tt_url"])
            page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
            mail_info["tt_num"] = page_body.xpath("//label[@title='Uniek zendingnummer bij TransMission']/../span[1]/text()")[0]
        except Exception:
            logger.error(f"getting t&t transmission failed, message id {message_treads_id}")
            continue

        try:
            mail_info["first_name"], *mail_info["last_name"] = [
                x.strip() for x in re.split(r"[ .]", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[1].lower(), 1)
            ]
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"], mail_info["house_number"], *_ = [
                x.strip() for x in re.split("(\d+)", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[2].lower())
            ]
            mail_info["postcode"], mail_info["city"] = [
                x.strip() for x in re.split(" ", mail_body.xpath("//b[contains(text(),'AFLEVERADRES:')]/../text()")[3].lower(), 1)
            ]
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 transmission {mail_info} failed {e}")


def process_gls_messages(conn):
    # get send info Amacom.
    message_treads_ids = get_messages(conn, 'from:(*@gls-group.eu) subject:"We hebben een pakje voor jou!"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        mail_info = {
            "dienst": "gls",
            "tt_url": mail_body.xpath("//h4[contains(text(),'Parcel nummer')]/../p/a/@href")[0],
        }
        try:
            if "gls-group.eu" in mail_info["tt_url"]:
                mail_info["tt_num"] = mail_info["tt_url"].split("=")[-1]
            elif "go.mygls.be" in mail_info["tt_url"]:
                mail_info["tt_num"] = mail_info["tt_url"].split("/")[-1]
            _, mail_info["first_name"], mail_info["last_name"] = [
                x.strip(",")
                for x in re.split(
                    "^(\w+)\s",
                    mail_body.xpath("//h3[normalize-space()='Bezorgadres']/../..//p//text()")[0].strip().lower(),
                )
            ]
            _, mail_info["street"], *_, mail_info["city"], _ = [
                x.strip(",")
                for x in re.split(
                    "^(\w+)\s(\d).+\s(\d+)\s(\w+)",
                    mail_body.xpath("//h3[normalize-space()='Bezorgadres']/../..//p//text()")[1].strip().lower(),
                )
            ]
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 gls {mail_info} failed {e}")


def process_dpd_messages(conn):
    # get send info difox
    message_treads_ids = get_messages(conn, 'from:(*@difox.com) subject:"Verzend informatie"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dpd",
                "tt_url": mail_body.xpath("//a[normalize-space()='Zending volgen']/@href")[0],
                "tt_num": mail_body.xpath("//td[contains(text(),'Paket')]")[0].text.split(":")[-1].split("-")[0].strip(),
            }
        except (IndexError, ValueError) as e:
            logger.info(f"stap 3 dpd failed because of no tt_link, message id {message_treads_id} {e}")
            add_label_processed_verzending(conn,message_treads_id)
            continue
        try:
            *_, mail_info["order_num"], _ = [
                x.strip().strip(",") for x in re.split("(.*)bestelnr: (\w+)", mail_body.xpath("//p[contains(text(),'Referentienummer:')]/text()")[0].lower())
            ]
        except (IndexError, ValueError) as e:
            logger.info(f"stap 3 dpd failed because of local delivery to us, message id {message_treads_id} {e}")
            conn.users().messages().modify(
                userId="me", id=message_treads_id["id"], body={"addLabelIds": ["Label_8612133870834283528"]}
            ).execute()  # verwerkte mail
            continue
        try:
            mail_info["full_name"], mail_info["street_nr"], mail_info["county_post_city"] = [
                x.strip() for x in re.split(",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower())
            ]
        except (IndexError, ValueError) as e:
            mail_info["full_name"] = mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower().strip()
            try:
                mail_info["street_nr"], mail_info["county_post_city"] = [
                    x.strip() for x in re.split(",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[3]")[0].lower())
                ]
            except (IndexError, ValueError) as e:
                *mail_info["full_name"],mail_info["street_nr"], mail_info["county_post_city"] = [
                    x.strip() for x in re.split(",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower())
                ]
                mail_info["full_name"] = " ".join(mail_info["full_name"])
        try:
            mail_info["first_name"] = mail_info["full_name"].split(" ", 1)[0]
            if len(mail_info["county_post_city"]) < 5:
                # sometime info on next line
                mail_info["full_name"], mail_info["street_nr"], _ = [
                    x.strip() for x in re.split(",", mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[2]")[0].lower())
                ]
                mail_info["county_post_city"] = mail_body.xpath("//p[contains(text(),'Referentienummer:')]//text()[3]")[0].lower()

            *_, mail_info["postcode"], mail_info["city"], _ = re.split("(\w+-)(\d+\w+)\s(.+)", mail_info["county_post_city"])
            try:
                _, mail_info["street"], mail_info["house_number"], _ = re.split("(\D+) (\d+.*)", mail_info["street_nr"])
            except (IndexError, ValueError) as e:
                logger.info(f"amazon ? {e}")
                mark_read(conn,message_treads_id)
                add_label_processed_verzending(conn,message_treads_id)
                continue
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dpd {mail_info} failed {e}")

    # get send info excellent, so different that i need to do it separate
    message_treads_ids = get_messages(conn, 'from:(*@dpd.nl | *@dpd.be ) subject:("Je pakket")')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        try:
            mail_info = {
                "dienst": "dpd",
                "tt_url": mail_body.xpath("//img[@alt='Volg je pakket']/../@href | //img[@style='float:right']/../@href")[0],
            }
        except (IndexError, ValueError) as e:
            logger.info(f"stap 3 dpd failed because of no tt_link(zoals in bezorgd links), message id {message_treads_id} {e}")
            add_label_processed_verzending(conn,message_treads_id)
            continue
        try:
            session = requests.Session()
            page = session.get(mail_info['tt_url'])
            page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
            try:
                mail_info["tt_num"] = page_body.xpath("//p[normalize-space()='DPD-pakketnummer:']/..//span/text() | //p[normalize-space()='DPD zendingsnummer:']/..//span/text()")[0]
            except (IndexError, ValueError) as e:
                try:
                    crfs = page_body.xpath("//meta[@name='_csrf']/@content")[0]
                    mail_info["tt_num"] = ''.join(char for char in mail_body.xpath("//span[contains(text(),'Uw bestelling met pakketnummer')]/text()")[0] if char in '0123456789') 
                    postalcode = mail_body.xpath("//span[contains(text(),'*:')]/..//span/text()[last()]")[0].strip().split(" ",1)[0]
                    data = {
                        '_csrf': crfs,
                        'parcelType': 'INCOMING',
                        'verificationCode': postalcode,
                        'recaptchaResponse': '',
                        'number': mail_info["tt_num"],
                        'shipmentType': 'PARCEL_DETAILS',
                        'validate': 'Bevestigen'
                    }
                    session.post("https://www.dpdgroup.com/be/mydpd/my-parcels/details/protection", data=data)
                    page = session.get(f"https://www.dpdgroup.com/be/mydpd/my-parcels/incoming?parcelNumber={mail_info['tt_num']}")
                    page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
                except (IndexError, ValueError) as e:
                    mail_info["tt_num"] = page_body.xpath("//span[@class='parcelAlias']/../span/span//text()")[0]
                    add_label_processed_verzending(conn,message_treads_id)
                    continue
            mail_info["full_name"] = page_body.xpath("//p[normalize-space()='Naar:']/../p[2]/text()")[0]
            mail_info["street_house"] = page_body.xpath("//p[normalize-space()='Naar:']/../p[3]/text()")[0]
            mail_info["postcode_plaats"] = page_body.xpath("//p[normalize-space()='Naar:']/../p[4]/text()")[0]
            mail_info["first_name"], *mail_info["last_name"] = mail_info["full_name"].lower().split(" ", 1)
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            *mail_info["street"], mail_info["house_number"] = mail_info["street_house"].split(" ")
            mail_info["street"] = " ".join(mail_info["street"])
            mail_info["postcode"], *mail_info["city"] = re.split(" ", mail_info["postcode_plaats"])
            mail_info["city"] = " ".join(mail_info["city"])
            mail_info["land"] = page_body.xpath("//p[normalize-space()='Naar:']/../p[5]/text()")
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 dpd {mail_info} failed {e}")


def process_postnl_ur_messages(conn):
    # get send info united retail.
    message_treads_ids = get_messages(conn, 'from:(*@vangilsweb.nl) "Uw bestelling is verzonden"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        mail_info = {
            "dienst": "united",
            "tt_url": mail_body.xpath("//*[contains(text(),'Volg mijn ')]/../a/@href")[0],
        }
        try:
            mail_info["tt_num"] = mail_info["tt_url"].split("/")[-1].split("-")[0]
            postnl_api_info = requests.get(f"https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/{mail_info['tt_url'].split('/')[-1]}?language=nl").json()["colli"].get(mail_info["tt_num"])
            if postnl_api_info["recipient"]["names"].get("personName"):
                mail_info["first_name"], *mail_info["last_name"] = postnl_api_info["recipient"]["names"].get("personName").split()
            elif postnl_api_info["recipient"]["names"].get("companyName"):
                mail_info["first_name"], *mail_info["last_name"] = postnl_api_info["recipient"]["names"].get("companyName").split()
            else: #sometimes, name not processed bij postnl,so use from mail
                _, mail_info["first_name"], *mail_info["last_name"] = [
                x.strip().strip(",") for x in re.split("^beste\s+(\w+)", mail_body.xpath("//strong[contains(text(),'Beste ')]")[0].text.lower())
            ]
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = postnl_api_info["recipient"]["address"]["street"] 
            mail_info["house_number"] = postnl_api_info["recipient"]["address"]["houseNumber"]
            mail_info["postcode"] = postnl_api_info["recipient"]["address"]["postalCode"] 
            mail_info["city"] = postnl_api_info["recipient"]["address"]["town"] 
            get_order_info_db = get_set_info_database(mail_info)
            if get_order_info_db:
                mark_read(conn,message_treads_id)
            add_label_processed_verzending(conn,message_treads_id)
        except Exception as e:
            logger.error(f"stap 3 united {mail_info} failed {e}")

def process_beekman_messages(conn):
    from requests_html import HTMLSession
    message_treads_ids = get_messages(conn, 'from:(*@beekman.nl) "bevestiging voor zending"')
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        mail_body = get_body_email(message)
        mail_info = {
            "dienst": "beekman",
            "beekman_url": mail_body.xpath("//a[normalize-space()='www.beekman.nl']/@href")[0],
        }
        if mail_body.xpath("//strong[contains(text(),'Audio Video Van Gils B.V.')]"):
            mark_read(conn,message_treads_id)
            continue
        session = HTMLSession()
        page = session.get("https://www.beekman.nl/inloggen")
        inlog_tokens = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
        token = inlog_tokens.xpath("//input[@name='_token']/@value")[0]
        hfh = inlog_tokens.xpath("//input[@name='hfh[]']/@value")[0]
        data = {
            '_token': token,
            'hfh[]': hfh,
            'username': config["beekman"].get("username"),
            'password': config["beekman"].get("password"),
        }
        session.post("https://www.beekman.nl/inloggen", data=data)
        js_page = session.get(mail_info["beekman_url"])
        tt_page = etree.parse(io.BytesIO(js_page.content), etree.HTMLParser())
        postorg = tt_page.xpath("//td[@class='text-left']//text()")[0]
        mail_info["tt_url"] = tt_page.xpath("//a[normalize-space()='Klik hier om uw zending te volgen']/@href")[0]
        if postorg == "PostNL":
            mail_info["tt_num"] = mail_info["tt_url"].split("/")[-1].split("-")[0]
            postnl_api_info = requests.get(f"{mail_info['tt_url'].replace('track-and-trace','track-and-trace/api/trackAndTrace')}?language=nl").json()["colli"].get(mail_info["tt_num"])
            if postnl_api_info["recipient"]["names"].get("personName"):
                mail_info["first_name"], *mail_info["last_name"] = postnl_api_info["recipient"]["names"].get("personName").split()
            elif postnl_api_info["recipient"]["names"].get("companyName"):
                mail_info["first_name"], *mail_info["last_name"] = postnl_api_info["recipient"]["names"].get("companyName").split()
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = postnl_api_info["recipient"]["address"]["street"] 
            mail_info["house_number"] = postnl_api_info["recipient"]["address"]["houseNumber"]
            mail_info["postcode"] = postnl_api_info["recipient"]["address"]["postalCode"] 
            mail_info["city"] = postnl_api_info["recipient"]["address"]["town"] 
        if postorg == "DHL Parcel":
            postal_code = tt_page.xpath("//address//text()[3]")[0].strip().split(" ",1)[0]
            mail_info["tt_num"] = re.split(r'[=&]', mail_info['tt_url'])[1]
            dhl_api_info = requests.get(f"https://api-gw.dhlparcel.nl/track-trace?key={mail_info['tt_num']}%2B{postal_code}").json()[0]
            mail_info["first_name"], *mail_info["last_name"] = dhl_api_info["receiver"]["name"].split()
            if mail_info["first_name"].lower() in ("ten","de","het","van","van den", "van der", "van het"): # sommige doen het net omgedraaid, daarom checken, of tussenvoegsel waarschijndelijk is 
                *mail_info["last_name"], mail_info["first_name"] = dhl_api_info["receiver"]["name"].split()
            mail_info["last_name"] = " ".join(mail_info["last_name"])
            mail_info["street"] = dhl_api_info["receiver"]["address"]["street"]
            mail_info["house_number"] = dhl_api_info["receiver"]["address"]["houseNumber"]
            mail_info["postcode"] = dhl_api_info["receiver"]["address"]["postalCode"]
            mail_info["city"] = dhl_api_info["receiver"]["address"]["city"]
        get_order_info_db = get_set_info_database(mail_info)
        if get_order_info_db:
            mark_read(conn,message_treads_id)
        add_label_processed_verzending(conn,message_treads_id)
       


def gmail_send_mail(conn,order_id,kvk_winkel,bol_email,waarvoor): #meerdere bol winkel onder zelfde kvk, maar kvk bepaald de layout
    """Create and insert a draft email.
       Print the returned draft's message and id.
       Returns: Draft object, including draft id and message meta data."""
    try:
        #html text for mail
        standaard_begin_text = "Beste klant,<br><br>Bedankt voor uw bestelling!<br><br>"
        standaard_eind_text = "<br><br><br>Met vriendelijke groet,<br><br>Uw Support-team:<br>"
        begin_levertijd = "Indien u dit voor"
        einde_levertijd = "door zou willen geven, kunnen wij uw pakket nog tegen houden voor verzending indien dit nodig is."

        onderwerpen = {
            "waterreservoir" : "U heeft bij ons een waterreservoir besteld voor uw koffiezetapparaat. Nu blijkt uit ervaring dat er vaak wat onduidelijkheid heerst over het type reservoir dat er nodig is.<br> Mocht u twijfelen of dat u het juiste exemplaar heeft besteld willen wij u vragen om een foto te maken van het typeplaatje dat op uw apparaat staat. In sommige gevallen zit er geen typeplaatje op het apparaat maar staat het typenummer in het apparaat zelf gedrukt. <br> <br>",
            "padhouder" : "U heeft bij ons een padhouder besteld voor uw koffiezetapparaat. Nu blijkt uit ervaring dat er vaak wat onduidelijkheid heerst over het type padhouder dat er nodig is.<br> Mocht u twijfelen of dat u het juiste exemplaar heeft besteld willen wij u vragen om een foto te maken van het typeplaatje dat op uw apparaat staat. In sommige gevallen zit er geen typeplaatje op het apparaat maar staat het typenummer in het apparaat zelf gedrukt. <br> <br>"
        }
        voor_welke_type = onderwerpen.get(waarvoor)
        message = EmailMessage()
        message['To'] = bol_email


        image_cid = make_msgid(domain=f"{kvk_winkel}.nl")
        if kvk_winkel == "toopbv":
            message['From'] = 'toopbv@gmail.com'
            message.add_alternative(f"<html><body>{standaard_begin_text}{voor_welke_type}{begin_levertijd} 3 uur in de middag {einde_levertijd}{standaard_eind_text}<img src='cid:{image_cid[1:-1]}' alt='logo toop'></body></html>", subtype='html')
            sign_picture = "toop.png"
        elif kvk_winkel == "vangilsweb":
            message['From'] = 'info@vangilsweb.nl'
            message.add_alternative(f"<html><body>{standaard_begin_text}{voor_welke_type}{begin_levertijd} 9 uur `s ochtends {einde_levertijd}{standaard_eind_text}<img src='cid:{image_cid[1:-1]}' alt='logo van gils'></body></html>", subtype='html')
            sign_picture = "vangils.jpg"

        if waarvoor in ("waterreservoir","padhouder"):
            with open("water_locatie_info.png", 'rb') as img:
                maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
                message.get_payload()[0].add_related(img.read(), 
                                                    maintype=maintype, 
                                                    subtype=subtype, 
                                                    cid=image_cid,
                                                    filename="afbeelding_met_uitleg")

        with open(sign_picture, 'rb') as img:
            # know the Content-Type of the image
            maintype, subtype = mimetypes.guess_type(img.name)[0].split('/')
            message.get_payload()[0].add_related(img.read(), 
                                                maintype=maintype, 
                                                subtype=subtype, 
                                                cid=image_cid,
                                                filename="signature")

        message['Subject'] = f'Bestelling {waarvoor} {order_id}'
        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        create_message = {
            'raw': encoded_message
        }
        send_message = (conn.users().messages().send(userId="me", body=create_message).execute())
        logger.info(f"sending succesful {send_message['id']}")
        return True

    except HttpError as e:
        logger.error(f"sending failed {e}")
        send_message = None
        return False


def process_bol_orders(conn,product_type,zoek_string):
    message_treads_ids = get_messages(conn, f'to:*@vangilsweb.nl OR to:*@toopbv.nl subject:"Nieuwe bestelling:" {zoek_string}',gewenste_aantal_dagen = "2d")
    for message_treads_id in message_treads_ids:
        message = conn.users().messages().get(userId="me", id=message_treads_id["id"]).execute()
        headers=message["payload"]["headers"]
        order_nr = re.search('\d+',[i['value'] for i in headers if i["name"]=="Subject"][0].rsplit(":")[-1]).group()
        try:
            _,webwinkel,to,_ = re.split("[@ .]",[i['value'] for i in headers if i["name"]=="To"][0])
        except ValueError as e:
            webwinkel,to,_ = re.split("[@ .]",[i['value'] for i in headers if i["name"]=="To"][0])
        winkel = {
            "alldayelektro" : "_ADE",
            "info":"_TB",
            "tpshopper":"_TS",
            "typischelektro":"_TE"
        }
        winkel_short = winkel.get(webwinkel)
        odin_order_nr = f"{order_nr}{winkel_short}"
        try:
            with engine.begin() as db_conn:
                bol_email_addres = db_conn.exec_driver_sql("SELECT shipmentdetails_email FROM orders_bol WHERE orderid LIKE %s",(odin_order_nr,)).first()[0]
            send = gmail_send_mail(conn,odin_order_nr,to,bol_email_addres,product_type)
        except Exception as e:
            send = False
            logger.error(f"order_id not found {odin_order_nr} {e}")
        if send:
            add_label_processed_return(conn,message_treads_id)

if __name__ == "__main__":
    credentials = get_autorisation_gooogle_api()
    connection = gmail_create_connection(credentials)
    process_bpost_messages(connection)
    process_dhl_messages(connection)
    process_dynalogic_messages(connection)
    process_transmision_messages(connection)
    process_gls_messages(connection)
    process_dpd_messages(connection)
    process_postnl_ur_messages(connection)
    process_beekman_messages(connection)

    # # auto replay on bol, sommige bol mailtje automatisch beantwoorden, om het aantal retouren te verminderen
    process_bol_orders(connection,product_type="waterreservoir",zoek_string="Dolce Gusto Waterreservoir")
    process_bol_orders(connection,product_type="padhouder",zoek_string="padhouder")