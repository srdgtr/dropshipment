import subprocess
import sys
import configparser
import logging
import datetime
import io
from lxml import etree


from pathlib import Path
import time

def install(package):
    subprocess.call([sys.executable, "-m", "pip", "install", package])

import requests
from sqlalchemy import (MetaData, Table, and_, create_engine, select, text,
                        update)
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

metadata = MetaData()

logger = logging.getLogger("process_gmail")
logging.basicConfig(
    filename="process_dropships_" + datetime.date.today().strftime("%V") + ".log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)  # nieuwe log elke week
logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)


def order_send_into_uploaded_to_bol(orderid,orderitemid):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
    logger.info(f"start stap 3 bol {orderid}")
    drop_send = (
        update(orders_info_bol)
        .where(and_(orders_info_bol.columns.orderid == orderid, orders_info_bol.columns.order_orderitemid == orderitemid))
        .values(order_droped_tt_to_bol=1)
    )
    with engine.begin() as conn:
        conn.execute(drop_send)

#blokker nog een keer regelen
# def set_order_info_db_blokker(order_info, track_en_trace_url, track_en_trace_num):
#     orders_info_blokker = Table("blokker_order_items", metadata, autoload_with=engine)
#     logger.info(f"start stap 3 blokker {order_info}")
#     drop_send = (
#         update(orders_info_blokker)
#         .where(orders_info_blokker.columns.order_line_id == order_info[0])
#         .values(dropship="3", t_t_dropshipment=track_en_trace_url, order_id_leverancier=track_en_trace_num)
#     )
#     with engine.begin() as conn:
#         conn.execute(drop_send)

class BOL_API:
    host = None
    key = None
    secret = None
    access_token = None
    access_token_expiration = None

    def __init__(self, host, key, secret):
        # set init values on creation
        self.host = host
        self.key = key
        self.secret = secret

        try:
            self.access_token = self.getAccessToken()
            if self.access_token is None:
                raise ValueError("Request for access token failed.")
        except ValueError as e:
            print(e)
        else:
            self.access_token_expiration = time.time() + 220

    def getAccessToken(self):
        # request the JWT
        try:
            # request an access token
            init_request = requests.post(self.host, auth=(self.key, self.secret))
            init_request.raise_for_status()
        except Exception as e:
            print(e)
            return None
        else:
            token = init_request.json()["access_token"]
            if token:  # add right headers
                post_header = {
                    "Accept": "application/vnd.retailer.v9+json",
                    "Content-Type": "application/vnd.retailer.v9+json",
                    "Authorization": "Bearer " + token,
                    "Connection": "keep-alive",
                }
            return post_header

    class Decorators:
        @staticmethod
        def refreshToken(decorated):
            # check the JWT and refresh if necessary
            def wrapper(api, *args, **kwargs):
                if time.time() > api.access_token_expiration:
                    api.access_token = api.getAccessToken()
                return decorated(api, *args, **kwargs)

            return wrapper

niet_verwerkte_bol_dropship_orders = "SELECT I.orderid,I.order_orderitemid,O.shipmentdetails_zipcode,I.dropship,I.order_id_leverancier,I.t_t_dropshipment FROM orders_info_bol I LEFT JOIN orders_bol O ON I.orderid = O.orderid WHERE I.t_t_dropshipment > 1 < 4 AND I.order_droped_tt_to_bol IS NULL AND O.active_order = 1 ORDER BY O.updated_on DESC"

def send_request_shiping_info_to_bol(self, verzender_drop, tt_num, bol_order_item,order_id):
    url = "https://api.bol.com/retailer/orders/shipment"
    transport_info_dict_bol = {
        "orderItems": [{"orderItemId": bol_order_item}],
        "shipmentReference": None,
        "transport": {"transporterCode": verzender_drop, "trackAndTrace": tt_num},
    }
    response = requests.request("PUT", url, headers=self.access_token, json=transport_info_dict_bol)
    if response.status_code == 202:
        url = f"https://api.bol.com/shared/process-status/{response.json()['processStatusId']}"
        for _ in range(10):
            time.sleep(2)
            result = requests.request("GET", url, headers=self.access_token).json()
            if result["status"] == "SUCCESS":
                order_send_into_uploaded_to_bol(order_id,bol_order_item)
                break
            elif result["status"] == "FAILURE":
                logger.error(f"no succes message {result['errorMessage']} {url}")
                break
    else:
        print(f"no succes message {response.text}")


bol_at_depot = []

with engine.connect() as connection:
    verzending_open_bol = connection.exec_driver_sql(niet_verwerkte_bol_dropship_orders)
    for order in verzending_open_bol:
        order_dict = dict(order._mapping)
        if "dhl" in order_dict['t_t_dropshipment']:
            shipment_info = requests.get(f"https://api-gw.dhlparcel.nl/track-trace?key={order_dict['order_id_leverancier']}%2B{order_dict['shipmentdetails_zipcode']}").json()[0]
            shipment_on_depot = any(event.get('status') == "PARCEL_ARRIVED_AT_LOCAL_DEPOT" for event in shipment_info['events'])
            if shipment_on_depot:
                order_dict["verzendpartner"] = "DHL"
                bol_at_depot.append(order_dict)
        elif "postnl" in order_dict['t_t_dropshipment']:
            shipment_info = requests.get(f"https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/{order_dict['t_t_dropshipment'].split('/')[-1]}?language=nl").json()
            observations = shipment_info.get("colli", {}).get(order_dict['order_id_leverancier'], {}).get("observations", [])
            status = shipment_info.get("colli", {}).get(order_dict['order_id_leverancier'], {}).get("statusPhase", []).get("message") == "Zending is gesorteerd"
            shipment_on_depot = any(observation.get("description") == "Zending is gesorteerd" for observation in observations)
            if shipment_on_depot or status:
                order_dict["verzendpartner"] = "TNT"
                bol_at_depot.append(order_dict)
        elif "dpd" in order_dict['t_t_dropshipment']:
            shipment_info = requests.get(f"https://tracking.dpd.de/rest/plc/nl_NL/{order_dict['order_id_leverancier']}").json()
            shipment_on_depot = any(status["status"] == "AT_DELIVERY_DEPOT" for status in shipment_info["parcellifecycleResponse"]["parcelLifeCycleData"]["statusInfo"])
            if shipment_on_depot or status:
                order_dict["verzendpartner"] = "DPD-NL"
                bol_at_depot.append(order_dict)
        elif "trans-mission" in order_dict['t_t_dropshipment']:
            page = requests.get(order_dict['t_t_dropshipment'])
            page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
            shipment_on_depot = next((text for text in page_body.xpath("//td[contains(text(),'In bestelling')]/text()|//td[contains(text(),'Aflever Scan')]/text()") if any(keyword in text for keyword in ["In bestelling", "Aflever Scan"])), None)
            if shipment_on_depot:
                order_dict["verzendpartner"] = "TRANSMISSION"
                bol_at_depot.append(order_dict)
            else:
                logger.info(f"nog niet verwerkt door transmission,{order_dict['t_t_dropshipment']}")


def custom_sort(item):
    return item['orderid'][-2:]

bol_at_depot_sorted = sorted(bol_at_depot, key=custom_sort)

winkel = {"all_day_elektro": "ADE", "toop_bv": "TB", "tp_shopper": "TS", "typisch_elektro": "TE"}
config = configparser.ConfigParser()
config.read(Path.home() / "bol_export_files.ini")

def send_info_bol():
    for order in bol_at_depot:
        for shop, short_shop in winkel.items():
            if short_shop == order["orderid"].split("_")[-1]:
                client_id, client_secret, _, _ = [x.strip() for x in config.get("bol_winkels_api", shop).split(",")]
        bol_auth = BOL_API(config["bol_api_urls"]["authorize_url"], client_id, client_secret)
        send_request_shiping_info_to_bol(bol_auth, order["verzendpartner"], order["order_id_leverancier"], order["order_orderitemid"],order["orderid"])

send_info_bol()

