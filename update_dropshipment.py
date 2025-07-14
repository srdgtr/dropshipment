import asyncio
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


sys.path.insert(0, str(Path.home()))


config = configparser.ConfigParser()

try:
    import httpx
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("httpx")
    import httpx

try:
    from asynciolimiter import StrictLimiter
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("asynciolimiter")
    from asynciolimiter import StrictLimiter

try:
    from sqlalchemy import MetaData, Table, and_, create_engine, update
    from sqlalchemy.engine.url import URL
except ModuleNotFoundError as e:
    print(e, "trying to install")
    install("sqlalchemy")
    from sqlalchemy import MetaData, Table, and_, create_engine, update
    from sqlalchemy.engine.url import URL


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


def order_send_into_uploaded_to_bol(orderid, orderitemid):
    orders_info_bol = Table("orders_info_bol", metadata, autoload_with=engine)
    logger.info(f"start stap 3 bol {orderid}")
    drop_send = (
        update(orders_info_bol)
        .where(
            and_(
                orders_info_bol.columns.orderid == orderid,
                orders_info_bol.columns.order_orderitemid == orderitemid,
            )
        )
        .values(order_droped_tt_to_bol=1)
    )
    with engine.begin() as conn:
        conn.execute(drop_send)

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
            init_request = httpx.post(self.host, auth=(self.key, self.secret))
            init_request.raise_for_status()
        except Exception as e:
            print(e)
            return None
        else:
            token = init_request.json()["access_token"]
            if token:  # add right headers
                post_header = {
                    "Accept": "application/vnd.retailer.v10+json",
                    "Content-Type": "application/vnd.retailer.v10+json",
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


async def send_request_shiping_info_to_bol(self, verzender_drop, tt_num, bol_order_item, order_id, rate_limit):
    await rate_limit.wait()
    async with httpx.AsyncClient() as client:
        url = "https://api.bol.com/retailer/shipments"
        transport_info_dict_bol = {
            "orderItems": [{"orderItemId": bol_order_item}],
            "shipmentReference": None,
            "transport": {"transporterCode": verzender_drop, "trackAndTrace": tt_num},
        }
        response = await client.post(url, headers=self.access_token, json=transport_info_dict_bol)
        if response.status_code == 202:
            url = f"https://api.bol.com/shared/process-status/{response.json()['processStatusId']}"
            for _ in range(20):
                response = await client.get(url, headers=self.access_token)
                response_json = response.json()
                if response_json['status'] == 'SUCCESS':
                    order_send_into_uploaded_to_bol(order_id, bol_order_item)
                    return
                elif response_json['status'] == 'FAILURE':
                    logger.error(f"no succes message {response_json['errorMessage']} {url}")
                    return
                await asyncio.sleep(15)
        else:
            print(f"no succes message {response.text}")

bol_at_depot = []

with engine.connect() as connection:
    verzending_open_bol = connection.exec_driver_sql(niet_verwerkte_bol_dropship_orders)
    for order in verzending_open_bol:
        order_dict = dict(order._mapping)
        if "dhl.com" in order_dict["t_t_dropshipment"]:
            if datetime.datetime.now().minute < 5: # limit of 250 in api calls, so only check 1 time an hour
                headers = {
                    'accept': 'application/json',
                    'DHL-API-Key': config['dhl_api']['key']
                }
                shipment_info = httpx.get(
                    f"https://api-eu.dhl.com/track/shipments?trackingNumber={order_dict['order_id_leverancier']}&language=en&requesterCountryCode=NL&source=tt", headers=headers
                ).json()
                shipments = shipment_info.get('shipments', [])
                shipment_on_depot = any(
                    event.get('statusDetailed') == 'MVARR_NRQRD_PO'
                    for shipment in shipments
                    for event in shipment.get('events', [])
                )
                if shipment_on_depot:
                    order_dict["verzendpartner"] = "DHL_DE"
                    bol_at_depot.append(order_dict)
        if "dhlparcel" in order_dict["t_t_dropshipment"]:
            shipment_info = httpx.get(
                f"https://api-gw.dhlparcel.nl/track-trace?key={order_dict['order_id_leverancier']}%2B{order_dict['shipmentdetails_zipcode']}"
            ).json()[0]
            shipment_on_depot = any(
                event.get("status") == "PARCEL_ARRIVED_AT_LOCAL_DEPOT" for event in shipment_info["events"]
            )
            if (
                datetime.date.today()
                == datetime.datetime.strptime(shipment_info.get("plannedDeliveryTimeframe")[:10], "%Y-%m-%d").date()
                if shipment_info.get("plannedDeliveryTimeframe")
                else None
            ):
                shipment_on_depot = True
            if shipment_on_depot:
                order_dict["verzendpartner"] = "DHL"
                bol_at_depot.append(order_dict)
        elif "postnl" in order_dict["t_t_dropshipment"]:
            shipment_info = httpx.get(
                f"https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/{order_dict['t_t_dropshipment'].split('/')[-1].strip()}?language=nl"
            )
            if shipment_info.status_code == 500:
                shipment_info = httpx.get(
                    f"https://jouw.postnl.nl/track-and-trace/api/trackAndTrace/{order_dict['order_id_leverancier']}-{'NL' if 'NL' in order_dict['t_t_dropshipment'] else 'BE' }-{order_dict['shipmentdetails_zipcode']}?language=nl"
                )
            observations = (
                shipment_info.json()
                .get("colli", {})
                .get(order_dict["order_id_leverancier"].upper(), {})
                .get("observations", [])
            )
            status = shipment_info.json().get("colli", {}).get(order_dict["order_id_leverancier"].upper(), {}).get(
                "statusPhase", {}
            ).get("message") in [
                "Pakket is bezorgd",
                "Zending is gesorteerd",
                "Bezorger is onderweg",
                "Zending is bezorgd in de brievenbus",
            ]
            shipment_on_depot = any(
                observation.get("description") == "Zending is gesorteerd" for observation in observations
            )
            if shipment_on_depot or status:
                order_dict["verzendpartner"] = "TNT"
                bol_at_depot.append(order_dict)
        elif "trans-mission" in order_dict["t_t_dropshipment"]:
            page = httpx.get(order_dict["t_t_dropshipment"])
            page_body = etree.parse(io.BytesIO(page.content), etree.HTMLParser())
            shipment_on_depot = next(
                (
                    text
                    for text in page_body.xpath(
                        "//h6[normalize-space()='Aflevering']/../h4/text()"
                    )
                    if any(keyword in text for keyword in ["Verwacht tussen", "Afgeleverd op"])
                ),
                None,
            )
            if shipment_on_depot:
                order_dict["verzendpartner"] = "TRANSMISSION"
                bol_at_depot.append(order_dict)
            else:
                logger.info(f"nog niet verwerkt door transmission,{order_dict['t_t_dropshipment']}")
        elif "dynalogic" in order_dict["t_t_dropshipment"]:
            headers = {
                "Referer": "https://track.mydynalogic.eu/track/order",
                "X-Auth-Token": "dyna:6507f86f6f3a1",
                "X-Requested-With": "XMLHttpRequest",
            }
            response = httpx.get(
                f"https://track.mydynalogic.eu/api/transportorder/full/ordernumber/{order_dict['t_t_dropshipment'].split('=')[-1]}/zipcode/{order_dict['shipmentdetails_zipcode']}",
                headers=headers,
            )
            if response.headers.get("Content-Type").startswith("application/json"):
                shipment_info = response.json()
                if len(order_dict["order_id_leverancier"]) > 1:
                    order_dict["order_id_leverancier"] = order_dict["order_id_leverancier"].split(" ", 1)[-1]
                active_step = shipment_info.get("data", {}).get("ActiveStep")
                if active_step is not None and active_step >= 3:
                        order_dict["verzendpartner"] = "DYL"
                        bol_at_depot.append(order_dict)
        elif "gls" in order_dict["t_t_dropshipment"]:
            response = httpx.get(
                f"https://gls-group.eu/app/service/open/rest/GROUP/en/rstt029?match={order_dict['order_id_leverancier']}"
            )
            shipment_info = response.json()
            if shipment_info["tuStatus"][0]["progressBar"]["colourIndex"] >= 3:
                order_dict["verzendpartner"] = "GLS"
                bol_at_depot.append(order_dict)
            else:
                logger.info(f"nog niet verwerkt door gls,{order_dict['t_t_dropshipment']}")
        # elif "dpd" in order_dict["t_t_dropshipment"]:
        #     time.sleep(10) # prevent 429 errors, by slowing down
        #     if len(order_dict["order_id_leverancier"]) == 14:
        #         response = httpx.get(f"https://extranet.dpd.de/rest/plc/nl_NL/{order_dict['t_t_dropshipment'].split('=')[-1]}")
        #     else:
        #         response = httpx.get(f"https://extranet.dpd.de/rest/plc/nl_NL/{order_dict['t_t_dropshipment'].split('/')[-1][:-1]}")
        #     if response.status_code == 429:
        #         break
        #     if response.headers.get("Content-Type").startswith("application/json"):
        #         shipment_info = response.json()
        #         try:
        #             status_info = (
        #                 shipment_info.get("parcellifecycleResponse").get("parcelLifeCycleData").get("statusInfo")
        #             )
        #             shipment_on_depot = any(status["status"] == "AT_DELIVERY_DEPOT" for status in status_info)
        #         except AttributeError:
        #             # if datetime.datetime.now().time() >= datetime.time(10, 0): # voor drukke periodes auto afmelden
        #             #     shipment_on_depot = True
        #             # else:
        #             shipment_on_depot = None
        #         if shipment_on_depot:
        #             order_dict["verzendpartner"] = "DPD-NL"
        #             bol_at_depot.append(order_dict)
        #     else:
        #         logger.info(f"niet bekend bij api dpd,{order_dict['t_t_dropshipment']}")


def custom_sort(item):
    return item["orderid"][-2:]


bol_at_depot_sorted = sorted(bol_at_depot, key=custom_sort)

winkel = {
    "all_day_elektro": "ADE",
    "toop_bv": "TB",
    "tp_shopper": "TS",
    "typisch_elektro": "TE",
}
config = configparser.ConfigParser()
config.read(Path.home() / "bol_export_files.ini")

async def send_info_bol():
    tasks = []
    rate_limit = StrictLimiter(25)
    for order in bol_at_depot:
        for shop, short_shop in winkel.items():
            if short_shop == order["orderid"].split("_")[-1]:
                client_id, client_secret, _, _ = [x.strip() for x in config.get("bol_winkels_api", shop).split(",")]
        bol_auth = BOL_API(config["bol_api_urls"]["authorize_url"], client_id, client_secret)
        # Create a task for each order to be sent asynchronously
        task = asyncio.create_task(send_request_shiping_info_to_bol(
            bol_auth,
            order["verzendpartner"],
            order["order_id_leverancier"],
            order["order_orderitemid"],
            order["orderid"],
            rate_limit,
        ))
        tasks.append(task)

    # Wait for all tasks to complete in parallel
    await asyncio.gather(*tasks)

asyncio.run(send_info_bol())
