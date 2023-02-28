import logging
from multiprocessing import Process
import time
import requests
import json
import boto3
from boto3.s3.transfer import TransferConfig
from tempfile import NamedTemporaryFile
from botocore.exceptions import ClientError

from openpyxl import Workbook

import threading

s3_client = boto3.client("s3")

logger = logging.getLogger(__name__)

url = "https://services.talma.com.pe:8015/srvTCMV/api/volantevalida"
headers = {"Content-type": "application/json"}

req_body = {
    "rugAgenteAduana": "20100246768",
    "listVolantes": [
        {"strvolante": "07681958"},
        {"strvolante": "07681959"},
        {"strvolante": "07681960"},
        {"strvolante": "076819635"},
        {"strvolante": "076819647"},
    ],
}

def process_function(index):
    response = requests.get(url, data=json.dumps(req_body), headers=headers)
    print("#" * 100)
    print(response.json())
    print(index)
    print("$" * 100)
    time.sleep(10)

def fun1(timer):
    time.sleep(timer)
    return "Hola" * 5


def fun2(timer):
    time.sleep(2 * timer)
    return "Mundo" * 5


from threading import Thread


class CustomThread(Thread):
    def __init__(
            self, 
            group=None, 
            target=None, 
            name=None,
            args=(), 
            kwargs={}, 
            Verbose=None
        ):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None
 
    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)
             
    def join(self, *args):
        Thread.join(self, *args)
        return self._return




def handler1(event, context):
    start_time = time.perf_counter()
    threads = []
    x = CustomThread(target=fun2, name = "func-1", args=(10,))
    threads.append(x)
    x.start()
    y = CustomThread(target=fun1, name = "func-2", args=(10,))
    threads.append(y)
    y.start()

    map = {}
    for thread in threads:
        a = thread.join()
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(a, " -> ", thread.name, " :: ", run_time)
        map[thread.name] = a

    end_time = time.perf_counter()
    run_time = end_time - start_time

    print(run_time)

    # processes = []
    # # 3 procesos
    # for index in range(10):
    #     x = Process(target=process_function, args=(index,))
    #     processes.append(x)
    #     x.start()
        
    # for process in processes:
    #     process.join()

    # end_time = time.perf_counter()
    # run_time = end_time - start_time
    logger.info(f"...............Finished execution in {run_time:.4f} seg")
    return {
        "status_code": 200,
        "output": f"...............Finished execution in {run_time:.4f} seg",
        "data": map
    }

class ProgressPercentage(object):
    def __init__(self, filename, file_size):
        self._filename = filename
        self._size = float(file_size)
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._last_seen_so_far = 0

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            # Using two pointers technique
            # saving database with at least 5% of distance
            logger.info(":: CallBack from S3 :: Event callback :: ...............")
            self._seen_so_far += bytes_amount
            difference = 100 * (self._seen_so_far - self._last_seen_so_far) / self._size
            percentage = (self._seen_so_far / self._size) * 100
            logger.info(f":: CallBack from S3 :: Event callback :: {percentage}%")
            if difference >= 20 or percentage >= 100:
                # TODO: save % to report table with filename and user
                # TODO: change it as command
                # 5.5% is the slice assigned to S3 uploads so delta_offset = 94.5

                logger.info(f":: CallBack from S3 :: Event callback :: {self._seen_so_far}")
                self._last_seen_so_far = self._seen_so_far

            logger.info(
                f":: CallBack from S3 :: Event callback :: {self._seen_so_far} bytes"
            )
            logger.info(":: CallBack from S3 :: Finish Event callback ::............")



def upload_report_object(
    file_name: str,
    bucket: str,
    key: str,
    **kwargs,
):
    try:
        s3_client.upload_file(
            Filename=file_name,
            Bucket=bucket,
            Key=key,
            **kwargs,
        )
    except ClientError as e:
        logger.error(key, bucket, e)
        raise e



def handler(event, context):
    valuesColumn = [
        {
            "volante_num": "329482948",
            "agent_ruc": "32849248294842",
            "agent_name": "Manuel Odria",
            "withdrawal_date": "2023-10-23",
            "volante_payment_status": "rejected",
            "quotation_payment_status": "rejected",
            "invoice_payment_status": "rejected",
            "volante_created_at": "2023-10-23 23:20",
            "billable_ruc": "2392424242",
            "billable_ruc_name": "DHL FORWADING SAC",
            "billable_ruc_option": "customs_agency",
            "quotation_created_at": "2023-10-23 23:20",
            "quotation_updated_at": "2023-10-23 23:20",
            "id_pedido": "1293143141",
            "payment_type": "upfront",
            "payer_ruc_option": "customs_agency",
            "payer_ruc": "1381412424",
            "payer_ruc_name": "Antonio sarosi",
            "user_email_aproved": "almat.almat@almat.almat",
            "tci_num": "32-324-424242",
            "sales_invoice_price_usd": 3242.1,
            "sales_invoice_price_pen": 3242.1,
            "payment_date": "2023-10-10",
            "compensated_by": "dhl@talmclickj.com",
            "user_invoice_full_name": "juan perz",
        }
        for _ in range(375000)
    ]
    columns = [
        "NÚMERO DE VOLANTE",
        "RUC DE LA AGENCIA ADUANERA",
        "AGENCIA ADUANERA",
        "FECHA DE RETIRO DE LA CARGA",
        "ESTADO DE PAGO DEL VOLANTE",
        "ESTADO DE PAGO DE LA PREFACTURA",
        "ESTADO DE PAGO DE LA FACTURA",
        "PRIMERA CONSULTA DEL VOLANTE",
        "RUC FACTURABLE",
        "NOMBRE DEL RUC FACTUABLE",
        "TIPO DE RUC FACTURABLE",
        "CREACIÓN DE LA PREFACTURA",
        "ÚLTIMA ACTUALIZACIÓN DE LA PREFACTURA",
        "NÚMERO DE PEDIDO EN SAP",
        "TIPO DE PAGO",
        "TIPO DE RUC PAGADOR",
        "RUC PAGADOR",
        "NOMBRE DEL RUC PAGADOR",
        "USUARIO QUIEN APROBÓ EL PAGO",
        "NÚMERO DE FACTURA EN SAP",
        "MONTO EN SOLES",
        "MONTO EN DÓLARES",
        "FECHA DE PAGO",
        "USUARIO QUE COMPENSÓ EL PAGO",
        "USUARIO DEL AGENTE ADUANERO QUE PARTICIPÓ EN EL PROCESO",
    ]
    title = "title"
    name_file = "file.xlsx"
    logger.info(":: Generating Excel :: building rows :: Reports :: ...........")
    start_time = time.perf_counter()
    wb = Workbook()
    ws = wb.active  # Update the first page of the excel.
    ws.title = title
    ws.append(columns)

    for value in valuesColumn:
        keys = list(value.keys())
        row = [value[key] for key in keys]
        ws.append(row)

    end_time = time.perf_counter()
    process_time = end_time - start_time
    logger.info(
        f".....Finish execution build..................{process_time:.4f} seg....."
    )
    logger.info(":: Finish Excel on stash :: building rows :: Reports :: ........")
    transfer_config = TransferConfig(
        max_concurrency=10,  # num transfer request or threats
        multipart_threshold=1024 * 5,  # min size file on megabytes
        multipart_chunksize=1024 * 5,  # size of chunksize of each thread
        use_threads=True,  # if use threats
    )
    with NamedTemporaryFile() as tmp:
        logger.info(":: Saving on tmp :: Reports :: ........................")
        wb.save(tmp.name)
        logger.info(":: Seek :: Report :: ..................................")
        tmp.seek(0)
        logger.info(":: To S3 :: Report :: Calculating bytes................")
        file_size_class = os.stat(tmp.name)
        file_size = file_size_class.st_size
        logger.info(f":: Total Size :: Report :: {file_size} bytes..........")
        logger.info("......... :: Uploading the file data to s3 :: .........")
        end_time = time.perf_counter()
        run_time = end_time - start_time
        logger.info("#" * 100)
        logger.info(
            f"Finish execution to generate excel in {run_time:.4f} seg........."
        )
        logger.info("#" * 100)
        keystr = f"reports/volantes/{name_file}"
        upload_report_object(
            file_name=tmp.name,
            bucket="e2ecarga-dev-files",
            key=keystr,
            Config=transfer_config,
            Callback=ProgressPercentage(
                filename=keystr,
                file_size=file_size,
            ),
        )
    return {
        "status_code": 200,
        "key": keystr,
    }