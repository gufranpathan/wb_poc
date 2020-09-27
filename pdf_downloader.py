from wb_poc.download_pdf import DownloadPDF

url = 'http://ceowestbengal.nic.in/FinalRoll?DCID=4%20&ACID=34&PSID={ac_no}'

import logging
logging.basicConfig(level=logging.INFO)

downloader = DownloadPDF(url,1,270,output_dir="data/kaliaganj/pdf")
downloader.run()
