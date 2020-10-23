from wb_poc.download_pdf import DownloadPDFList
import logging
logging.basicConfig(level=logging.INFO)

url = 'https://ceo.karnataka.gov.in/finalrolls_2020/English/MR/AC154/S10A154P{part_no}.pdf'
parts = [93,94,97,98,105,106,107,108,109,110,111,112,113,114,155,156,157,158,159,167,168,169,170,171,172,173,174,175,176,177,178]

rr_nagar_download = DownloadPDFList(url_string=url,part_list=parts,output_dir='data/rr_nagar/pdf')
rr_nagar_download.run()

