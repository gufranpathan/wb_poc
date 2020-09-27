import wget
import time
import os
import logging

class DownloadPDF:
    def __init__(self,url_string,constituency_start,constituency_stop,output_dir="data",sleep=10):
        self.url_string = url_string
        self.constituency_start = constituency_start
        self.constituency_stop = constituency_stop
        self.output_dir = output_dir if output_dir[-1]==os.sep else output_dir+os.sep
        self.sleep = sleep

    def download_file(self,url,out_file):
        wget.download(url, out=out_file)
        time.sleep(self.sleep)

    def run(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        for ac_no in range(self.constituency_start,self.constituency_stop+1):
            logging.info(f'Downloading {str(ac_no)}')
            url = self.url_string.format(ac_no=str(ac_no))
            self.download_file(url,out_file=os.path.join(self.output_dir,f'AC_34_Part_{ac_no}.pdf'))

