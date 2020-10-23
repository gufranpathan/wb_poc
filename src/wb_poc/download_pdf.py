import wget
import time
import os
import logging

class DownloadPDF:
    def __init__(self,url_string,part_start,part_stop,output_dir="data",sleep=10):
        self.url_string = url_string
        self.part_start = part_start
        self.part_stop = part_stop
        self.output_dir = output_dir if output_dir[-1]==os.sep else output_dir+os.sep
        self.sleep = sleep

    def download_file(self,url,out_file):
        wget.download(url, out=out_file)
        time.sleep(self.sleep)

    def get_part_list(self):
        return range(self.part_start,self.part_stop+1)

    def run(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        for part_no in self.get_part_list():
            logging.info(f'Downloading {str(part_no)}')
            url = self.url_string.format(part_no=str(part_no))
            self.download_file(url,out_file=os.path.join(self.output_dir,f'part={part_no}.pdf'))


class DownloadPDFList(DownloadPDF):
    def __init__(self, url_string, part_list, output_dir="data", sleep=10):
        self.url_string = url_string
        self.part_list = part_list
        self.output_dir = output_dir if output_dir[-1] == os.sep else output_dir + os.sep
        self.sleep = sleep

    def get_part_list(self):
        return self.part_list

