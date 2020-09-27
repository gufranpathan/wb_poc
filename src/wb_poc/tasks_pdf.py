import luigi
from luigi.contrib.s3 import S3Client, S3Target, S3FlagTarget,AtomicS3File
import boto3
from pdf2image import convert_from_path
from .keys import AWS_ACCESS_KEY, AWS_SECRET_KEY
import os
import re
import io
import logging
import threading
from botocore.config import Config


s3_bucket_name = 'bihar-rolls'
s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY, config=Config(max_pool_connections=500))

s3 = boto3.resource('s3',
                    aws_access_key_id=AWS_ACCESS_KEY,
                    aws_secret_access_key=AWS_SECRET_KEY)
bucket = 'bihar-rolls'
my_bucket = s3.Bucket(bucket)
from luigi import Parameter

class S3FlagTargetWrite(S3FlagTarget):
    def open(self, mode='r', s3_filename=None):
        if mode == 'w':
            return self.format.pipe_writer(AtomicS3File(self.path+s3_filename, self.fs, **self.s3_options))
        else:
            super().open(mode)


class S3PdfFile(luigi.ExternalTask):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()

    def output(self):
        file_name = f'downloads/FinalRoll_ACNo_{self.assembly}PartNo_{self.part}.pdf'
        full_s3_path = 's3://' + s3_bucket_name + '/' + file_name
        return S3Target(full_s3_path, client=s3_client, format=luigi.format.Nop)


class PdfDownload(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()

    def requires(self):
        return S3PdfFile(assembly=self.assembly, part=self.part)

    def run(self):
        base_dir = os.path.dirname(self.output().path)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        fo = self.output().open('w')
        with self.input().open('r') as f:
            fo.write(f.read())
        fo.close()

    def output(self):
        filename = f'data/pdfs/ACNo_{self.assembly}PartNo_{self.part}.pdf'
        return luigi.LocalTarget(filename, format=luigi.format.Nop)


class PdfToImage(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    dpis = luigi.IntParameter(default=200)

    def requires(self):
        return PdfDownload(assembly=self.assembly, part=self.part)



    def run(self):
        if not os.path.exists(self.output().path):
            os.makedirs(self.output().path)

        with self.input().open() as f:
            images = convert_from_path(self.input().path, dpi=self.dpis)
            for page, image in enumerate(images, 1):
                image.save(
                    f"{self.output().path}image_{self.assembly}_{self.part}_{page}_{self.dpis}dpis.jpg")

    def output(self):
        file_name = f'data/images/{self.assembly}/part_{self.part}/{self.dpis}/'
        return luigi.LocalTarget(file_name)


class PdfToImageOnS3(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    dpis = luigi.IntParameter(default=200)

    def requires(self):
        return PdfDownload(assembly=self.assembly, part=self.part)

    def images_to_s3(self,image,page,page_len):
        s3_filename = f"{page}_of_{page_len}_{self.dpis}dpis.jpg"
        logging.info(s3_filename)
        with self.output().open(mode='w', s3_filename=s3_filename) as fo:
            in_mem_file = io.BytesIO()
            image.save(in_mem_file, 'JPEG')
            fo.write(in_mem_file.getvalue())
            in_mem_file.seek(0)

    def run(self):
        if not os.path.exists(self.output().path):
            os.makedirs(self.output().path)

        with self.input().open():
            images = convert_from_path(self.input().path, dpi=self.dpis)
            page_len = len(images)
            threads = []
            for page, image in enumerate(images, 1):
                t = threading.Thread(target=self.images_to_s3,args=(image,page,page_len))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            logging.info(f'Completed parsing for Assembly: {self.assembly}, Part: {self.part}')
            prefix = f"images/image_{self.assembly}_{self.part}/dpi={self.dpis}"
            parts = [re.findall(r'\d+', f.key)[1]
                     for f in my_bucket.objects.filter(Prefix=prefix).all()]
            if len(parts) == page_len:
                logging.info(f"Succesfully completed {self.assembly}, {self.dpis} dpi")
                s3_client.put_string("", self.output().path + "_SUCCESS")
            else:
                raise Exception(f"Upload for assembely {self.assembly}, {self.dpis} dpi failed. Expected {page_len} pages, got {len(parts)}")



    def output(self):
        file_name = f"images/image_{self.assembly}_{self.part}/dpi={self.dpis}"
        # return luigi.LocalTarget(file_name)
        #file_name = f'downloads/FinalRoll_ACNo_{self.assembly}PartNo_{self.part}.pdf'
        full_s3_path = 's3://' + s3_bucket_name + '/' + file_name + "/"
        return S3FlagTargetWrite(full_s3_path, client=s3_client, format=luigi.format.Nop)

class GetAssemblyImages(luigi.WrapperTask):
    assembly = luigi.IntParameter()

    def requires(self):

        prefix = f'downloads/FinalRoll_ACNo_{self.assembly}PartNo'
        parts = [re.findall(r'\d+', f.key)[1]
                 for f in my_bucket.objects.filter(Prefix=prefix).all()]
        parts = sorted([int(part) for part in parts])

        for part in parts:
            yield PdfToImageOnS3(assembly=self.assembly, part=part, dpis=200)
            yield PdfToImageOnS3(assembly=self.assembly, part=part, dpis=500)


if __name__ == '__main__':
    luigi.run()


pdf_prefix = 'downloads/FinalRoll_ACNo'
all_files = [f.key[1] for f in my_bucket.objects.filter(Prefix=pdf_prefix).all()]
[pdf_prefix.format(assembly) for assembly in range(2)]

# assembly_list = [223,224]
# dpis = [200,500]
# parts = sorted([int(part) for part in parts])
#
# prefix = f"images/image_{assembly_list}_{1}/dpi={200}/"
s3_file_results = my_bucket.objects.filter(Prefix=pdf_prefix)
# parts = [re.findall(r'\d+', f.key)[1]
#                  for f in s3_file_results.all()]
#
#

z = list(s3_file_results.all())
q = list(s3_file_results)

[f.key for f in s3_file_results]
s3.ObjectSummary(bucket_name='bihar-rolls', key='downloads/FinalRoll_ACNo_100PartNo_1.pdf')

pdf_pattern = 'downloads/FinalRoll_ACNo_(\d+)PartNo_(\d+).pdf'
pdfs = [f.key for f in s3_file_results]
available_pdfs = [re.findall(pdf_pattern,pdf)[0] for pdf in pdfs]
len(available_pdfs)