import luigi
from luigi.contrib.s3 import S3Client, S3Target
from .keys import AWS_ACCESS_KEY, AWS_SECRET_KEY
from .utils import parse_document, pages_to_text,get_images,parse_image
from .utils2 import ParsePage
import pytesseract
import pandas as pd
from pdf2image import convert_from_path
from wb_poc import crop_voter_id
from .crop_voter_id import get_rectanges,crop_200
from .parse_voter_cropped import parse_page


import os
import shutil

s3_bucket_name = 'bihar-rolls'
s3_client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)
import logging


def make_check_dir(dir_name):
    if not os.path.exists(dir_name):
        try:
            os.makedirs(dir_name)
        except Exception:
            pass

# cpi_assemblies = {
# 196:'Tarari',
# 195:'Agiaon',
# 194:'Ara',
# 201:'Dumraon',
# 107:'Darauli',
# 106:'Ziradei',
# 109:'Daronda',
# 65:'Balrampur',
# 190:'Paliganj',
# 188:'Phulwarisharif',
# 213:'Karakat',
# 214:'Arwal',
# 217:'Ghosi',
# 9:'Sikta',
# 103:'Bhore',
# 132:'Warisnagar',
# 16:'Kalyanpur',
# 89:'Ouraie',
# 181:'Digha'
# }

class S3PdfFile(luigi.ExternalTask):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()

    def output(self):
        file_name = f'downloads/FinalRoll_ACNo_{self.assembly}PartNo_{self.part}.pdf'
        full_s3_path = 's3://' + s3_bucket_name + '/' + file_name
        return S3Target(full_s3_path, client=s3_client, format=luigi.format.Nop)


class PdfDownload(luigi.ExternalTask):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()

    def output(self):
        filename = f'data/rr_nagar/pdf/assembly={self.assembly}/part={self.part}.pdf'
        logging.info(f'filename is {filename}')
        print(filename)
        return luigi.LocalTarget(filename, format=luigi.format.Nop)

# class PdfDownload(luigi.Task):
#     assembly = luigi.IntParameter()
#     part = luigi.IntParameter()
#
#     def requires(self):
#         return S3PdfFile(assembly=self.assembly, part=self.part)
#
#     def run(self):
#         logging.info(f'Writing to {self.output().path}')
#         make_check_dir(os.path.dirname(self.output().path))
#         fo = self.output().open('w')
#         with self.input().open('r') as f:
#             fo.write(f.read())
#         fo.close()
#
#     def output(self):
#         filename = f'data/rr_nagar/pdf/assembly={self.assembly}/part={self.part}.pdf'
#         return luigi.LocalTarget(filename, format=luigi.format.Nop)


class PdfToImage(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    #dpis = luigi.IntParameter(default=200)

    def requires(self):
        return PdfDownload(assembly=self.assembly, part=self.part)


    def run(self):
        row_col_combinations = [(row,col) for row in range(1,11) for col in range(1,4)]
        try:
            if not isinstance(self.output(),list):
                if not os.path.exists(self.output().path):
                    os.makedirs(self.output().path)
            else:
                for output_dir in self.output():
                    if not os.path.exists(output_dir.path):
                        os.makedirs(output_dir.path)

            with self.input().open() as f:
                for dpi_index, dpi in enumerate([200]):
                    images = convert_from_path(self.input().path, dpi=dpi)
                    consolidated_page_started = False
                    for page, image in enumerate(images, 1):
                        logging.debug(f'Page is {page}')
                        page_dir = f"{self.output()[dpi_index].path}/page_image/"
                        make_check_dir(page_dir)
                        image.save(
                            f"{page_dir}page={page}of{len(images)}.jpg")
                        voter_dir = f"{self.output()[dpi_index].path}/voter_image/page={page}/"
                        if page not in [1,2,len(images)]:
                            make_check_dir(voter_dir)
                            y_min = get_rectanges(image)
                            consolidated_page = False
                            consolidated_page = True if y_min > 120 and page > 5 else consolidated_page
                            consolidated_page_started = True if y_min > 120 and page>5 else consolidated_page_started
                            for row, col in row_col_combinations:
                                cropped_image = crop_voter_id.crop_200(image,row,col,page,len(images),
                                                                       consolidated_page_started,consolidated_page)
                                num = (row - 1) * 3 + col
                                cropped_image.save(
                                f"{voter_dir}serial={num}.jpg"
                                )


        except Exception as e:
            logging.error(e,exc_info=True)
            if not isinstance(self.output(),list):
                if os.path.exists(self.output().path):
                    shutil.rmtree(self.output().path)
            else:
                for output_dir in self.output():
                    if os.path.exists(output_dir.path):
                        shutil.rmtree(output_dir.path)
            raise Exception(e)

    def output(self):
        return [luigi.LocalTarget(f'data/rr_nagar/images/assembly={self.assembly}/part={self.part}/dpi=200/'),
                luigi.LocalTarget(f'data/rr_nagar/images/assembly={self.assembly}/part={self.part}/dpi=500/')]

class AssemblyPdfToImage(luigi.WrapperTask):
    assembly = luigi.IntParameter()
    start_part = luigi.IntParameter()
    end_part = luigi.IntParameter()

    def requires(self):
        yield [PdfToImage(assembly=self.assembly,part=part) for part in range(self.start_part,self.end_part+1)]

class PageImage(luigi.ExternalTask):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    page = luigi.IntParameter()
    dpis = luigi.IntParameter(default=200)

    def output(self):
        dir_name = f'data/rr_nagar/images/assembly={self.assembly}/part={self.part}/dpi={self.dpis}/voter_image/page={self.page}'
        #file_name = f"{dir_name}image_{self.assembly}_{self.part}_{self.page}_{self.dpis}dpis.jpg"
        return luigi.LocalTarget(dir_name)

class ParseImage(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    page = luigi.IntParameter()
    method = luigi.Parameter(default="simple")
    lang = luigi.Parameter()
    #dpis = luigi.IntParameter(default=200)

    def requires(self):
        return PageImage(assembly=self.assembly, part=self.part, page=self.page, dpis=200)
        # return [PageImage(assembly=self.assembly, part=self.part,page=self.page,dpis=200),
        #         PageImage(assembly=self.assembly, part=self.part, page=self.page,dpis=500)]
        #return PdfToImage(assembly=self.assembly,part=self.part)

    def run(self):
        logging.info(f'Parsing Part {self.part}, Page {self.page}')
        # df200, df500 = parse_image((self.page, (self.input()[0].path,self.input()[1].path)))
        #df = pd.DataFrame(df)
        df = parse_page(self.input().path,self.lang)
        #df200 = ParsePage(self.input()[0].path,lang=self.lang).run()
        #df500 = ParsePage(self.input()[1].path,lang=self.lang).run()
        #df200.to_csv(self.output()[0].path,encoding='utf-8-sig')
        #df500.to_csv(self.output()[1].path,encoding='utf-8-sig')
        make_check_dir(os.path.dirname(self.output().path))
        df['assembly'] = self.assembly
        df['part'] = self.part
        df['page'] = self.page
        df.to_csv(self.output().path, encoding='utf-8-sig')


    def output(self):
        filename200 = f'data/rr_nagar/parsed/assembly={self.assembly}/part={self.part}/page={self.page}_200.csv'
        #filename500 = f'data/rr_nagar/parsed/ACNo_{self.assembly}_PartNo_{self.part}_Page{self.page}_500.csv'

        # return [luigi.LocalTarget(filename200, format=luigi.format.Nop),
        #         luigi.LocalTarget(filename500, format=luigi.format.Nop)]
        return luigi.LocalTarget(filename200, format=luigi.format.Nop)


class PartParseImage(luigi.WrapperTask):
    assembly = luigi.IntParameter()
    start_part = luigi.IntParameter()
    end_part = luigi.IntParameter()

    def requires(self):
        # input_dirs_200 = {part:os.path.join(PdfToImage(assembly=self.assembly,part=part).output()[0].path,'page_image')
        #                   for part in range(self.start_part,self.end_part+1)}
        input_dirs_200 = {part:PdfToImage(assembly=self.assembly,part=part).output()[0].path
                          for part in range(self.start_part,self.end_part+1)}
        input_dirs_200 = {part: os.path.join(input_dirs_200[part],'page_image') for part in input_dirs_200
                          if os.path.exists(input_dirs_200[part]) and os.listdir(input_dirs_200[part])}

        #input_dirs_500 = [PdfToImage(assembly=self.assembly,part=part).output()[1].path for part in range(self.start_part,self.end_part+1)]
        #images_200 = [os.listdir(dir) for dir in input_dirs_200]
        #images_500 = [os.listdir(dir) for dir in input_dirs_500]
        yield [ParseImage(assembly=self.assembly,part=part,page=page,lang='eng')  \
               for part in input_dirs_200 for page in range(3,len(os.listdir(input_dirs_200[part])))]


class ParseDocument(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    method = luigi.Parameter(default="simple")

    def requires(self):
        return PdfDownload(assembly=self.assembly, part=self.part)

    def run(self):
        df = parse_document(self.input().path)
        df.to_csv(self.output().path)

    def output(self):
        filename = f'data/parsed/ACNo_{self.assembly}PartNo_{self.part}.csv'
        return luigi.LocalTarget(filename, format=luigi.format.Nop)

class ImageToText(luigi.Task):
    assembly = luigi.IntParameter()
    part = luigi.IntParameter()
    page = luigi.IntParameter()

    def requires(self):
        return [PageImage(assembly=self.assembly, part=self.part,page=self.page,dpis=200),
                PageImage(assembly=self.assembly, part=self.part, page=self.page,dpis=500)]

    def run(self):
        import pdf2image
        # images_200 = pdf2image.convert_from_path(self.input().path[0], dpi=200, fmt='jpg')
        # images_500 = pdf2image.convert_from_path(self.input().path[0], dpi=500, fmt='jpg')

        for image in [page.path for page in self.input()]:
            for lang in ['ben','eng+ben']:
                text = (pytesseract.image_to_string(
                    image, config='--psm 6', lang=lang))
                with open(self.output()[lang].path,'w') as f:
                    f.write(text)

        #df.to_csv(self.output().path)

    def output(self):
        dirname = f'data/parsed_text/ACNo_{self.assembly}_PartNo_{self.part}'
        return {lang:luigi.LocalTarget(os.path.join(dirname, str(f'{self.page}_{lang}.txt')), format=luigi.format.Nop)
                for lang in ['ben','eng+ben']}

#
# if __name__ == '__main__':
#     #luigi.run()
# 	luigi.build([ParseDocument(assembly=223,part=1)],local_scheduler=True)


import pandas as pd

df = pd.DataFrame({'a':[1,2,3],
              'b':[1,2,3]})

df['milestone_event'] = df.a==4
milestone_event = df.groupby('a')['milestone_event'].sum().astype(float)
milestone_event = milestone_event[~milestone_event]
sum(milestone_event)
