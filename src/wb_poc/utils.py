from io import BytesIO
import pandas as pd
import difflib
import re
import pytesseract
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LAParams
from pdfminer.layout import LTTextBoxHorizontal
from tqdm import tqdm
import pdf2image
import multiprocessing
from multiprocessing.pool import ThreadPool, Pool
import os
import logging

VOTERS_NAME = 'निर्वाचक का नाम'
HUSBANDS_NAME = 'पति का नाम'
FATHERS_NAME = 'पिता का नाम'
MOTHERS_NAME = 'माता का नाम' 
OTHERS_NAME = 'अन्य का नाम'
AGE = 'उम्र'
HOUSE_COUNT = 'गृह संख्या'
FEMALE = 'महिला'
MALE = 'पुरूष'
BOOTH_NAME_COUNT = 'प्रभाग संख्या व नाम'

# Bengali
VOTERS_NAME = 'নাম'
HUSBANDS_NAME = 'স্বামীর নাম'
FATHERS_NAME = 'पপিতার নাম'
MOTHERS_NAME = 'মার নাম'
OTHERS_NAME = 'अन्य का नाम'
AGE = 'বয়স'
HOUSE_COUNT = 'বাড়ীর নং'
FEMALE = 'স্ত্রী'
MALE = 'পুং'
BOOTH_NAME_COUNT = 'पভাগ নং ,নাম'


def get_images(pdf):
    images_200 = pdf2image.convert_from_path(pdf, dpi=200, fmt='jpg')
    images_500 = pdf2image.convert_from_path(pdf, dpi=500, fmt='jpg')
    return [(img200, img500) for img200, img500 in enumerate(zip(images_200, images_500), 1)]


def parse_page(image):

    def parse_lang(lang):

        #pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
        # config='--psm 4' config='-c preserve_interword_spaces=1'
        text = (pytesseract.image_to_string(
            image, config='--psm 6', lang=lang))
        # print('hello')

        # fix error in OCR recognition of keywords

        def OCR_recog_fix(str_to_replace, words, text):

            similar_words = []
            logging.info(f'String to replace is: {str_to_replace}')
            logging.info(f'Length is: {str_to_replace.split(" ")}')
            if len(str_to_replace.split(" ")) == 6:
                word_combo = [' '.join([i, j, k, l, m, n]) for i, j, k, l, m, n in zip(
                    words, words[1:], words[2:], words[3:], words[4:], words[5:])]
            if len(str_to_replace.split(" ")) == 3:
                word_combo = [' '.join([i, j, k])
                              for i, j, k in zip(words, words[1:], words[2:])]
            if len(str_to_replace.split(" ")) == 4:
                word_combo = [' '.join([i, j, k, l])
                              for i, j, k, l in zip(words, words[1:], words[2:], words[3:])]
            if len(str_to_replace.split(" ")) == 2:
                word_combo = [' '.join([i, j])
                              for i, j in zip(words, words[1:])]

            if len(str_to_replace.split(" ")) == 1:
                word_combo = words

            for word in word_combo:
                if difflib.SequenceMatcher(None, str_to_replace, word).ratio() > 0.90 and difflib.SequenceMatcher(None, str_to_replace, word).ratio() < 1.0:
                    similar_words.append(word)
            for word in similar_words:
                text = text.replace(word, str_to_replace)

            return text

        words = text.split()

        text = OCR_recog_fix(f'{BOOTH_NAME_COUNT}', words, text)
        text = OCR_recog_fix(VOTERS_NAME, words, text)
        text = OCR_recog_fix(HUSBANDS_NAME, words, text)
        text = OCR_recog_fix(FATHERS_NAME, words, text)
        text = OCR_recog_fix(AGE, words, text)

        new_text = text.replace(VOTERS_NAME, f'\n {VOTERS_NAME}')
        new_text = new_text.replace(HUSBANDS_NAME, f'\n {HUSBANDS_NAME}')
        new_text = new_text.replace(FATHERS_NAME, f'\n {FATHERS_NAME}')
        new_text = new_text.replace(MOTHERS_NAME, f'\n {MOTHERS_NAME}')
        new_text = new_text.replace(OTHERS_NAME, '\n OTHERS_NAME')
        new_text = new_text.replace(HOUSE_COUNT, f'\n {HOUSE_COUNT}')
        new_text = new_text.replace(AGE, f'\n {AGE}')
        new_text = new_text.replace('SH:', '\n SH:')
        new_text = new_text.replace('GH:', '\n GH:')
        new_text = re.sub('.H:', '\n SH:', new_text)

        line_list = new_text.split('\n')
        line_list = list(filter(None, line_list))
        line_list = [line.strip() for line in line_list]
        return line_list

    line_list = parse_lang('ben')

    name = []
    rel_type = []
    rel_name = []
    sex = []

    name_error_check = 0
    rel_name_error_check = 0
    sex_error_check = 0

    for index, line in enumerate(line_list):
        if line.startswith(VOTERS_NAME):
            rel_name_error_check = 0
            if sex_error_check < 3 and sex_error_check != 0:
                sex.append("")
                sex_error_check = sex_error_check + 1
            name.append(line[18:len(line)].strip())
            name_error_check = name_error_check + 1

        elif line.startswith(HUSBANDS_NAME) or line.startswith(FATHERS_NAME) or line.startswith(MOTHERS_NAME) or line.startswith(OTHERS_NAME):
            sex_error_check = 0
            if name_error_check < 3 and name_error_check != 0:
                name.append("")
                name_error_check = name_error_check + 1
            if line.startswith(HUSBANDS_NAME):
                rel_name.append(line[12:len(line)].strip())
                rel_type.append('husband')
            elif line.startswith(FATHERS_NAME):
                rel_name.append(line[13:len(line)].strip())
                rel_type.append('father')
            elif line.startswith(MOTHERS_NAME):
                rel_name.append(line[13:len(line)].strip())
                rel_type.append('mother')
            elif line.startswith(OTHERS_NAME):
                rel_name.append(line[13:len(line)].strip())
                rel_type.append('other')
            rel_name_error_check = rel_name_error_check + 1

        elif line.startswith(AGE):
            name_error_check = 0
            if rel_name_error_check < 3 and rel_name_error_check != 0:
                rel_name.append("")
                rel_type.append("")
                rel_name_error_check = rel_name_error_check + 1
            if FEMALE in line:
                sex.append(FEMALE)
            else:
                sex.append(MALE)
            sex_error_check = sex_error_check + 1

    line_list = parse_lang('eng+ben')

    house_no = []
    age = []

    line_list = [line.strip() for line in line_list]

    house_no_error_check = 0
    age_error_check = 0
    voter_ID_error_check = 0
    division_no_name = ""

    voter_id_list = []

    for index, line in enumerate(line_list):

        if line.startswith(BOOTH_NAME_COUNT):
            division_no_name = line[23:].strip()

        elif line.startswith(VOTERS_NAME):
            if VOTERS_NAME not in line_list[index-1]:
                voter_id_line = line_list[index-1]
                voter_ID_error_check = 0
                words = voter_id_line.split()
                for word in words:
                    try:
                        if (word[0].isalpha() or word[1].isalpha()) and (word[len(word)-1].isdigit() or word[len(word)-2].isdigit()) and len(word) > 8 and '|' not in word:
                            voter_id_list.append(word)
                            voter_ID_error_check = voter_ID_error_check + 1
                    except IndexError:
                        pass

                while voter_ID_error_check < 3:
                    voter_id_list.append("")
                    voter_ID_error_check = voter_ID_error_check + 1

        elif line.startswith(HOUSE_COUNT):

            if house_no_error_check == 3:
                house_no_error_check = 0

            if age_error_check < 3 and age_error_check != 0:
                age.append("")
                age_error_check = age_error_check + 1

            house_no_text = (line[12:len(line)]).strip()
            try:
                house_no.append(re.findall(r'\d+', house_no_text)[0])
            except IndexError:
                house_no.append("")
            house_no_error_check = house_no_error_check + 1

        elif line.startswith(AGE) or line.startswith("SH:") or line.startswith("GH:"):

            if age_error_check == 3:
                age_error_check = 0

            if house_no_error_check < 3 and house_no_error_check != 0:
                house_no.append("")
                house_no_error_check = house_no_error_check + 1

            if line.startswith(AGE):
                age_text = (line[5:10]).strip(':').strip()
                try:
                    age.append(re.findall(r'\d+', age_text)[0])
                except IndexError:
                    age.append("")
            else:
                age_text = (line[4:10]).strip(':').strip()
                try:
                    age.append(re.findall(r'\d+', age_text)[0])
                except IndexError:
                    age.append("")

            age_error_check = age_error_check + 1

    division = [division_no_name] * len(name)

    return division, name, rel_name, rel_type, house_no, age, sex, voter_id_list


def hocr_for_voter_id_mem(image):
    voter_id = []
    final_words_list = []

    pdf = pytesseract.image_to_pdf_or_hocr(image, extension='pdf', lang='eng')
    document = BytesIO(pdf)

    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    df_VI = pd.DataFrame(columns=['x', 'y', 'text'])

    for page in PDFPage.get_pages(document):

        interpreter.process_page(page)
        layout = device.get_result()

        for element in layout:
            if isinstance(element, LTTextBoxHorizontal):
                for line in element._objs:
                    df = pd.DataFrame([[line.bbox[0], line.bbox[1], line.get_text()]], columns=[
                                      'x', 'y', 'text'])
                    df_VI = df_VI.append(df, ignore_index=True)

    df_VI = df_VI.sort_values(by=['y', 'x'], ascending=[
                              False, True], ignore_index=True)

    for index, element in enumerate(df_VI['y']):
        if index < len(df_VI['y']) - 1:
            if abs(element-df_VI['y'][index+1]) < 2:
                df_VI.at[index+1, 'y'] = element

    words_list = df_VI.sort_values(by=['y', 'x'], ascending=[
                                   False, True], ignore_index=True)['text'].to_list()

    for word in words_list:
        if len(word.split()) > 0:
            try:
                if (word.split()[0][0].isalpha() or word.split()[0][1].isalpha()) and (word.split()[0][len(word.split()[0])-1].isdigit() or word.split()[0][len(word.split()[0])-2].isdigit()):
                    if len(word.split()[0]) < 9:
                        final_word = word.split()[0] + word.split()[1]
                        if len(final_word) > 8:
                            final_words_list.append(final_word)
                    else:
                        final_words_list.append(word.split()[0])
            except IndexError:
                pass
    voter_id = final_words_list
    return voter_id


def parse_image(image_tuple):

    page, (img200, img500) = image_tuple

    division, name, rel_name, rel_type, house_no, age, sex, voter_id_list = parse_page(
        img200)

    logging.info(f'Length of 200 DPI name is {len(name)}')
    logging.info(f'Length of 200 DPI voter id is {len(voter_id_list)}')
    logging.info(f'Length of 200 DPI rel_name is {len(rel_name)}')
    logging.info(f'Length of 200 DPI rel_type is {len(rel_type)}')
    logging.info(f'Length of 200 DPI house_no is {len(house_no)}')
    logging.info(f'Length of 200 DPI age is {len(age)}')
    logging.info(f'Length of 200 DPI sex is {len(sex)}')



    #if '' in name or '' in rel_name or '' in rel_type or '' in house_no or '' in age or '' in sex or '' in voter_id_list:
    division_t, name_t, rel_name_t, rel_type_t, house_no_t, age_t, sex_t, voter_id_list_t = parse_page(
        img500)

    logging.info(f'Length of 500 DPI name is {len(name_t)}')
    logging.info(f'Length of 500 DPI voter id is {len(voter_id_list_t)}')
    logging.info(f'Length of 500 DPI rel_name is {len(rel_name_t)}')
    logging.info(f'Length of 500 DPI rel_type is {len(rel_type_t)}')
    logging.info(f'Length of 500 DPI house_no is {len(house_no_t)}')
    logging.info(f'Length of 500 DPI age is {len(age_t)}')
    logging.info(f'Length of 500 DPI sex is {len(sex_t)}')


    # if name_t.count("") < name.count(""):
    #     name = name_t
    # if rel_name_t.count("") < rel_name.count(""):
    #     rel_name = rel_name_t
    # if rel_type_t.count("") < rel_type.count(""):
    #     rel_type = rel_type_t
    # if house_no_t.count("") < house_no.count(""):
    #     house_no = house_no_t
    # if age_t.count("") < age.count(""):
    #     age = age_t
    # if sex_t.count("") < sex.count(""):
    #     sex = sex_t
    # if voter_id_list_t.count("") < voter_id_list.count(""):
    #     voter_id_list = voter_id_list_t

    # if '' in voter_id_list and len(name) > 0:
    #     voter_id_list_t = hocr_for_voter_id_mem(img500)
    #     if voter_id_list_t.count("") < voter_id_list.count(""):
    #         voter_id_list = voter_id_list_t
    voter_id_list_h = hocr_for_voter_id_mem(img500)

    page_no = [page] * len(name)
    df = pd.DataFrame(list(zip(page_no, division, name, rel_name, rel_type, house_no, age, sex, voter_id_list,voter_id_list_h)), columns=[
                      'page', 'division', 'name', 'rel_name', 'rel_type', 'house_no', 'age', 'sex', 'voter_id', 'voter_id_hocr'])
    df_t = pd.DataFrame(list(zip(page_no, division_t, name_t, rel_name_t, rel_type_t, house_no_t, age_t, sex_t, voter_id_list_t,voter_id_list_h)), columns=[
                      'page', 'division', 'name', 'rel_name', 'rel_type', 'house_no', 'age', 'sex', 'voter_id', 'voter_id_hocr'])

    df = pd.DataFrame(list(zip(page_no, division, name, house_no, age, sex, voter_id_list,voter_id_list_h)), columns=[
                      'page', 'division', 'name', 'house_no', 'age', 'sex', 'voter_id', 'voter_id_hocr'])
    df_t = pd.DataFrame(list(zip(page_no, division_t, name_t, house_no_t, age_t, sex_t, voter_id_list_t,voter_id_list_h)), columns=[
                      'page', 'division', 'name', 'house_no', 'age', 'sex', 'voter_id', 'voter_id_hocr'])



    return (df, df_t)


def parse_document(pdf, method='simple'):
    images = get_images(pdf)[2:10]

    if method == 'simple':
        pages = map(parse_image, tqdm(images))

    if method == 'cores':
        cpus = multiprocessing.cpu_count()
        pool = Pool(cpus)
        pages = pool.map(parse_image, tqdm(images))
        pool.close()
        pool.join()
    if method == 'threads':
        pages = ThreadPool().map(parse_image, tqdm(images))
    return pd.concat(pages,  ignore_index=True)

def write_text(text, output_dir, page, ocr_type):
    with open(f'{output_dir}_{ocr_type}_Page_{page}.txt', 'w', encoding='utf-8') as out_file:
        out_file.write(text)


def pages_to_text(pdf,output_dir):
    images = get_images(pdf)
    print(type(images))
    print(len(images))
    print(images[0])
    for page,image in tqdm(images):

        hin_text_200 = (pytesseract.image_to_string(
            image[0], config='--psm 6', lang='hin'))
        write_text(hin_text_200,output_dir,page,'hin_text_200')

        eng_text_200 = (pytesseract.image_to_string(
            image[0], config='--psm 6', lang='eng+hin'))
        write_text(eng_text_200,output_dir,page,'eng_text_200')

        hin_text_500 = (pytesseract.image_to_string(
            image[1], config='--psm 6', lang='hin'))
        write_text(hin_text_500, output_dir,page, 'hin_text_500')

        eng_text_500 = (pytesseract.image_to_string(
            image[1], config='--psm 6', lang='eng+hin'))
        write_text(eng_text_500, output_dir,page, 'eng_text_500')





