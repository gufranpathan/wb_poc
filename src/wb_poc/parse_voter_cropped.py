from PIL import Image
import pandas as pd
import pytesseract
import os
import regex as re
import importlib
#lang='ben'
#image_files = [os.path.join(image_dir,image) for image in os.listdir(image_dir)]

def get_image_files(image_dir):
    return [os.path.join(image_dir,image) for image in os.listdir(image_dir)]

def get_ocr(image,lang):
    text = pytesseract.image_to_string(image, config='--psm 6', lang=lang)
    text_lines = text.split('\n')
    text_lines = [line for line in text_lines if line != ""]
    return text_lines

def get_text(image_files, lang):
    ben_texts = []
    eng_texts = []
    for image in image_files:
        ben_texts.append(get_ocr(image,lang))
        eng_texts.append(get_ocr(image, 'eng'))
    return (ben_texts,eng_texts)

def parse_line(name_line,regex_string):
    name_line = re.split(regex_string,name_line)
    name_line = [re.sub(r"[^\w\s]+", " ", name).strip() for name in name_line]
    name_line = [name for name in name_line if name]
    return " ".join(name_line)

def parse_house_line(name_line,regex_string):
    name_line = re.split(regex_string,name_line)
    name_line = [re.sub(r"[^\w\s]+", " ", name).strip() for name in name_line]
    name_line = [name for name in name_line if name]
    return " ".join(name_line)


def parse_text(ben_texts,eng_texts, image_files, lang):

    keyword_constants = importlib.import_module('wb_poc.keyword_constants.' +lang)
    VOTERS_NAME = keyword_constants.VOTERS_NAME
    HUSBANDS_NAME = keyword_constants.HUSBANDS_NAME
    FATHERS_NAME = keyword_constants.FATHERS_NAME
    FATHERS_NAME2 = keyword_constants.FATHERS_NAME2
    MOTHERS_NAME = keyword_constants.MOTHERS_NAME
    MOTHERS_NAME2 = keyword_constants.MOTHERS_NAME2
    OTHERS_NAME = keyword_constants.OTHERS_NAME
    AGE = keyword_constants.AGE
    HOUSE_COUNT = keyword_constants.HOUSE_COUNT
    GENDER = keyword_constants.GENDER
    FEMALE = keyword_constants.FEMALE
    MALE = keyword_constants.MALE
    BOOTH_NAME_COUNT = keyword_constants.BOOTH_NAME_COUNT

    rel_regex = f'{FATHERS_NAME}|{HUSBANDS_NAME}|{MOTHERS_NAME}|{OTHERS_NAME}'

    if FATHERS_NAME2:
        rel_regex = rel_regex + "|" + FATHERS_NAME2

    if MOTHERS_NAME2:
        rel_regex = rel_regex + "|" + MOTHERS_NAME2

    parsed_lines = []
    for text_index, (ben_voters,eng_voters) in enumerate(zip(ben_texts,eng_texts)):
        name = ''
        rel_name = ''
        age = ''
        gender = ''
        house_number = ''

        name_index = None
        rel_index = None
        age_index = None
        house_number_index = None
        age_gender_index = None


        for index, line_text in enumerate(ben_voters,1):

            if line_text.count(VOTERS_NAME) >= 1 and len(re.findall(rel_regex, line_text)) == 0 and not name_index:
                name = parse_line(line_text, VOTERS_NAME)
                #print(f'{text_index},{name}')
                name_index = index

            if len(re.findall(rel_regex, line_text)) >= 1:
                rel_name = parse_line(line_text, rel_regex)
                rel_index = index

            if len(re.findall(HOUSE_COUNT, line_text)) >= 1:
                house_number = re.findall(f'{HOUSE_COUNT}[^\d]+(\d+)[^\d]+', line_text)
                house_number_index = index

            if len(re.findall(AGE, line_text)) >= 1 or len(re.findall(GENDER, line_text)) >= 1:
                age = re.findall(f'{AGE}[^\d]+(\d+)[^\d]+', line_text)
                gender = re.findall(f'{GENDER}[^\w]+(\w+)', line_text)
                age_gender_index = index

        voter_line = eng_voters[0].split()
        if not voter_line:
            voter_line = eng_voters[1].split()
        max_length = max([len(text) for text in voter_line])
        voter_id = [text for text in voter_line if len(text) == max_length][0]

        if not age:
            pass


        parsed_lines.append((voter_id, name, rel_name, " ".join(age), " ".join(gender), " ".join(house_number),image_files[text_index],re.search("serial=(\d+).jpg",image_files[text_index]).group(1)))
    return pd.DataFrame(parsed_lines,columns=['voter_id','name','rel_name','age','gender','house_number','filename','text_index'])


def parse_page(image_dir, lang):
    image_files = get_image_files(image_dir)
    ben_texts,eng_texts = get_text(image_files, lang)
    df = parse_text(ben_texts, eng_texts,image_files, lang)
    #df.to_csv('data/test.csv',encoding='utf-8-sig')
    return df
#image_dir = "data/kaliaganj/images/assembly=34/part=1/dpi=200/voter_image/page=3"

#parse_page(image_dir)