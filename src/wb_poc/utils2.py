from io import BytesIO
import pandas as pd
import regex as re
import pytesseract
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LAParams
from pdfminer.layout import LTTextBoxHorizontal


def parse_line(name_line,regex_string):
    name_line = re.split(regex_string,name_line)
    name_line = [re.sub(r"[^\w\s]+", " ", name).strip() for name in name_line]
    name_line = [name for name in name_line if name]
    return name_line

def parse_house_line(name_line,regex_string):
    name_line = re.split(regex_string,name_line)
    name_line = [re.sub(r"[^\w\s]+", " ", name).strip() for name in name_line]
    name_line = [name for name in name_line if name]
    return name_line

def next_index(i):
    return 0 if i==3 else i+1

class ParsePage:
    def __init__ (self, image_path, lang):
        self.image_path = image_path
        self.lang = lang

        self.text = None
        self.eng_text = None
        self.voter_id = []

    def get_text(self):
        self.text = (pytesseract.image_to_string(self.image_path, config='--psm 6', lang=self.lang))
        return self.text.split('\n')

    def get_voterids_position(self):
        '''
        This is David's function as is. I'm using a simpler function below. Use that
        '''
        final_words_list = []

        pdf = pytesseract.image_to_pdf_or_hocr(self.image_path, extension='pdf', lang='eng')
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
                if abs(element - df_VI['y'][index + 1]) < 2:
                    df_VI.at[index + 1, 'y'] = element

        words_list = df_VI.sort_values(by=['y', 'x'], ascending=[
            False, True], ignore_index=True)['text'].to_list()

        for word in words_list:
            if len(word.split()) > 0 and len(word.split()) <= 3:
                try:
                    if (word.split()[0][0].isalpha() or word.split()[0][1].isalpha()) and (
                            (word.split()[0][len(word.split()[0]) - 1].isdigit() or word.split()[0][
                                len(word.split()[0]) - 2].isdigit()) \
                            or (word.split()[1][len(word.split()[0]) - 1].isdigit() or word.split()[1][
                        len(word.split()[1]) - 2].isdigit()
                            )):
                        if len(word.split()[0]) < 9:
                            final_word = word.split()[0] + word.split()[1]
                            if len(final_word) > 8:
                                final_words_list.append(final_word)
                        else:
                            final_words_list.append(word.split()[0])
                except IndexError:
                    pass
        self.voter_id = final_words_list

    def get_voterids(self):
        self.eng_text = (pytesseract.image_to_string(self.image_path, config='--psm 6', lang='eng'))
        text_lines = self.eng_text.split('\n')
        final_words_list = []
        for line_num, word in enumerate(text_lines):
            if len(word.split()) > 0 and len(word.split()) <= 7:
                try:
                    if (word.split()[0][0].isalpha() or word.split()[0][1].isalpha()) and (
                            (word.split()[0][len(word.split()[0]) - 1].isdigit() or word.split()[0][
                                len(word.split()[0]) - 2].isdigit()) \
                            or (word.split()[1][len(word.split()[0]) - 1].isdigit() or word.split()[1][
                        len(word.split()[1]) - 2].isdigit()
                            )):
                        if len(word.split()[0]) < 9:
                            final_word = word.split()[0] + word.split()[1]
                            if len(final_word) > 8:
                                print(line_num)
                                print(word)
                                final_words_list.append(final_word)
                        else:
                            print(line_num)
                            print(word)
                            final_words_list.append(word.split()[0])
                except IndexError:
                    pass
        self.voter_id = final_words_list

    def run(self):
        text_lines = self.get_text()

        import importlib
        keyword_constants = importlib.import_module('wb_poc.keyword_constants.'+self.lang)
        VOTERS_NAME = keyword_constants.VOTERS_NAME
        HUSBANDS_NAME = keyword_constants.HUSBANDS_NAME
        FATHERS_NAME = keyword_constants.FATHERS_NAME
        FATHERS_NAME2 = keyword_constants.FATHERS_NAME2
        MOTHERS_NAME = keyword_constants.MOTHERS_NAME
        OTHERS_NAME = keyword_constants.OTHERS_NAME
        AGE = keyword_constants.AGE
        HOUSE_COUNT = keyword_constants.HOUSE_COUNT
        GENDER = keyword_constants.GENDER
        FEMALE = keyword_constants.FEMALE
        MALE = keyword_constants.MALE
        BOOTH_NAME_COUNT = keyword_constants.BOOTH_NAME_COUNT

        rel_regex = f'{FATHERS_NAME}|{HUSBANDS_NAME}|{MOTHERS_NAME}|{OTHERS_NAME}'
        if FATHERS_NAME2:
            rel_regex = rel_regex+"|"+FATHERS_NAME2

        name_lines = []
        rel_name_lines = []
        house_lines = []
        age_lines = []
        gender_lines = []

        all_values = [name_lines,rel_name_lines,house_lines,age_lines,gender_lines]

        last_parsed = None
        current_parsed = None
        parsed = None
        indices = list(range(4))

        for line_num in range(len(text_lines)):
            line_text = text_lines[line_num]

            if line_text.count(VOTERS_NAME)>1 and len(re.findall(rel_regex,line_text))==0:
                l = parse_line(line_text,VOTERS_NAME)
                l += [''] * (3 - len(l))
                name_lines.extend(l)
                current_parsed = 0
                parsed = True

            if len(re.findall(rel_regex,line_text))>=1:
                l = parse_line(line_text,rel_regex)
                l += [''] * (3 - len(l))
                rel_name_lines.extend(l)
                current_parsed = 1
                parsed = True

            if len(re.findall(HOUSE_COUNT, line_text)) >= 1:
                l = re.findall(f'{HOUSE_COUNT}[^\d]+(\d+)[^\d]+',line_text)
                l += [''] * (3 - len(l))
                house_lines.extend(l)
                current_parsed = 2
                parsed = True

            if len(re.findall(AGE, line_text)) >= 1 and len(re.findall(GENDER, line_text)) >= 1:
                #print(line_text)
                l = re.findall(f'{AGE}[^\d]+(\d+)[^\d]+', line_text)
                l += [''] * (3 - len(l))
                age_lines.extend(l)
                l = re.findall(f'{GENDER}[^\w]+(\w+)', line_text)
                l += [''] * (3 - len(l))
                gender_lines.extend(l)
                current_parsed = 3
                parsed = True
            print(f'Parse index {current_parsed} from line {line_num}')

            if parsed:
                if last_parsed and indices[indices.index(current_parsed)-1]!=last_parsed:
                    print(f'Missed previous at line {line_num}')
                    print(f'Last Parsed {last_parsed}')
                    print(f'Current Parsed {current_parsed}')
                    for i in indices[next_index(last_parsed):current_parsed]:
                        print(f'extend Blank for index {i}')
                        all_values[i].extend(['']*3)
                last_parsed = current_parsed
                current_parsed = None
                parsed = None

            if line_num == len(text_lines) - 1 and not all([len(l)==30 for l in all_values]):
                print([len(l) for l in all_values])
                all_val_length = [len(l)<30 for l in all_values]
                missing_indices = [i for i,v in enumerate(all_val_length) if v]
                print('IN Line num')
                for i in missing_indices:
                    print(f'Appending Blank for index {i}')
                    all_values[i].extend([''] * 3)
        self.page_list = all_values
        self.get_voterids_position()
        self.page_df = pd.DataFrame([self.voter_id]+all_values,
                            index=['voter_id','name','rel_name','house','age','gender']).T
        return self.page_df
