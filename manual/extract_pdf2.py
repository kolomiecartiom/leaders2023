import re
import os
import pdfplumber

loaded_file = []


def get_data_about_standart(pdf):
    text_pdf = ''

    for page in pdf.pages:
        text_lines = page.extract_text_lines()
        text_pdf += (' '.join([lines['text'] for lines in text_lines]))
        text_pdf += ';;;'
    if re.findall('.+(Стандарт .+)Категория возрастная:', text_pdf):
        standart_name = re.findall('.+(Стандарт .+)Категория возрастная:', text_pdf)[0]
    else:
        standart_name = re.findall('.+(Стандарт .+)1. Медицинские', text_pdf)[0]
    return standart_name


def get_data_about_mkb(pdf):
    text_pdf = ''
    for page in pdf.pages:
        text_lines = page.extract_text_lines()
        text_pdf += (' '.join([lines['text'] for lines in text_lines]))
        text_pdf += '$$$$$$'
    mkb_standart = re.findall('[A-ZА-Я]\d{1,4}\.\d{0,4} .+\s', text_pdf)
    if not mkb_standart:
        mkb_standart = re.findall('Нозологические единицы ([A-ZА-Я]\d{1,4}.+\s)', text_pdf)
    if not mkb_standart:
        mkb_standart = re.findall('[A-ZА-Я]\d{1,4} .+\s', text_pdf)
    print(mkb_standart)
    result = [f"{mkb.split(' ')[0]};{' '.join(mkb.split(' ')[1:])}" for mkb in mkb_standart]
    return result


for root, dirs, files in os.walk("download_pdf/"):
    for filename in files:
        if filename in loaded_file:
            continue
        # filename = 'Приказ_Министерства_здравоохранения_Российской_Федерации_от_20_декабря_2012_г._№_1088н.pdf'
        file = os.path.join(root, filename)

        print('-----', filename)

        with pdfplumber.open(file) as pdf:
            standart = get_data_about_standart(pdf)
            mkb = get_data_about_mkb(pdf)



        for i in mkb:
            i = i.replace("\n", "")
            print(f'{filename.strip()};{standart.strip()};{i.strip()}')

            with open(f'stg_table/d_mkb_standart.csv', 'a', encoding='utf-8') as f:
                f.write(f'{filename.strip()};{standart.strip()};{i.strip()}' + '\n')

