'''
Программа, запускаемая вручную для экстракции данных из PDF в CSV-файл
для дальнейшей загрузки в stg_pdf.d_standart
'''

import re
import os
import pdfplumber

folder_producer = 'loaded/'
filepath_consumer = 'stg_table/d_standart.csv'
ignore_file = []
sep_word = ' '
sep_page = ';;;'
sep_csv = '$'
reload_file = True

re_standart_name_1 = '.+(Стандарт .+)Категория возрастная:'
re_standart_name_2 = '.+(Стандарт .+)1. Медицинские'
re_decree_number = '.+№ (\d{1,10})н "Об'
re_dt_decree = 'Приказ Министерства здравоохранения Российской Федерации от (\d{1,2}.+\d{4}) г\.\s?№ \d{1,8}н "'
re_dt_reg_decree = 'Зарегистрировано в Минюсте РФ (\d{1,2}.+\d{4}) г. Регистрационный №'
re_patient_age_category = 'Категория возрастная: (.+)Пол:'
re_patient_gender = 'Пол: (.+)Фаза:'
re_mkb_phase = 'Фаза: (.+)Стадия:'
re_mkb_stage = 'Стадия: (.+)Осложнени[яе]:'
re_mkb_complication = 'Осложнени[яе]: (.+)Вид медицинской помощи:'
re_med_care_type = 'Вид медицинской помощи: (.+)Услови[ея] оказания'
re_med_care_condition = 'Услови[ея] оказания .+:? (.+)Форма оказания медицинской помощи:'
re_med_care_form = 'Форма оказания медицинской помощи: (.+)Средние сроки лечения \(количество дней\):'
re_therapy_duration_avg = 'Средние сроки лечения \(количество дней\): (\d{1,10})'


def get_standart_desc(pdf):
    text_pdf = ''

    for page in pdf.pages:
        text_lines = page.extract_text_lines()
        text_pdf += (sep_word.join([lines['text'] for lines in text_lines])) + sep_page

    decree_number = re.findall(re_decree_number, text_pdf)[0]

    if re.findall(re_standart_name_1, text_pdf):
        standart_name = re.findall(re_standart_name_1, text_pdf)[0]
    else:
        standart_name = re.findall(re_standart_name_2, text_pdf)[0]

    dt_decree = re.findall(re_dt_decree, text_pdf)[0]
    dt_reg_decree = re.findall(re_dt_reg_decree, text_pdf)[0]
    patient_age_category = re.findall(re_patient_age_category, text_pdf)[0]
    patient_gender = re.findall(re_patient_gender, text_pdf)[0]
    mkb_phase = re.findall(re_mkb_phase, text_pdf)[0]
    mkb_stage = re.findall(re_mkb_stage, text_pdf)[0]
    mkb_complication = re.findall(re_mkb_complication, text_pdf)[0]
    med_care_type = re.findall(re_med_care_type, text_pdf)[0]
    med_care_condition = re.findall(re_med_care_condition, text_pdf)[0]
    med_care_form = re.findall(re_med_care_form, text_pdf)[0]
    therapy_duration_avg = re.findall(re_therapy_duration_avg, text_pdf)[0]

    return {
        'standart_name': f'{standart_name.strip()}',
        'dt_decree': f'{dt_decree.strip()}',
        'dt_reg_decree': f'{dt_reg_decree.strip()}',
        'decree_number': f'{decree_number.strip()}',
        'patient_age_category': f'{patient_age_category.strip()}',
        'patient_gender': f'{patient_gender.strip()}',
        'mkb_phase': f'{mkb_phase.strip()}',
        'mkb_stage': f'{mkb_stage.strip()}',
        'mkb_complication': f'{mkb_complication.strip()}',
        'med_care_type': f'{med_care_type.strip()}',
        'med_care_condition': f'{med_care_condition.strip()}',
        'med_care_form': f'{med_care_form.strip()}',
        'therapy_duration_avg': f'{therapy_duration_avg.strip()}',
    }


for root, dirs, files in os.walk(folder_producer):
    for filename in files:
        if filename in ignore_file:
            continue
        try:
            file = os.path.join(root, filename)
            with pdfplumber.open(file) as pdf:
                standart_desc = get_standart_desc(pdf)

            if reload_file:
                with open(filepath_consumer, 'w', encoding='utf-8') as f:
                    f.write(f'{sep_csv.join(list(standart_desc.keys()))}' + '\n')
                reload_file = False

            with open(filepath_consumer, 'a', encoding='utf-8') as f:
                paste_str = sep_csv.join([value.strip() for value in standart_desc.values()])
                f.write(f'{filename.strip()}{sep_csv}{paste_str}{sep_csv}' + '\n')
        except Exception as e:
            print(f'filename: {file}')
            print(f'error: {e}')