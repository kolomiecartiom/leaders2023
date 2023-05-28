import re
import os
import pdfplumber

loaded_file = [
]

not_loaded = [
]


table1 = '1. Медицинские мероприяти'
table2 = '2. Медицинские услуги для лечения заболевания, состояния и контроля за лечением'
table3 = '. Перечень лекарственных препаратов'
stop_ph = '*(1) - Международная статистическая классификация болезней и пробле'


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


def transofrm_table(table, prescription_category, filename, standart):
    for line in table:
        result = ''
        if line[0] is not None and line[1] is None and line[2] is None and line[3] is None:
            prescription_subcategory = line[0].replace('\n', ' ')
        else:
            result += filename + ';;;'
            result += standart + ';;;'
            result += prescription_category + ';;;'
            result += prescription_subcategory + ';;;'
            for i in range(len(line)):
                if line[i] is None:
                    line[i] = ''
                line[i] = line[i].replace('\n', ' ').strip()
            result += ';;;'.join(line)

            with open(f'stg_table/d_standart_prescription.csv', 'a', encoding='utf-8') as f:
                f.write(result + '\n')


def upper_then_title(elem, page, title, pdf_line_info):
    title_coor = list(filter(lambda x: (title.replace('\n', ' ').strip() in x[0]), pdf_line_info))[0]
    elem_coor = list(filter(lambda x: (elem.replace('\n', ' ').strip() in x[0] and x[2] == page), pdf_line_info))[0]
    return (title_coor[1] > elem_coor[1] and title_coor[2] == page) or (title_coor[2] < page)


def get_text(pdf):
    pdf_line_info = []
    for i, page in enumerate(pdf.pages):
        text_lines = page.extract_text_lines()
        for line in text_lines:
            pdf_line_info.append([line['text'].replace('\n', ' ').strip(), line['chars'][0]['y1'], i])
    return pdf_line_info


def get_table(pdf, pdf_line_info):
    data_table1 = []
    data_table2 = []
    for i, page in enumerate(pdf.pages):
        tables = page.extract_tables()
        for table in tables:
            elem = None
            for k1 in range(0, len(table)):
                for k2 in range(len(table[k1])):
                    if table[k1][k2] and '\n' not in table[k1][k2]:
                        elem = table[k1][k2]
                        break
                if elem:
                    break
            if elem:
                if list(filter(lambda x: (table2.replace('\n', ' ').strip() in x[0]), pdf_line_info))\
                        and list(filter(lambda x: (table3.replace('\n', ' ').strip() in x[0]), pdf_line_info)):
                    if upper_then_title(elem, i, table1, pdf_line_info) and not upper_then_title(elem, i, table2, pdf_line_info):
                        data_table1 += table
                    elif upper_then_title(elem, i, table2, pdf_line_info) and not upper_then_title(elem, i, table3, pdf_line_info):
                        data_table2 += table
                    elif upper_then_title(elem, i, table3, pdf_line_info):
                        return data_table1, data_table2
                elif list(filter(lambda x: (table3.replace('\n', ' ').strip() in x[0]), pdf_line_info)):
                    if upper_then_title(elem, i, table1, pdf_line_info) and not upper_then_title(elem, i, table3, pdf_line_info):
                        data_table1 += table
                    elif upper_then_title(elem, i, table3, pdf_line_info):
                        return data_table1, []
                elif list(filter(lambda x: (table2.replace('\n', ' ').strip() in x[0]), pdf_line_info)):
                    if upper_then_title(elem, i, table1, pdf_line_info) and not upper_then_title(elem, i, table2, pdf_line_info):
                        data_table1 += table
                    elif upper_then_title(elem, i, table2, pdf_line_info) and  not upper_then_title(elem, i, stop_ph, pdf_line_info):
                        data_table2 += table
                        return data_table1, data_table2
    return data_table1, data_table2


for root, dirs, files in os.walk("download_pdf/"):
    for filename in files:
        if filename in loaded_file:
            continue
        if filename in not_loaded:
            continue
        file = os.path.join(root, filename)
        try:
            with pdfplumber.open(file) as pdf:
                standart = get_data_about_standart(pdf)
                pdf_line_info = get_text(pdf)

            p1, p2 = get_table(pdf, pdf_line_info)

            if p1:
                transofrm_table(p1, 'Диагностика', filename, standart)
            if p2:

                transofrm_table(p2, 'Лечение', filename, standart)
        except:
            print(f'Файл не загрузился: {filename}')

