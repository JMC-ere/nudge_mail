from openpyxl import Workbook
from openpyxl.styles import Border, Side, Font, PatternFill

# 표 스타일 속성값
THIN_BORDER = Border(left=Side(style='thin'),
                     right=Side(style='thin'),
                     top=Side(style='thin'),
                     bottom=Side(style='thin'))
# 슬롯 수 제거
type_header = ['일자', '넛지종류', '넛지 노출 수', '넛지 포커스 수', '넛지 클릭 수',
               '클릭 전환율', '노출 STB 수', '포커스 STB 수', '클릭 STB 수', '이용율']

# 구분, 슬롯 수 제거
location_header = ['일자', '넛지위치', '넛지 노출 수', '넛지 포커스 수',
                   '넛지 클릭 수', '클릭 전환율', '노출 STB 수', '포커스 STB 수', '클릭 STB 수', '이용율']


# 헤더 색상, 글씨 굵게 변경
def header_styles(area):
    for row in area:
        for cell in row:
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color='b7b9bc', end_color='b7b9bc', fill_type='solid')


# 셀 표 모든테두리
def sheet_styles(full_area):
    for row in full_area:
        for cell in row:
            cell.border = THIN_BORDER


# 전환율, 이용률 구하기
def percentage(name):
    if 'click.voice_text.button' in name:
        click = name['click.voice_text.button'] / name['page_show'] * 100
        use = name['click.voice_text.button_stb'] / name['page_show_stb'] * 100
        name['click'] = click
        name['use'] = use
    else:
        name['click'] = 0
        name['use'] = 0
        name['click.voice_text.button_stb'] = 0
        name['click.voice_text.button'] = 0
        name['focus.voice_text.button'] = 0
        name['focus.voice_text.button_stb'] = 0

    if 'focus.voice_text.button' not in name:
        name['focus.voice_text.button'] = 0
        name['focus.voice_text.button_stb'] = 0


def insert_data(location_data, type_data):
    # 시트 번호를 알기위한 변수
    num = 1

    wb = Workbook()
    type_ws = wb.create_sheet(title="종류별 자동넛지 데이터")
    location_ws = wb.create_sheet(title="위치별 자동넛지 데이터")

    # 종류별 헤더값 입력
    type_ws.append(type_header)
    # 종류별 헤더 스타일 지정
    header_styles(type_ws['A1:J1'])
    # 종류별 데이터 입력
    for t in type_data:
        percentage(t)
        type_ws.append([
            t['day'],
            t['type_value'],
            t['page_show'],
            t['focus.voice_text.button'],
            t['click.voice_text.button'],
            t['click'],
            t['page_show_stb'],
            t['focus.voice_text.button_stb'],
            t['click.voice_text.button_stb'],
            t['use']
        ])
        num = num + 1

    # 셀 테두리
    sheet_styles(type_ws['A1:J' + str(num)])

    # 초기화
    num = 1

    # ----------------------------------------------------------------------------------------------------------------

    # 위치별 헤더값 입력
    location_ws.append(location_header)
    # 종류별 헤더 스타일 지정
    header_styles(location_ws['A1:J1'])
    # 종류별 데이터 입력
    for lo in location_data:
        percentage(lo)
        location_ws.append([
            lo['day'],
            lo['location_value'],
            lo['page_show'],
            lo['focus.voice_text.button'],
            lo['click.voice_text.button'],
            lo['click'],
            lo['page_show_stb'],
            lo['focus.voice_text.button_stb'],
            lo['click.voice_text.button_stb'],
            lo['use']
        ])
        num = num + 1

    # 셀 테두리
    sheet_styles(location_ws['A1:J' + str(num)])

    del wb['Sheet']
    wb.save('자동넛지데이터.xlsx')
