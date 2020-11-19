import PySimpleGUI as sg
from cropio import table

sg.theme('Bright Colors')

layout = [
            [sg.Text('Начальная дата', key='-START_DATE_TEXT-'), sg.Text('Конечная дата', key='-FINISH_DATE_TEXT-')],
            [sg.In(key='-START_DATE-', enable_events=True, visible=False),
             sg.CalendarButton('Начальная дата',
                               target='-START_DATE-',
                               pad=None,
                               font=('MS Sans Serif', 10, 'bold'),
                               format=('%Y-%m-%dT%H:%M:%SZ'))
             ,
             sg.In(key='-FINISH_DATE-', enable_events=True, visible=False),
             sg.CalendarButton('Конечная дата',
                               target='-FINISH_DATE-',
                               pad=None,
                               font=('MS Sans Serif', 10, 'bold'),
                               format=('%Y-%m-%dT%H:%M:%SZ'))
             ],
            [sg.Button('Сделать таблицу', key='-START-')]]

window = sg.Window('Calendar', layout)

while True:             # Event Loop
    event, values = window.read()

    window['-START_DATE_TEXT-'].update(values['-START_DATE-'][:10])
    window['-FINISH_DATE_TEXT-'].update(values['-FINISH_DATE-'][:10])
    if event in (None, 'Exit'):
        break
    if event == '-START-':
        s_date, f_date = values['-START_DATE-'], values['-FINISH_DATE-']
        if s_date == '' or f_date == '':
            sg.PopupError('Выбери даты')
        else:
            table(s_date, f_date)
            sg.popup_ok("Готово")
window.close()