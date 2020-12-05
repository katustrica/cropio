import PySimpleGUI as sg
from cropio import table, open_drivers, load_drivers, open_prices

sg.theme('Tan')
dates_layout = [
    [sg.In(key='-START_DATE-', enable_events=True, visible=False),
     sg.CalendarButton('Начальная дата',
                       target='-START_DATE-',
                       pad=None,
                       font=('MS Sans Serif', 10, 'bold'),
                       format=('%Y-%m-%dT%H:%M:%SZ')),
     sg.Text('Начальная дата', key='-START_DATE_TEXT-')],
    [sg.In(key='-FINISH_DATE-', enable_events=True, visible=False),
     sg.CalendarButton('Конечная дата  ',
                       target='-FINISH_DATE-',
                       pad=None,
                       font=('MS Sans Serif', 10, 'bold'),
                       format=('%Y-%m-%dT%H:%M:%SZ')),
     sg.Text('Конечная дата', key='-FINISH_DATE_TEXT-')]
]
drivers_layout = [[sg.Button(' Водители ', key='-OPEN_USERS-'),
                   sg.Button(' Обновить водителей ', key='-LOAD_USERS-')]]
prices_layout = [[sg.Button(' Открыть ', key='-OPEN_PRICES-')]]
layout = [
            [sg.Frame('Даты', dates_layout, font='Helvetica 12', title_color='blue')],
            [sg.Button('                 Сделать таблицу                 ', key='-START-')],
            [sg.Frame('Водители', drivers_layout, font='Helvetica 12', title_color='blue'), sg.Frame('Расценки', prices_layout, font='Helvetica 12', title_color='blue')]
]

window = sg.Window('Calendar', layout)

while True:             # Event Loop
    event, values = window.read()

    if event in (None, 'Exit'):
        break
    window['-START_DATE_TEXT-'].update(values['-START_DATE-'][:10])
    window['-FINISH_DATE_TEXT-'].update(values['-FINISH_DATE-'][:10])
    if event == '-OPEN_USERS-':
        open_drivers()
    if event == '-LOAD_USERS-':
        load_drivers()
    if event == '-OPEN_PRICES-':
        open_prices()
    if event == '-START-':
        s_date, f_date = values['-START_DATE-'], values['-FINISH_DATE-']
        if s_date == '' or f_date == '':
            sg.PopupError('Выбери даты')
        else:
            table(s_date, f_date)
            sg.popup_ok("Готово")
window.close()