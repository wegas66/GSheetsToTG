import time
import gspread
import pandas as pd
import schedule
import configparser
import telebot

config = configparser.ConfigParser()
config.read('config.ini')

TOKEN = config['DEFAULT']['token']
TIMEOUT = int(config['DEFAULT']['timeout'])

with open('groups.txt', encoding="utf-8") as f:
    groups = f.readlines()
    groups = [group.split(';') for group in groups]

bot = telebot.TeleBot(TOKEN)
gc = gspread.service_account(filename='service_account.json')


class Tables:

    tables = {}


old_tables = Tables()


def get_table(worksheet):
    df = pd.DataFrame(worksheet.get_all_records())
    return df


def get_updates(**kwargs):
    updates = (kwargs['old'].merge(kwargs['new'], how='outer', on=kwargs['columns'],
                   suffixes=['', '_new'], indicator=True))
    updates = updates.query("_merge=='right_only'")
    return updates


def create_msgs(updates, group):
    msgs = []
    for i, row in updates.iterrows():
        if str(row['Комментарий менеджера']).lower().replace(' ', '').replace(',','').replace('.','').replace('/','').replace('\\','') != 'чс' and str(row['Комментарий менеджера']) != '':
            msg = (group[0], f"Номер заявки: {row['Номер']}.{row['Дата']}\n\n{row['Комментарий менеджера']}\n\nПо вопросам этой заявки обращайтесь к {group[1]}")
            msgs.append(msg)
    return msgs


def send_updates(msgs):
    for msg in msgs:
        for _ in range(3):
            try:
                bot.send_message(msg[0], msg[1])
                break
            except:
                e = Exception
                time.sleep(10)
                continue
        else:
            print(e)


def do_all():
    for group in groups:
        sh = gc.open_by_url(group[0])
        worksheet = sh.worksheet(group[1])
        new_table = get_table(worksheet)
        updates = get_updates(old=old_tables.tables[group[0]], new=new_table, columns=['Номер', 'Дата', 'Номер телефона', 'Комментарий менеджера'])
        msgs = create_msgs(updates, group[2:])
        send_updates(msgs)
        old_tables.tables[group[0]] = new_table


def main():
    for group in groups:
        sh = gc.open_by_url(group[0])
        worksheet = sh.worksheet(group[1])
        old_tables.tables[group[0]] = get_table(worksheet)
    print('BOT STARTED')
    schedule.every(TIMEOUT).minutes.do(do_all)
    while 1:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
        input()

