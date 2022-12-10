import datetime

def add_week_days(date):
    u = datetime.datetime.strptime(date, "%Y-%m-%d")
    d = datetime.timedelta(weeks=1)
    t = u + d
    print(t)

add_week_days(('2022-01-07'))