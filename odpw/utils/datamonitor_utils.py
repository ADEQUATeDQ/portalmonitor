import datetime


def parseDate(date):
    date_parts = []
    # YYYY<MM|DD|HH|MM|SS>
    if len(date) < 4:
        # error
        pass
    else:
        date_parts_length = [4, 2, 2, 2, 2, 2]
        date_parts = []
        for l in date_parts_length:
            if len(date) >= l:
                date_parts.append(int(date[0:l]))
                date = date[l:]

    if len(date_parts) == 0:
        return None
    elif len(date_parts) == 1:
        date_parts.append(1)
        date_parts.append(1)
    elif len(date_parts) == 2:
        date_parts.append(1)

    return datetime.datetime(*map(int, date_parts))