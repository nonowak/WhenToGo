def transform_price(data):
    row = data['row']
    yield {
        **data,
        'price_new_year': row[4],
        'day_of_week_new_year': row[5],
        'price_spring': row[6],
        'day_of_week_spring': row[7],
        'price_weekend': row[8],
        'day_of_week_weekend': row[9],
        'price_one_month': row[10],
        'day_of_week_one_month': row[11],
        'price_winter': row[12],
        'day_of_week_winter': row[13],
        'price_today': row[14],
        'day_of_week_today': row[15],
        'price_autumn': row[16],
        'day_of_week_autumn': row[17],
        'price_five_months': row[18],
        'day_of_week_five_months': row[19],
        'price_three_months': row[20],
        'day_of_week_three_months': row[21],
        'price_summer': row[22],
        'day_of_week_summer': row[23]
    }
