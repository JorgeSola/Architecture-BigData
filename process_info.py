import pandas as pd

def process_info(df):

    dataset = df.to_dict(orient = 'records')

    new_data = []
    for row in dataset:
        dictionary = {}
        dictionary['Host ID'] = row['Host ID']
        row = str(row['Amenities']).split(',')
        
        if 'TV' in row:
            dictionary['TV'] = 1
        else:
            dictionary['TV'] = 0
        if 'Cable TV' in row:
            dictionary['Cable TV'] = 1
        else:
            dictionary['Cable TV'] = 0
        if 'Kitchen' in row:
            dictionary['Kitchen'] = 1
        else:
            dictionary['Kitchen'] = 0
        if 'Smoking allowed' in row:
            dictionary['Smoking allowed'] = 1
        else:
            dictionary['Smoking allowed'] = 0
        if 'Pets allowed' in row:
            dictionary['Pets allowed'] = 1
        else:
            dictionary['Pets allowed'] = 0
        if 'Heating' in row:
            dictionary['Heating'] = 1
        else:
            dictionary['Heating'] = 0
        if 'Washer' in row:
            dictionary['Washer'] = 1
        else:
            dictionary['Washer'] = 0
        if 'Dryer' in row:
            dictionary['Washer'] = 1
        else:
            dictionary['Washer'] = 0
        if 'Dryer' in row:
            dictionary['Dryer'] = 1 
        else:
            dictionary['Dryer'] = 0
        if '24-hour check-in' in row:
            dictionary['24-hour check-in'] = 1
        else:
            dictionary['24-hour check-in'] = 0

        new_data.append(dictionary)

    df = pd.DataFrame(new_data)
    df.to_csv('flat_info.csv', index = False, sep = ';')