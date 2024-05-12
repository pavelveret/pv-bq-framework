import pandas as pd
import phonenumbers
import pycountry

# %%
def country_to_alpha_2(country):
    country = str(country)
    if country:
        if len(country) > 2:
            try:
                country = pycountry.countries.search_fuzzy(country)
                country = country[0].alpha_2
            except: 
                country = ""
    elif country == 'Select a country':
        country = ""
    return str(country)

# %%
def alpha2_to_country_name(alpha2_code):
    try:
        country = pycountry.countries.get(alpha_2=alpha2_code)
        return country.name if country else ""
    except Exception as e:
        return ""

# %%
def validate_phonе(phone, country):
    try:
        p = phonenumbers.parse(str(phone), str(country))
        p = phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.E164)
    except Exception as err:
        #print(phone)
        #print(country)
        #print(err)
        p = ''
    
    return p

# %%
def add_international_phone(df):
    df.phone = df.phone.fillna('')
    def check_reqs(country, phone):
        if country != '' and phone != '':
            return True
        else:
            return False

    for index, row in df.iterrows():
        if check_reqs(row['country_2symbols'], row['phone']):
            df.loc[index,'international_phone'] = str(validate_phonе(row['phone'], row['country_2symbols']))
        else:
            df.loc[index,'international_phone'] = ''
    return df

# %%
def add_country_and_phone(df):
    df.phone = df.phone.fillna('')
    df.country = df.country.fillna('')
    df['country_2symbols'] = df['country'].apply(country_to_alpha_2)
    df['country_readable'] = df['country_2symbols'].apply(alpha2_to_country_name)
    df = add_international_phone(df)

    return df

# %%



