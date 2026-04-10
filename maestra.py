import pandas as pd
import requests
import environ

env = environ.Env()
environ.Env.read_env()

maestra_secret = env('MAESTRA_KEY')

def safe_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        return value if value else None
    return value


def format_birth_date(value):
    value = safe_value(value)
    if value is None:
        return None

    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def get_base_headers(secret_key):
    return {
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "Authorization": f"SecretKey {secret_key}",
    }


def find_customer_by_email(email):
    url = "https://api.maestra.io/v3/operations/sync?endpointId=instantfunding.dashboard&operation=customerFind"
    headers = get_base_headers(maestra_secret)
    payload = {"customer": {"email": email}}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data["customer"]["processingStatus"]
    except requests.exceptions.HTTPError:
        return None
    except Exception:
        return None


def send_customer_registration(payload):
    url = "https://api.maestra.io/v3/operations/async?endpointId=instantfunding.dashboard&operation=customerRegistration"
    headers = get_base_headers(maestra_secret)

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.status_code, response.json()


def map_row_fields(row):
    return {
        "email": safe_value(row.get("email")),
        "first_name": safe_value(row.get("first_name")),
        "last_name": safe_value(row.get("last_name")),
        "birth_date": format_birth_date(row.get("birth_date")),
        "phone": safe_value(row.get("phone")),
        "country_code": safe_value(row.get("country_code")),
        "country": safe_value(row.get("country")),
        "city": safe_value(row.get("city")),
        "state": safe_value(row.get("state")),
        "postcode": safe_value(row.get("postcode")),
    }


def build_custom_fields(mapped_data):
    custom_fields_mapping = {
        "country_code": "langprop",
        "country": "country",
        "city": "city",
        "state": "state",
        "postcode": "zip",
    }

    custom_fields = {}

    for source_field, target_field in custom_fields_mapping.items():
        value = mapped_data.get(source_field)
        if value is not None:
            custom_fields[target_field] = value

    return custom_fields


def build_customer_payload(mapped_data):
    customer = {
        "email": mapped_data["email"],
        "subscriptions": [
            {
                "brand": "instantfunding",
                "pointOfContact": "Email"
            }
        ]
    }

    direct_fields_mapping = {
        "first_name": "firstName",
        "last_name": "lastName",
        "birth_date": "birthDate",
        "phone": "mobilePhone",
    }

    for source_field, target_field in direct_fields_mapping.items():
        value = mapped_data.get(source_field)
        if value is not None:
            customer[target_field] = value

    custom_fields = build_custom_fields(mapped_data)
    if custom_fields:
        customer["customFields"] = custom_fields

    return {"customer": customer}


def sync_customer_row(row):
    mapped_data = map_row_fields(row)
    email = mapped_data["email"]

    if not email:
        return {
            "status": "skipped",
            "reason": "empty email"
        }

    try:
        check = find_customer_by_email(email)

        if check != "NotFound":
            return {
                "status": "exists",
                "email": email,
                "processing_status": check
            }

        payload = build_customer_payload(mapped_data)
        status_code, response = send_customer_registration(payload)

        return {
            "status": "created",
            "email": email,
            "response_status": status_code,
            "response": response
        }

    except Exception as e:
        return {
            "status": "error",
            "email": email,
            "error": str(e)
        }


def sync_customers_from_df(df):
    if "email" not in df.columns:
        raise ValueError("df must contain 'email' column")

    results = []

    for _, row in df.iterrows():
        results.append(sync_customer_row(row))

    return pd.DataFrame(results)