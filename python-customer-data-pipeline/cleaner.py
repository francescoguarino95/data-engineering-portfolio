from validator import parse_date, parse_income, parse_boolean


def normalize_text(value: str) -> str:
    #Pulisce testo generico rimuovendo spazi inutili.
    if value is None:
        return ""
    return str(value).strip()


def normalize_name(value: str) -> str:
    #Formatta nomi e cognomi in Title Case.
    value = normalize_text(value)
    return value.title()


def normalize_email(value: str) -> str:
    #Pulisce email e la converte in lowercase.
    value = normalize_text(value)
    return value.lower()


def normalize_city(value: str) -> str:
    #Formatta la città in Title Case.
    value = normalize_text(value)
    return value.title()


def normalize_country(value: str) -> str:
    #Normalizza il nome del paese.
    value = normalize_text(value).lower()

    if value in {"italy", "italia"}:
        return "Italy"

    return value.title()


def clean_row(row: dict) -> dict:
    #Restituisce una versione pulita e normalizzata della riga.
    cleaned = {
        "customer_id": normalize_text(row.get("customer_id", "")),
        "first_name": normalize_name(row.get("first_name", "")),
        "last_name": normalize_name(row.get("last_name", "")),
        "email": normalize_email(row.get("email", "")),
        "phone": normalize_text(row.get("phone", "")),
        "city": normalize_city(row.get("city", "")),
        "country": normalize_country(row.get("country", "")),
        "signup_date": parse_date(row.get("signup_date", "")),
        "annual_income": parse_income(row.get("annual_income", "")),
        "is_active": parse_boolean(row.get("is_active", "")),
    }

    return cleaned