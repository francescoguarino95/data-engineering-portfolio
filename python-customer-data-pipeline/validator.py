import re
from datetime import datetime
from typing import Optional

EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"


def parse_date(value: str) -> Optional[str]:
    #Converte una data in formato YYYY-MM-DD, se possibile.
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    formats = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]

    for fmt in formats:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue

    return None


def is_valid_email(email: str) -> bool:
    #Controllo base sul formato email.
    if email is None:
        return False
    email = str(email).strip().lower()
    return bool(re.match(EMAIL_REGEX, email))


def parse_income(value: str) -> Optional[float]:
    #Converte annual_income in float, se possibile.
    if value is None:
        return None

    value = str(value).strip()
    if not value:
        return None

    try:
        return float(value)
    except ValueError:
        return None


def parse_boolean(value: str) -> Optional[bool]:
    #Converte is_active in booleano.
    if value is None:
        return None

    value = str(value).strip().lower()

    true_values = {"yes", "true", "1"}
    false_values = {"no", "false", "0"}

    if value in true_values:
        return True
    if value in false_values:
        return False

    return None


def validate_row(row: dict) -> Optional[str]:
    
    #Valida una singola riga.
    #Restituisce None se la riga è valida,
    #altrimenti restituisce una stringa con il motivo di scarto.
    
    customer_id = str(row.get("customer_id", "")).strip()
    first_name = str(row.get("first_name", "")).strip()
    last_name = str(row.get("last_name", "")).strip()
    email = row.get("email", "")
    signup_date = row.get("signup_date", "")
    annual_income = row.get("annual_income", "")
    is_active = row.get("is_active", "")

    if not customer_id:
        return "missing_customer_id"

    if not first_name:
        return "missing_first_name"

    if not last_name:
        return "missing_last_name"

    if not is_valid_email(email):
        return "invalid_email"

    if parse_date(signup_date) is None:
        return "invalid_date"

    if parse_income(annual_income) is None:
        return "invalid_income"

    if parse_boolean(is_active) is None:
        return "invalid_is_active"

    return None