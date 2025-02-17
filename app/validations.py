import re
import json

def is_valid_issn(issn):
    """
    Validate an ISSN (International Standard Serial Number).
    Returns True if valid, False otherwise.
    """
    issn_pattern = re.compile(r"^\d{4}-\d{3}[\dX]$")

    if not issn_pattern.match(issn):
        return False  # Format is incorrect

    return True

def is_valid_issn_checksum(issn):
    # ISSN Checksum validation
    digits = [10 if ch == "X" else int(ch) for ch in issn.replace("-", "")]
    checksum = sum(d * (8 - i) for i, d in enumerate(digits[:-1])) % 11
    check_digit = (11 - checksum) % 11

    return check_digit == digits[-1] or (check_digit == 10 and digits[-1] == "X")

def validate_json(json_data):
    """
    Validate JSON after conversion from TSV.
    Ensures each object has a valid 'print_identifier' field.
    """
    errors = []
    warnings = []

    for i, entry in enumerate(json_data):
        issn = entry.get("print_identifier")
        if issn and not is_valid_issn(issn):
            errors.append({"row": i, "error": f"Invalid ISSN", "data": issn})
        if issn and not is_valid_issn_checksum(issn):
            warnings.append({"row": i, "warning": f"ISSN check digit does not match", "data": issn})

    return (False, json.dumps(errors), json.dumps(warnings)) if errors else (True, None, json.dumps(warnings))
