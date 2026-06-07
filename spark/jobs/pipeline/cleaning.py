from pyspark.sql import Column
from pyspark.sql.functions import regexp_replace, trim, length, when, lit


# Regex patterns------
# HTML tags like <br>, <a href="...">...</a>
_HTML_TAG = r"<[^>]+>"

# URLs: http://..., https://..., www....
_URL = r"(https?://\S+|www\.\S+)"

# Collapse any run of whitespace (spaces, tabs, newlines) into a single space
_WHITESPACE = r"\s+"

# Zero-width and control characters that often sneak in from copy/paste
_CONTROL = r"[\x00-\x1F\x7F\u200B-\u200D\uFEFF]"

# Email: word chars/.+- , then @, then a domain.
_EMAIL = r"[\w.+-]+@[\w-]+\.[\w.-]+"

# @mention of another user, e.g. "@john_doe".
_MENTION = r"@\w+"

_PHONE = r"(?<!\d)\+?\d[\d\-.()]{6,}\d(?!\d)"
_IP = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"


def scrub_pii(c: Column) -> Column:
    c = regexp_replace(c, _EMAIL,   " <EMAIL> ")
    c = regexp_replace(c, _MENTION, " <USER> ")
    c = regexp_replace(c, _PHONE,   " <PHONE> ")
    c = regexp_replace(c, _IP,      " <IP> ")
    return c

def clean_text(c: Column) -> Column:
    c = regexp_replace(c, _HTML_TAG,   " ")
    c = regexp_replace(c, _URL,        " ")
    c = scrub_pii(c) 
    c = regexp_replace(c, _CONTROL,    "")
    c = regexp_replace(c, _WHITESPACE, " ")
    c = trim(c)
    return c


# Validity rules ------
# After cleaning, decide if the record is still useful.
MAX_TEXT_LEN = 10_000

def invalid_reason(c: Column) -> Column:
    n = length(c)
    return (
        when(c.isNull(),         lit("empty_after_clean"))
        .when(n == 0,  lit("empty_after_clean"))
        .when(n > MAX_TEXT_LEN,  lit("text_too_long"))
        .otherwise(lit(None).cast("string"))
    )
