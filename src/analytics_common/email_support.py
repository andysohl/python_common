# -*- coding: utf-8 -*-
from . import ALL_SPACE
from email.utils import parseaddr, formataddr
import re
from typing import Optional
import xxhash


def _parse_email_addresses(emails: str) -> list:
    """Parse and normalise a set of email addresses"""

    unique_emails: dict = {}
    emails = re.sub(ALL_SPACE, " ", emails)

    # Split the list. This uses a chunking method because of the quoted names at the start.
    pos = 0
    email_chunk_pattern = re.compile(r'''.*?@[A-Z0-9-]+(?:\.[A-Z0-9-]+)*\s*>?\s*(?=,|;|$)''', flags=re.I)

    while True:
        match = email_chunk_pattern.search(emails, pos)
        if not match:
            break

        parsed = parseaddr(match.group(0))
        # store it it is new or of better quality (with name)
        if (parsed[1] not in unique_emails) or (parsed[0] and not unique_emails[parsed[1]][0]):
            unique_emails[parsed[1]] = parsed

        pos = match.span(0)[1] + 1

    # return the sorted and de-duplicated emails
    val = [unique_emails[x] for x in sorted(unique_emails)]
    return val


def normalise_email_addresses(emails: str) -> Optional[str]:
    """Normalise a set of emails separated by commas or semicolons into a list of emails"""
    if emails is None or emails.strip() == "":
        return None
    return ";".join([formataddr(x) for x in _parse_email_addresses(emails)])


def filter_email_addresses(emails: str, pattern: str) -> Optional[str]:
    if emails is None or emails.strip() == "":
        return None
    if pattern is None or pattern.strip() == "":
        return normalise_email_addresses(emails)

    pattern_compiled = re.compile(pattern, re.IGNORECASE)
    emails_joined = ";".join([formataddr(x) for x in _parse_email_addresses(emails) if not pattern_compiled.fullmatch(x[1])])
    return None if emails_joined == "" else emails_joined


def hash_email_addresses(emails: str, seed: int = 42) -> Optional[int]:
    """Normalise and then hash a set of emails separated by commas or semicolons into a list of emails"""
    if emails is None or emails.strip() == "":
        return None

    hash_func = xxhash.xxh64(seed=seed)
    for x in _parse_email_addresses(emails):
        hash_func.update(x[1].lower())
    return hash_func.intdigest()


def compare_email_addresses(left: str, right: str) -> bool:
    """Compare two sets of email addresses and ensure they are equal. Returns True if equal"""
    left_arr = _parse_email_addresses(left)
    right_arr = _parse_email_addresses(right)

    # Assume two null lists are not equal as no content.
    if left_arr is None or 0 == len(left_arr) or right_arr is None or 0 == len(right_arr) or len(left) != len(right):
        return False

    return all([lemail[1] == remail[1] for lemail, remail in zip(left_arr, right_arr)])
