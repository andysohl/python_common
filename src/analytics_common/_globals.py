"""
Support globals
"""

__ALL__ = {
    # These are regexes for different character types
    'ALL_CJK', 'ALL_CJK_PLUS_PUNCTUATION', 'ALL_CJK_PLUS_PUNCTUATION_NUMBERS',
    'ALL_PUNCTUATION', 'ALL_CJK_PLUS_PUNCTUATION_NUMBERS_SPACE', 'ALL_SPACE'
    # These are utilities for import/export of CSV/JSON
    'CSV_IMPORT_OPTIONS', 'CSV_EXPORT_OPTIONS', 'JSON_IMPORT_OPTIONS', 'JSON_EXPORT_OPTIONS',
    # Other patterns
    'EMAIL'
}

# Disallow reloading this module so as to preserve the identities of the
# classes defined here.
if '_is_loaded' in globals():
    raise RuntimeError('Reloading analytics_common._globals is not allowed')
_is_loaded = True


def assemble_character_class(block_list: list) -> str:
    """Create a regex pattern from a set of unicode character ranges"""

    block_list_as_char = []
    for i in block_list:
        if isinstance(i, list):
            f, t = i
            try:
                f = ("\\u%04x" % f) if type(f) == int else f
                t = ("\\u%04x" % t) if type(t) == int else t
                block_list_as_char.append('%s-%s' % (f, t))
            except:
                pass  # A narrow python build, so can't use chars > 65535 without surrogate pairs!

        else:
            try:
                block_list_as_char.append(("\\u%04x" % i) if type(i) == int else i)
            except:
                pass

    return '[%s]' % ''.join(block_list_as_char)


# Predefined character lists. This page is very helpful to emulate a complete character class list
# https://docs.microsoft.com/en-us/dotnet/standard/base-types/character-classes-in-regular-expressions#supported-named-blocks
__ALL_CJK = [[0x2E80, 0x2EFF],  # IsCJKRadicalsSupplement
             [0x2F00, 0x2FDF],  # IsKangxiRadicals
             [0x3000, 0x303F],  # IsCJKSymbolsandPunctuation
             [0x3040, 0x309F],  # IsHiragana
             [0x30A0, 0x30FF],  # IsKatakana
             [0x3100, 0x312F],  # IsBopomofo
             [0x3130, 0x318F],  # IsHangulCompatibilityJamo
             [0x3190, 0x319F],  # IsKanbun
             [0x31A0, 0x31BF],  # IsBopomofoExtended
             [0x31F0, 0x31FF],  # IsKatakanaPhoneticExtensions
             [0x3200, 0x32FF],  # IsEnclosedCJKLettersandMonths
             [0x3300, 0x33FF],  # IsCJKCompatibility
             [0x3400, 0x4DBF],  # IsCJKUnifiedIdeographsExtensionA
             [0x4DC0, 0x4DFF],  # IsYijingHexagramSymbols
             [0x4E00, 0x9FFF],  # IsCJKUnifiedIdeographs
             [0xA000, 0xA48F],  # IsYiSyllables
             [0xA490, 0xA4CF],  # IsYiRadicals
             [0xAC00, 0xD7AF],  # IsHangulSyllables
             [0xF900, 0xFAFF],  # IsCJKCompatibilityIdeographs
             [0xFE30, 0xFE4F],  # IsCJKCompatibilityForms
             [0xFE70, 0xFEFF],  # IsArabicPresentationForms-B
             [0xFF00, 0xFFEF],  # IsHalfwidthandFullwidthForms, Careful of this, it contains characters which OCR may use instead of ASCII.
             ]
__ALL_PUNCTUATION = [[0x21, 0x2F],
                     [0x3A, 0x40],
                     [0x5B, 0x60],
                     [0x7B, 0x7E],
                     [0x2000, 0x206F]]

__ALL_SPACE = [[0x0020, 0x0020],
               [0x00A0, 0x00A0],
               [0x1680, 0x1680],
               [0x2000, 0x200A],
               [0x202F, 0x202F],
               [0x205F, 0x205F],
               [0x3000, 0x3000]]

# Create the character classes for the language patterns
ALL_CJK = assemble_character_class(__ALL_CJK)
ALL_PUNCTUATION = assemble_character_class(__ALL_PUNCTUATION)
ALL_CJK_PLUS_PUNCTUATION = assemble_character_class(__ALL_CJK + __ALL_PUNCTUATION)
ALL_CJK_PLUS_PUNCTUATION_NUMBERS = assemble_character_class(__ALL_CJK + __ALL_PUNCTUATION + ['\\d'])
ALL_CJK_PLUS_PUNCTUATION_NUMBERS_SPACE = assemble_character_class(__ALL_CJK + __ALL_PUNCTUATION + ['\\d\\s'])
ALL_SPACE = assemble_character_class(__ALL_SPACE + ['\\s'])

# Other patterns
EMAIL = r'''[A-Z0-9_!#$%&'*+/=?`{|}~^-]+(?:\.[A-Z0-9_!#$%&'*+/=?`{|}~^-]+)*@[A-Z0-9-]+(?:\.[A-Z0-9-]+)*'''

# CSV import options
CSV_IMPORT_OPTIONS = {
    "header": "true",
    "encoding": "utf-8",
    "multiLine": "false",
    "quote": "\"",
    "escape": "\\",
    "inferSchema": "false"
}
CSV_IMPORT_MULTILINE_OPTIONS = {
    "header": "true",
    "encoding": "utf-8",
    "multiLine": "true",
    "quote": "\"",
    "escape": "\\",
    "inferSchema": "false"
}

# CSV export options
CSV_EXPORT_OPTIONS = {
    "quoteAll": "true",
    "header": "true",
    "encoding": "utf-8",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    "dateFormat": "yyyy-MM-dd"
}

# CSV export options with ISO 8601 date stamps
CSV_EXPORT_OPTIONS_ISO = {
    "quoteAll": "true",
    "header": "true",
    "encoding": "utf-8",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "dateFormat": "yyyy-MM-dd"
}

# JSON import options
JSON_IMPORT_OPTIONS = {
    "encoding": "utf-8",
    "multiLine": "false"
}

# JSON export options
JSON_EXPORT_OPTIONS = {
    "encoding": "utf-8",
    "ignoreNullFields": "false"
}

