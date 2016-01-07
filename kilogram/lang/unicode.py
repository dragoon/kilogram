# Update this dict as needed
_CHAR_REPLACEMENT = {
    # latin-1 characters that don't have a unicode decomposition
    0x2012: u'-',  # FIGURE DASH
    0x2013: u'-',  # EN DASH
    0x2014: u'-',  # EM DASH
    0x2212: u'-',
    0x2018: u"'",  # LEFT SINGLE QUOTATION MARK
    0x2019: u"'",  # RIGHT SINGLE QUOTATION MARK
    0x201c: u'"',  # LEFT DOUBLE QUOTATION MARK
    0x201d: u'"',  # RIGHT DOUBLE QUOTATION MARK
    0xFB01: u'fi',
    0xFB02: u'fl',
    0xFB03: u'ffi',
    0xFB04: u'ffl',
    0xFB06: u'st',
    0xFB00: u'ff',
    0xFFFD: u'',
    0x00A0: u' '
}


def strip_unicode(text):
    return text.translate(_CHAR_REPLACEMENT)
