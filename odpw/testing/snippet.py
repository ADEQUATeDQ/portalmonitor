# -*- coding: utf-8 -*-
from urllib import quote


def safe_unicode1(value):
    """ return the unicode representation of obj """
    if value is None:
        return None

    if type(value) == str:
        # Ignore errors even if the string is not proper UTF-8 or has
        # broken marker bytes.
        # Python built-in function unicode() can do this.
        value = unicode(value, "utf-8", errors="ignore")
    else:
        # Assume the value object has proper __unicode__() method
        value = unicode(value)
    return value

def safe_unicode(obj, *args):
    """ return the unicode representation of obj """
    try:
        return unicode(obj, *args)
    except UnicodeDecodeError:
        # obj is byte string
        ascii_text = str(obj).encode('string_escape')
        return unicode(ascii_text)
s='その他'

url='http://www.sagarpa.gob.mx//quienesomos/datosabiertos/siap/cartografia//Ubicaci\u00f3n_DDR_SAGARPA_2014.kmz'
print type(safe_unicode1(url))
print quote(safe_unicode1(url).encode('utf-8'), safe="%/:=&?~#+!$,;'@()*[]").encode('utf-8')
s=safe_unicode1(s)

print s