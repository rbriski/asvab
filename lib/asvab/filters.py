
from parsedatetime import parsedatetime as pdt
from time import strftime

def date(format):
    def d(text):
        p = pdt.Calendar()
        result = p.parse(text)

        return strftime(format, result[0])

    return d

#Stop annoying debug messages
pdt._debug=False
