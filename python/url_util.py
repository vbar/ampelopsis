import re

def format_url_pattern(tmpl, last_grp=None):
    leg_grp = "(2[0-9]{3})"
    ses_grp = "([0-9]{3})"
    if tmpl.find("{2}") >= 0:
        assert last_grp
        return tmpl.format(leg_grp, ses_grp, last_grp)
    elif tmpl.find("{1}") >= 0:
        return tmpl.format(leg_grp, ses_grp)
    else:
        return tmpl.format(leg_grp)


def make_url_pattern(tmpl, whole=True, last_grp=None):
    expr = '^' + format_url_pattern(tmpl, last_grp)
    if whole:
        expr += '$'

    return expr


def compile_url_pattern(tmpl, whole=True, last_grp=None):
    return re.compile(make_url_pattern(tmpl, whole, last_grp))
