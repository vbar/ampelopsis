import re
from url_util import compile_url_pattern

legislature_index_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/zip/index.htm"

session_archive_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/zip/{1}schuz.zip"

session_folder_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/{1}schuz/"

session_index_tmpl = session_folder_tmpl + "index.htm"

page_local_name = "{2}.html?"

session_page_tmpl = session_folder_tmpl + page_local_name

segment_local_name = "s{2}.htm"

segment_tmpl = session_folder_tmpl + segment_local_name

speaker_mp_tmpl = "https://www.psp.cz/sqw/detail.sqw?id="

speaker_minister_tmpl = "https://www.vlada.cz/cz/clenove-vlady/"

synth_url_tmpl = "http://localhost/{0}ps/{1}schuz/{2}"

legislature_index_rx = compile_url_pattern(legislature_index_tmpl)

session_archive_rx = compile_url_pattern(session_archive_tmpl)

session_index_rx = compile_url_pattern(session_index_tmpl)

session_page_rx = compile_url_pattern(session_page_tmpl, last_grp="[0-9]+-[0-9]+")

page_local_rx = compile_url_pattern(page_local_name, last_grp="([0-9]+)-([0-9]+)")

segment_rx = compile_url_pattern(segment_tmpl, whole=False, last_grp="[0-9]+")

segment_local_rx = compile_url_pattern(segment_local_name, whole=False, last_grp="[0-9]+")

speaker_mp_rx = re.compile('^' + re.escape(speaker_mp_tmpl))

speaker_minister_rx = re.compile('^' + re.escape(speaker_minister_tmpl))

speaker_rx = re.compile('^(?:' + re.escape(speaker_mp_tmpl) + '|' + re.escape(speaker_minister_tmpl) + ')')
