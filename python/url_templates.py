from url_util import compile_url_pattern

legislature_index_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/zip/index.htm"

session_archive_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/zip/{1}schuz.zip"

session_folder_tmpl = "https://www.psp.cz/eknih/{0}ps/stenprot/{1}schuz/"

session_index_tmpl = session_folder_tmpl + "index.htm"

session_page_tmpl = session_folder_tmpl + "{2}.html"

segment_tmpl = session_folder_tmpl + "s{2}.htm"

legislature_index_rx = compile_url_pattern(legislature_index_tmpl)

session_archive_rx = compile_url_pattern(session_archive_tmpl)

session_index_rx = compile_url_pattern(session_index_tmpl)

session_page_rx = compile_url_pattern(session_page_tmpl, last_grp="[0-9-]+")

segment_rx = compile_url_pattern(segment_tmpl, whole=False, last_grp="[0-9]+")
