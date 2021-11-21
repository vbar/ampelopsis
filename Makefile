all: merge condensate

merge:
	python/merge.py

condensate:
	python/condensate.py
