SHELL=/bin/bash
funnel_links=$(shell awk -F "[ \t]*=[ \t]*" '/^funnel_links/ {print $$2}' ampelopsis.ini)
has_statuses=$(shell funnel_links=$(funnel_links); echo $$(( funnel_links >= 1 )) )

all: fulltext main

main: datetime datetimes volume lang sankey chord heatmap datemap treemap distance timeline timecycle vocab demanding

fulltext: preliminary majka morphodita

majka:
	python/text-separator.py
	python/majka-stemmer.py

morphodita:
	python/text-extractor.py
	python/morphodita-stemmer.py

ifeq ($(has_statuses),1)
preliminary: condensate redirext
demanding: pool rain
else
preliminary: condensate redireco
demanding:
endif

condensate:
	python/condensate.py

redirext:
	python/redir-extend.py

redireco:
	python/redir-record.py

datetime:
	python/date-graph.py > web/datetime.csv

datetimes:
	python/datetimes-graph.py
	cp datetimes.csv datetimes.json web

sankey:
	python/network-graph.py > web/sankey.json

pinhole:
	python/pinhole-graph.py > web/sankey.json

chord:
	python/pinhole-graph.py --chord web/chord.json

tag:
	python/tag-graph.py --chord web/chord.json

link:
	python/link-graph.py --chord web/chord.json

redir:
	python/redir-graph.py --chord web/chord.json

indir:
	python/indir-graph.py --chord web/chord.json

lang:
	python/lang-graph.py
	cp lang.csv lang.json web

volume:
	python/volume-graph.py > web/volume.json

heatmap:
	python/dirichlet-cluster.py > web/heatmap.json

datemap:
	python/dirichlet-date.py > web/datemap.json

treemap:
	python/hierarchy-graph.py > web/treemap.json

distance:
	python/cosine-distance.py --histogram web/distance-check.json > web/distance.json

jaccard:
	python/jaccard-distance.py --histogram web/jaccard-check.json > web/jaccard.json

pool:
	python/pool-distance.py --histogram web/pool-check.json > web/pool.json

rain:
	python/rain-distance.py --histogram web/rain-check.json > web/rain.json

letter:
	python/letter-distance.py --histogram web/letter-check.json > web/letter.json

timeline:
	python/timeline-graph.py > web/timeline.json

timecycle:
	python/timecycle-graph.py > web/timecycle.json

repeat:
	python/repeat-graph.py > web/histogram.json

profile:
	python/profile-graph.py > web/profile.json

vocab:
	python/word-graph.py --content-words-only > web/vocab.json

tagvocab:
	python/hashtag-graph.py > web/vocab.json

pos:
	python/pos-graph.py > web/pos.json
