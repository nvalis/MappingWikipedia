#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import os
import hashlib
import threading
import time

from gephistreamer import graph
from gephistreamer import streamer

class Article:
	def __init__(self, title, namespace=0):
		self.title = title
		self.namespace = namespace
		self.id = self._generate_id(title)

	def scrape(self):
		self.request()
		self.parse_infos()

	def request(self):
		params = {
			'action':'query',
			'titles':self.title,
			'prop':'links',
			'pllimit':500, # max is 500
			'plnamespace':'*',
			'format':'json'
		}
		r = requests.get('https://en.wikipedia.org/w/api.php', params=params)
		self.json = r.json()

	def _generate_id(self, title):
		return hashlib.md5(title.encode('utf-8')).hexdigest()

	def parse_infos(self):
		pages = list(self.json['query']['pages'].values())
		if not 'links' in pages[0]:
			self.linked_articles = []
			return

		links = pages[0]['links']
		self.linked_articles = [Article(link['title'], namespace=link['ns']) for link in links]

	def __repr__(self):
		return '<Article \'{}\'>'.format(self.title)


def worker(id, queue, history):
	web_sock = streamer.GephiWS(workspace='workspace1')
	stream = streamer.Streamer(web_sock)

	def add_to_graph(origin, linked_articles):
		o_node = graph.Node(origin.id, label=origin.title, namespace=origin.namespace)
		stream.change_node(o_node)

		for article in linked_articles:
			node = graph.Node(article.id, label=article.title, namespace=article.namespace)
			stream.add_node(node)

			edge = graph.Edge(o_node, node)
			stream.add_edge(edge)
		stream.commit()

	while True:
		try:
			article = queue.pop()
		except KeyError:
			continue

		print('[{}] Working on: {}'.format(id, article))
		history.append(article.id)

		article.scrape()
		for linked in article.linked_articles:
			if linked.id in history:
				continue
			if len(queue) < 500: # limit queue size
				queue.add(linked)

		add_to_graph(article, article.linked_articles)


def main():
	history_file = 'nodes.csv'
	if os.path.isfile(history_file):
		history = open(history_file).read().splitlines()[1:-1]
		print('Read {} history items from {}.'.format(len(history), history_file))
	else:
		history = []

	start = Article('Web scraping')

	queue = set()
	queue.add(start)

	for i in range(5):
		t = threading.Thread(target=worker, args=(i, queue, history))
		t.start()
		time.sleep(1)

if __name__ == '__main__':
	main()