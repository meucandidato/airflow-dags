# -*- coding: utf-8 -*-

import unittest

from mongomock import MongoClient

from meucandidato_dags.processors.portals import G1FeedProcessor

DB_MOCK = MongoClient().db

FEED_FIXTURE = """
<rss xmlns:atom="http://www.w3.org/2005/Atom" xmlns:media="http://search.yahoo.com/mrss/" version="2.0">
   <channel>
      <title>&amp;gt; Julgamento do mensalão</title>
      <link>http://g1.globo.com/politica/mensalao/index.html</link>
      <description>Acompanhe a cobertura completa com notícias, fotos e vídeos do julgamento do mensalão.</description>
      <language>pt-BR</language>
      <copyright>© Copyright Globo Comunicação e Participações S.A.</copyright>
      <atom:link href="http://pox.globo.com/rss/g1/politica/mensalao/" rel="self" type="application/rss+xml" />
      <image>
         <url>https://s2.glbimg.com/veNWQCjPmWVRAfzfLSJt35f_V58=/i.s3.glbimg.com/v1/AUTH_afd7a7aa13da4265ba6d93a18f8aa19e/pox/g1.png</url>
         <title>&amp;gt; Julgamento do mensalão</title>
         <link>http://g1.globo.com/politica/mensalao/index.html</link>
         <width>144</width>
         <height>144</height>
      </image>
      <item>
         <title>STF concede liberdade condicional à ex-presidente do Banco Rural Kátia Rabello</title>
         <link>http://g1.globo.com/politica/mensalao/noticia/stf-concede-liberdade-condicional-a-ex-presidente-do-banco-rural-katia-rabello.ghtml</link>
         <guid isPermaLink="true">http://g1.globo.com/politica/mensalao/noticia/stf-concede-liberdade-condicional-a-ex-presidente-do-banco-rural-katia-rabello.ghtml</guid>
         <description><![CDATA[<img src="https://s2.glbimg.com/Kxaur76sw_u2rwrNIzwF8RD_IdE=/s.glbimg.com/jo/g1/f/original/2016/11/30/katia.jpg" /><br /> ]]>  Banqueira foi condenada à prisão pelo tribunal, em 2012, no julgamento do mensalão. Benefício foi concedido pelo ministro Luís Roberto Barroso, responsável pela execução das penas.</description>
         <media:content url="https://s2.glbimg.com/Kxaur76sw_u2rwrNIzwF8RD_IdE=/s.glbimg.com/jo/g1/f/original/2016/11/30/katia.jpg" medium="image" />
         <category>G1</category>
         <pubDate>Fri, 30 Jun 2017 19:52:53 -0000</pubDate>
      </item>
      <item>
         <title>Ex-funcionária de Valério obtém liberdade condicional após 3 anos presa</title>
         <link>http://g1.globo.com/politica/mensalao/noticia/ex-funcionaria-de-valerio-obtem-liberdade-condicional-apos-3-anos-presa.ghtml</link>
         <guid isPermaLink="true">http://g1.globo.com/politica/mensalao/noticia/ex-funcionaria-de-valerio-obtem-liberdade-condicional-apos-3-anos-presa.ghtml</guid>
         <description>Simone Vasconcelos foi condenada no julgamento do mensalão a 12 anos e 7 meses por corrupção, lavagem e evasão; MP recomendou benefício por bom comportamento.</description>
         <category>G1</category>
         <pubDate>Mon, 12 Dec 2016 20:24:26 -0000</pubDate>
      </item>
   </channel>
</rss>

""" # noqa


class FeedG1ProcessTest(unittest.TestCase):
    def setUp(self):
        self.processor = G1FeedProcessor(db_warehouse=DB_MOCK)
        self.processor.feed = FEED_FIXTURE
        self.processor.clear()

    def test_should_fetch_g1_feed(self):
        news_data = [
            ("title", "STF concede liberdade condicional à ex-presidente do "
             "Banco Rural Kátia Rabello"),

            ("description", "Banqueira foi condenada à prisão pelo "
             "tribunal, em 2012, no julgamento do mensalão. "
             "Benefício foi concedido pelo ministro Luís "
             "Roberto Barroso, responsável pela "
             "execução das penas."),

            ("link", "http://g1.globo.com/politica/mensalao/noticia/"
             "stf-concede-liberdade-condicional-a-ex-presidente-do"
             "-banco-rural-katia-rabello.ghtml"),

            ("image", "https://s2.glbimg.com/Kxaur76sw_u2rwrNIzwF8RD_IdE=/"
             "s.glbimg.com/jo/g1/f/original/2016/11/30/katia.jpg"),

            ("portal_name", "G1"),

            ("published_at", "Fri, 30 Jun 2017 19:52:53 -0000")
        ]

        self.processor.run()

        for field, value in news_data:
            with self.subTest():
                self.assertEqual(value, self.processor.data[0].get(field))

    def test_should_persist_g1_feed(self):
        self.assertEqual(2, DB_MOCK.feeds.count())
