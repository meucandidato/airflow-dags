# -*- coding: utf-8 -*-

import os
import sys

import requests

from collections import OrderedDict

from logbook import Logger, StreamHandler
from rows import fields, import_from_csv
from pymongo import MongoClient, UpdateOne

from meucandidato_dags.processors import BaseProcessor
from meucandidato_dags.rows.fields import PtBrDateTimeMinField

StreamHandler(sys.stdout).push_application()
log = Logger(__name__)


CANDIDATE_FIELDS = OrderedDict([
    ('generation_date', PtBrDateTimeMinField),
    ('generation_hour', fields.TextField),
    ('election_year', fields.TextField),
    ('type_code_election', fields.IntegerField),
    ('type_election', fields.TextField),
    ('turn', fields.IntegerField),
    ('code_election', fields.IntegerField),
    ('description_election', fields.TextField),
    ('date_election', PtBrDateTimeMinField),
    ('type_comprehensiveness', fields.TextField),
    ('acronym_unit_federation', fields.TextField),
    ('acronym_unit_electoral', fields.TextField),
    ('name_unit_electoral', fields.TextField),
    ('office_code', fields.IntegerField),
    ('office_description', fields.TextField),
    ('sequential_candidate', fields.TextField),
    ('number_candidate', fields.TextField),
    ('candidate_name', fields.TextField),
    ('candidate_name_urn', fields.TextField),
    ('social_name_candidate', fields.TextField),
    ('candidate_cpf', fields.TextField),
    ('candidate_email', fields.TextField),
    ('candidacy_situation_code', fields.IntegerField),
    ('candidacy_situation_description', fields.TextField),
    ('candidacy_situation_detail_code', fields.IntegerField),
    ('candidacy_situation_detail_description', fields.TextField),
    ('type_association', fields.TextField),
    ('party_number', fields.IntegerField),
    ('party_acronym', fields.TextField),
    ('party_name', fields.TextField),
    ('legend_sequence', fields.TextField),
    ('legend_name', fields.TextField),
    ('legend_composition', fields.TextField),
    ('candidate_code', fields.IntegerField),
    ('candidate_description', fields.TextField),
    ('candidate_acronym_uf_birth', fields.TextField),
    ('candidate_city_birth_code', fields.IntegerField),
    ('candidate_city_name', fields.TextField),
    ('candidate_birthday', PtBrDateTimeMinField),
    ('candidate_age_occupation', fields.IntegerField),
    ('candidate_electoral_number', fields.TextField),
    ('gender_code', fields.IntegerField),
    ('gender_description', fields.TextField),
    ('degree_instruction_code', fields.IntegerField),
    ('degree_instruction_description', fields.TextField),
    ('marital_status_code', fields.IntegerField),
    ('marital_status_description', fields.TextField),
    ('color_race_code', fields.IntegerField),
    ('color_race_description', fields.TextField),
    ('occupation_code', fields.IntegerField),
    ('occupation_description', fields.TextField),
    ('maximum_campaign_expense', fields.FloatField),
    ('situation_totalisation_turn_code', fields.IntegerField),
    ('situation_totalisation_turn_description', fields.TextField),
    ('situation_reelection', fields.TextField),
    ('situation_declaration_property', fields.TextField),
    ('number_protocol_candidacy', fields.IntegerField),
    ('number_process', fields.TextField)
])

DIVULGA_CAND_CONTAS_API = "http://divulgacandcontas.tse.jus.br/"
ENDPOINT_SEARCH = "divulga/rest/v1/candidatura/buscar/"


class TSECandidateDataProcessor(BaseProcessor):
    data = []

    def __init__(self, data_path=None, db_uri=None, filename=None, **kwargs):
        if not db_uri:
            raise ValueError(
                "Need the database uri to connect."
            )
        if not data_path:
            raise ValueError(
                "Need the data path to import and extract"
            )

        self.data_path = data_path
        self.db_warehouse = MongoClient(db_uri)['meucandidato']
        self.filename = filename

    def run(self, **kwargs):
        self.extract(**kwargs)
        log.info('Collected {0} rows'.format(len(self.data)))
        log.info('Save candidates data on database...')
        self.db_warehouse.candidates.insert_many(self.data)

    def extract(self, **kwargs):
        self._read_file(self.data_path + '/' + self.filename)

    def _read_file(self, candidate_file):
        if os.stat(candidate_file).st_size:
            log.info('Reading file {0}.'.format(candidate_file))
            candidates = import_from_csv(candidate_file,
                                         encoding='latin-1',
                                         fields=CANDIDATE_FIELDS)

            for candidate in candidates:
                candidate_doc = candidate._asdict()
                self._parse_null_values(candidate_doc)
                self._prepare_mail(candidate_doc)
                self.data.append(candidate_doc)

    def _parse_null_values(self, candidate):
        null_str_values = ['#NULO#', '#NE#']
        null_int_values = [-1, -3]

        for field, value in candidate.items():
            if value in null_str_values:
                candidate[field] = None
            if value in null_int_values:
                candidate[field] = 0

    def _prepare_mail(self, candidate):
        if candidate['candidate_email']:
            candidate['candidate_email'] = candidate['candidate_email'].lower()

    def clear(self):
        self._data = []


class DivulgacaoCandContasTSEProcessor(BaseProcessor):
    data = []

    def __init__(self, db_uri=None, chunk_size=500, election_year=None, **kwargs):
        if not db_uri:
            raise ValueError(
                "Need the database uri to connect."
            )

        if not election_year:
            raise ValueError(
                "Need the election year to fetch candidate extra data."
            )

        if chunk_size:
            self.chunk_size = chunk_size

        self.db_warehouse = MongoClient(db_uri)['meucandidato']
        self.election_year = election_year

    def extract(self, **kwargs):
        operations = []
        candidates = self.db_warehouse.candidates.find({}, {
            "_id": 1,
            "acronym_unit_electoral": 1,
            "sequential_candidate": 1
        })

        for candidate in candidates:
            filter_path = "{year}/{state}/{election_id}/candidato/{candidate_seq}".format(
                year=self.election_year,
                state=candidate['acronym_unit_electoral'],
                election_id="2022802018",
                candidate_seq=candidate['sequential_candidate']
            )
            url = DIVULGA_CAND_CONTAS_API + ENDPOINT_SEARCH + filter_path
            response = requests.get(url)
            data = response.json()

            log.info('Update data of candidate {0}...'.format(
                candidate['sequential_candidate'],
            ))

            operations.append(
                UpdateOne(
                    {"_id": candidate['_id']},
                    {
                        '$set': {
                        'photo': data['fotoUrl'],
                        'spending': {
                            'turn_one': data['gastoCampanha1T'],
                            'turn_two': data['gastoCampanha2T']
                        },
                        "sites": data['sites']
                        }
                    }
                )
            )

            if len(operations) % self.chunk_size == 0:
                log.info("Updating block with {0} rows".format(self.chunk_size))
                self.db_warehouse.candidates.bulk_write(operations)
                operations = []

    def run(self, **kwargs):
        self.extract(**kwargs)