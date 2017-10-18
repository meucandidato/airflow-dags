# -*- coding: utf-8 -*-

import os
import sys

from collections import OrderedDict

from logbook import Logger, StreamHandler
from rows import fields, import_from_csv

from meucandidato_dags.processors import BaseProcessor

StreamHandler(sys.stdout).push_application()
log = Logger(__name__)


CANDIDATE_FIELDS = OrderedDict([
    ('generation_date', fields.TextField),
    ('generation_hour', fields.TextField),
    ('election_year', fields.IntegerField),
    ('shift_number', fields.IntegerField),
    ('election_description', fields.TextField),
    ('acronym_federal_unit', fields.TextField),
    ('acronym_electoral_unit', fields.TextField),
    ('electoral_unit_description', fields.TextField),
    ('office_code', fields.IntegerField),
    ('office_description', fields.TextField),
    ('candidate_name', fields.TextField),
    ('sequential_candidate', fields.IntegerField),
    ('candidate_number', fields.IntegerField),
    ('candidate_cpf', fields.TextField),
    ('candidate_name_urn', fields.TextField),
    ('candidacy_situation_code', fields.IntegerField),
    ('candidacy_situation_description', fields.TextField),
    ('party_number', fields.IntegerField),
    ('party_acronym', fields.TextField),
    ('party_name', fields.TextField),
    ('legend_code', fields.IntegerField),
    ('legend_comsposition', fields.TextField),
    ('legend_acronym', fields.TextField),
    ('legend_name', fields.TextField),
    ('occupation_code', fields.IntegerField),
    ('occupation_description', fields.TextField),
    ('birth_date', fields.TextField),
    ('candidate_electoral_number', fields.TextField),
    ('election_date_age', fields.IntegerField),
    ('gender_code', fields.IntegerField),
    ('gender_description', fields.TextField),
    ('degree_instruction_code', fields.IntegerField),
    ('degree_instruction_description', fields.TextField),
    ('marital_status_code', fields.IntegerField),
    ('marital_status_description', fields.TextField),
    ('color_race_code', fields.IntegerField),
    ('color_race_description', fields.TextField),
    ('nationality_code', fields.IntegerField),
    ('nationality_description', fields.TextField),
    ('acronym_state_birth', fields.TextField),
    ('birth_city_code', fields.IntegerField),
    ('birth_city_name', fields.TextField),
    ('campaign_expenditure_max', fields.IntegerField),
    ('situation_totalisation_turn_code', fields.IntegerField),
    ('situation_totalisation_turn_description', fields.TextField),
    ('email', fields.TextField),

])


class TSECandidateDataProcessor(BaseProcessor):
    data = []

    def __init__(self, data_path=None, db_warehouse=None, **kwargs):
        if not db_warehouse:
            raise ValueError(
                "Need the database warehouse object to persist the data."
            )
        if not data_path:
            raise ValueError(
                "Need the data path to import and extract"
            )
        self.data_path = data_path
        self.db_warehouse = db_warehouse

    def run(self, **kwargs):
        self.extract(**kwargs)
        self.db_warehouse.candidates.insert_many(self.data)

    def extract(self, **kwargs):
        _, _, candidate_files = next(os.walk(self.data_path))

        for candidate_file in candidate_files:
            self._read_file(self.data_path + '/' + candidate_file)

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
        if candidate['email']:
            candidate['email'] = candidate['email'].lower()

    def clear(self):
        self._data = []
