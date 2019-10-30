import os

import biothings, config
biothings.config_for_app(config)
import biothings.hub.dataload.uploader
from .parser import load_data_products, load_data_nutrients, load_data_sizes


class BfpdbProductsUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    main_source = "bfpdb"
    name = "products"
    __metadata__ = {"src_meta": {}}
    idconverter = None
    storage_class = biothings.hub.dataload.storage.BasicStorage

    def load_data(self, data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return load_data_products(data_folder)

    @classmethod
    def get_mapping(klass):
        return         {
            'NDB_Number': {
                'type': 'integer'
            },
            'data_source': {
                'normalizer': 'keyword_lowercase_normalizer',
                'type': 'keyword'
            },
            'date_available': {
                'type': 'text'
            },
            'date_modified': {
                'type': 'text'
            },
            'ingredients_english': {
                'copy_to': ['all'],
                'type': 'text'
            },
            'long_name': {
                'copy_to': ['all'],
                'type': 'text'
            },
            'manufacturer': {
                'copy_to': ['all'],
                'type': 'text'
            }
        }


class BfpdbNutrientsUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    main_source = "bfpdb"
    name = "nutrients"
    __metadata__ = {"src_meta": {}}
    idconverter = None
    storage_class = biothings.hub.dataload.storage.MergerStorage

    def load_data(self, data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return load_data_nutrients(data_folder)

    @classmethod
    def get_mapping(klass):
        return {}

class BfpdbServingSizesUploader(biothings.hub.dataload.uploader.BaseSourceUploader):

    main_source = "bfpdb"
    name = "serving_sizes"
    __metadata__ = {"src_meta": {}}
    idconverter = None
    storage_class = biothings.hub.dataload.storage.MergerStorage

    def load_data(self, data_folder):
        self.logger.info("Load data from directory: '%s'" % data_folder)
        return load_data_sizes(data_folder)

    @classmethod
    def get_mapping(klass):
        return {}
