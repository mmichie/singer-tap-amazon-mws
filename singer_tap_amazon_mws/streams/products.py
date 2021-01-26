from singer_tap_amazon_mws.streams.base import InventoryIterationStream, pluck, get_price
from singer_tap_amazon_mws.state import incorporate, save_state

from dateutil.parser import parse

import singer
import json
import time
import mws


LOGGER = singer.get_logger()  # noqa


class ProductStream(InventoryIterationStream):
    TABLE = 'products'
    KEY_PROPERTIES = ['id']

    def __init__(self, *args, **kwargs):
        super(ProductStream, self).__init__(*args, **kwargs)

    def get_config(self, product_id):
        return {
            "marketplaceid": self.config.get('marketplace_ids')[0],
            "type_": "SellerSKU",
            "ids": [product_id]
        }

    def parse_product(self, r):
        LOGGER.info(r)
        """
{ 'Id': {'value': 'GG-LVADSM'},
  'IdType': {'value': 'SellerSKU'},
  'Products': { 'Product': { 'AttributeSets': { 'ItemAttributes': { 'Binding': { 'value': 'Health ' 'and ' 'Beauty'},
                                                                    'Brand': { 'value': 'Genki ' 'Garb'},
                                                                    'Color': { 'value': 'Black'},
                                                                    'Department': { 'value': 'mens'},
                                                                    'IsAdultProduct': { 'value': 'false'},
                                                                    'Label': { 'value': 'Genki ' 'Garb'},
                                                                    'Manufacturer': { 'value': 'Genki ' 'Garb'},
                                                                    'MaterialType': [ { 'value': 'nylon'},
                                                                                      { 'value': 'polyester'}],
                                                                    'PackageDimensions': { 'Height': { 'Units': { 'value': 'inches'},
                                                                                                       'value': '1.00'},
                                                                                           'Length': { 'Units': { 'value': 'inches'},
                                                                                                       'value': '8.00'},
                                                                                           'Weight': { 'Units': { 'value': 'pounds'},
                                                                                                       'value': '0.5070632026000'},
                                                                                           'Width': { 'Units': { 'value': 'inches'},
                                                                                                      'value': '6.00'}},
                                                                    'ProductGroup': { 'value': 'Health ' 'and ' 'Beauty'},
                                                                    'ProductTypeName': { 'value': 'HEALTH_PERSONAL_CARE'},
                                                                    'Publisher': { 'value': 'Genki ' 'Garb'},
                                                                    'Size': { 'value': 'Small'},
                                                                    'SmallImage': { 'Height': { 'Units': { 'value': 'pixels'}, 'value': '75'},
                                                                                    'URL': { 'value': 'https://m.media-amazon.com/images/I/31DX4skj9ZL._SL75_.jpg'},
                                                                                    'Width': { 'Units': { 'value': 'pixels'},
                                                                                               'value': '75'}},
                                                                    'Studio': { 'value': 'Genki ' 'Garb'},
                                                                    'Title': { 'value': 'Genki ' 'Garb ' 'LVAD ' 'Medical ' 'Shirt ' 'SM'},
                                                                    'lang': { 'value': 'en-US'}}},
                             'Identifiers': { 'MarketplaceASIN': { 'ASIN': { 'value': 'B089Y6QQ4L'},
                                                                   'MarketplaceId': { 'value': 'ATVPDKIKX0DER'}}},
                             'Relationships': { 'VariationParent': { 'Identifiers': { 'MarketplaceASIN': { 'ASIN': { 'value': 'B089Y1YHPJ'},
                                                                                                           'MarketplaceId': { 'value': 'ATVPDKIKX0DER'}}}}},
                             'SalesRankings': { 'SalesRank': [ { 'ProductCategoryId': { 'value': 'fashion_display_on_website'},
                                                                 'Rank': { 'value': '230072'}},
                                                               { 'ProductCategoryId': { 'value': '9056987011'},
                                                                 'Rank': { 'value': '8458'}},
                                                               { 'ProductCategoryId': { 'value': '7581669011'},
                                                                 'Rank': { 'value': '62079'}}]}}},
  'status': {'value': 'Success'}}
        """
        return {
            # Ids
            'id': pluck(r, ['Id', 'value']),
            'IdType': pluck(r, ['IdType', 'value']),

             # Structs
            "Product": {
                'MarketplaceId': pluck(r, ['Product', 'Identifiers', 'MarketplaceASIN', 'MarketplaceId', 'value']),
                'ASIN': pluck(r, ['Products', 'Product', 'Identifiers', 'MarketplaceASIN', 'ASIN', 'value']),
                'Binding': pluck(r, ['Products', 'Product', 'AttributeSets', 'ItemAttributes', 'Binding', 'value']),
            }
        }

    def get_stream_data(self, result):
        parsed = result.parsed
        LOGGER.info("Parsing data from product")
        parsed_record = self.parse_product(parsed)

        try:
            return self.transform_record(parsed_record)
        except Exception as e:
            if hasattr(parsed, 'Id'):
                LOGGER.warning("WARNING: Couldn't sync product with SellerSKU={}; {}".format(parsed.Id, e))
            else:
                LOGGER.warning("WARNING: Couldn't sync product {}; {}".format(parsed, e))
            return None

    def sync_records(self, request_config, end_date=None):
        table = self.TABLE
        raw_product = self.client.fetch_products(request_config)
        product = self.get_stream_data(raw_product)

        if product is not None:
            with singer.metrics.record_counter(endpoint=table) as counter:
                singer.write_records(table, [product])
                counter.increment()

        return product
